use std::time::Instant;

use axum::http::{HeaderMap, HeaderName, HeaderValue};
use chrono::Utc;
use phantom_core::{
    compute_body_diff, PhantomError, PhantomResult, ReplayBatchSummary, ReplayReport, WsEvent,
};
use uuid::Uuid;

use crate::state::{
    normalize_base_url, EngineState, REPLAY_OF_HEADER, REPLAY_TRACE_PREFIX, TRACE_HEADER,
};
use crate::wire;

pub async fn replay_session(
    engine_state: &EngineState,
    session_id: Uuid,
    target_staging_url: &str,
) -> PhantomResult<ReplayReport> {
    let base = normalize_base_url(target_staging_url)?;
    let session = engine_state
        .with_store(move |store| store.get_session(session_id))
        .await?
        .ok_or(PhantomError::SessionNotFound(session_id))?;

    let trace_id = format!("{REPLAY_TRACE_PREFIX}{}", Uuid::now_v7().simple());
    let request = session.inbound_request.clone();
    let original_response = session.inbound_response.clone();
    let original_latency_us = session.total_duration_us;
    let url = wire::join_url(&base, &request.uri);
    let headers = replay_headers(&trace_id, session_id)?;

    let lease = engine_state
        .sandbox
        .engage(
            trace_id.clone(),
            session,
            engine_state.config.ignore_rules.clone(),
        )
        .await;
    let started = Instant::now();
    let dispatched = wire::dispatch(
        &engine_state.http,
        &url,
        &request,
        headers,
        engine_state.config.max_body_bytes,
    )
    .await;
    let replay_latency_us = wire::elapsed_us(started);
    let outcome = lease.finish();
    let reply = dispatched?;

    if outcome.misses > 0 {
        tracing::warn!(%session_id, misses = outcome.misses, "replay issued outbound calls with no recorded counterpart");
    }

    let original_status = original_response.status.unwrap_or_default();
    let status_matched = original_status == reply.status;
    let body_diffs = engine_state
        .config
        .ignore_rules
        .retain(compute_body_diff(&original_response.body, &reply.body));
    let passed = status_matched && body_diffs.is_empty() && outcome.drift.is_empty();

    let report = ReplayReport {
        id: Uuid::now_v7(),
        session_id,
        executed_at_ms: Utc::now().timestamp_millis(),
        original_status,
        replay_status: reply.status,
        original_latency_us,
        replay_latency_us,
        status_matched,
        body_diffs,
        outbound_diffs: outcome.drift,
        mocked_outbound_count: outcome.mocked,
        passed,
    };

    let stored = report.clone();
    engine_state
        .with_store(move |store| store.insert_report(&stored))
        .await?;
    tracing::info!(
        %session_id,
        report = %report.id,
        %trace_id,
        passed = report.passed,
        original_status,
        replay_status = report.replay_status,
        drift = report.drift_count(),
        mocks = report.mocked_outbound_count,
        "replay completed"
    );
    engine_state.publish(WsEvent::ReplayCompleted(report.clone()));
    Ok(report)
}

pub async fn replay_all(
    engine_state: &EngineState,
    target_staging_url: &str,
    limit: usize,
) -> PhantomResult<ReplayBatchSummary> {
    let target_url = normalize_base_url(target_staging_url)?;
    let mut sessions = engine_state
        .with_store(move |store| store.list_sessions(limit))
        .await?;
    sessions.reverse();

    let mut summary = ReplayBatchSummary {
        target_url: target_url.clone(),
        total: sessions.len(),
        ..ReplayBatchSummary::default()
    };
    for session in sessions {
        match replay_session(engine_state, session.id, &target_url).await {
            Ok(report) if report.passed => summary.passed += 1,
            Ok(_) => summary.failed += 1,
            Err(err) => {
                summary.errored += 1;
                tracing::warn!(session = %session.id, error = %err, "replay aborted");
            }
        }
    }
    Ok(summary)
}

fn replay_headers(trace_id: &str, session_id: Uuid) -> PhantomResult<HeaderMap> {
    let mut headers = HeaderMap::new();
    for (name, value) in [
        (TRACE_HEADER, trace_id.to_owned()),
        (REPLAY_OF_HEADER, session_id.to_string()),
    ] {
        let value = HeaderValue::from_str(&value)
            .map_err(|err| PhantomError::Internal(format!("invalid replay header value: {err}")))?;
        headers.insert(HeaderName::from_static(name), value);
    }
    Ok(headers)
}
