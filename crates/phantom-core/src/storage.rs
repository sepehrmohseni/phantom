use std::path::Path;
use std::sync::Arc;

use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

use crate::error::{PhantomError, PhantomResult};
use crate::models::{ReplayReport, TrafficSession};

type BlobTable = TableDefinition<'static, &'static str, &'static [u8]>;

const SESSIONS: BlobTable = TableDefinition::new("sessions");
const REPORTS: BlobTable = TableDefinition::new("reports");

pub struct PhantomStore {
    database: Database,
}

impl std::fmt::Debug for PhantomStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PhantomStore")
            .finish_non_exhaustive()
    }
}

impl PhantomStore {
    pub fn open(path: impl AsRef<Path>) -> PhantomResult<Arc<Self>> {
        let database = Database::create(path.as_ref()).map_err(PhantomError::storage)?;
        let transaction = database.begin_write().map_err(PhantomError::storage)?;
        transaction
            .open_table(SESSIONS)
            .map_err(PhantomError::storage)?;
        transaction
            .open_table(REPORTS)
            .map_err(PhantomError::storage)?;
        transaction.commit().map_err(PhantomError::storage)?;
        Ok(Arc::new(Self { database }))
    }

    pub fn insert_session(&self, session: &TrafficSession) -> PhantomResult<()> {
        self.put_many(SESSIONS, std::slice::from_ref(session), |session| {
            session.id
        })
    }

    pub fn insert_sessions(&self, sessions: &[TrafficSession]) -> PhantomResult<()> {
        self.put_many(SESSIONS, sessions, |session| session.id)
    }

    pub fn get_session(&self, id: Uuid) -> PhantomResult<Option<TrafficSession>> {
        self.fetch(SESSIONS, id)
    }

    pub fn list_sessions(&self, limit: usize) -> PhantomResult<Vec<TrafficSession>> {
        self.scan_newest(SESSIONS, limit)
    }

    pub fn session_count(&self) -> PhantomResult<u64> {
        self.count(SESSIONS)
    }

    pub fn insert_report(&self, report: &ReplayReport) -> PhantomResult<()> {
        self.put_many(REPORTS, std::slice::from_ref(report), |report| report.id)
    }

    pub fn list_reports(&self, limit: usize) -> PhantomResult<Vec<ReplayReport>> {
        self.scan_newest(REPORTS, limit)
    }

    pub fn report_count(&self) -> PhantomResult<u64> {
        self.count(REPORTS)
    }

    pub fn delete_all(&self) -> PhantomResult<()> {
        let transaction = self.database.begin_write().map_err(PhantomError::storage)?;
        for table in [SESSIONS, REPORTS] {
            transaction
                .delete_table(table)
                .map_err(PhantomError::storage)?;
            transaction
                .open_table(table)
                .map_err(PhantomError::storage)?;
        }
        transaction.commit().map_err(PhantomError::storage)
    }

    fn put_many<T: Serialize>(
        &self,
        table: BlobTable,
        records: &[T],
        key_of: impl Fn(&T) -> Uuid,
    ) -> PhantomResult<()> {
        if records.is_empty() {
            return Ok(());
        }
        let encoded = records
            .iter()
            .map(|record| {
                bincode::serialize(record)
                    .map(|payload| (key_of(record).to_string(), payload))
                    .map_err(PhantomError::codec)
            })
            .collect::<PhantomResult<Vec<_>>>()?;
        let transaction = self.database.begin_write().map_err(PhantomError::storage)?;
        {
            let mut handle = transaction
                .open_table(table)
                .map_err(PhantomError::storage)?;
            for (key, payload) in &encoded {
                handle
                    .insert(key.as_str(), payload.as_slice())
                    .map_err(PhantomError::storage)?;
            }
        }
        transaction.commit().map_err(PhantomError::storage)
    }

    fn fetch<T: DeserializeOwned>(&self, table: BlobTable, id: Uuid) -> PhantomResult<Option<T>> {
        let transaction = self.database.begin_read().map_err(PhantomError::storage)?;
        let handle = transaction
            .open_table(table)
            .map_err(PhantomError::storage)?;
        let key = id.to_string();
        let Some(guard) = handle.get(key.as_str()).map_err(PhantomError::storage)? else {
            return Ok(None);
        };
        bincode::deserialize(guard.value())
            .map(Some)
            .map_err(PhantomError::codec)
    }

    fn scan_newest<T: DeserializeOwned>(
        &self,
        table: BlobTable,
        limit: usize,
    ) -> PhantomResult<Vec<T>> {
        let transaction = self.database.begin_read().map_err(PhantomError::storage)?;
        let handle = transaction
            .open_table(table)
            .map_err(PhantomError::storage)?;
        let mut records = Vec::with_capacity(limit.min(1024));
        for entry in handle
            .iter()
            .map_err(PhantomError::storage)?
            .rev()
            .take(limit)
        {
            let (_, value) = entry.map_err(PhantomError::storage)?;
            records.push(bincode::deserialize(value.value()).map_err(PhantomError::codec)?);
        }
        Ok(records)
    }

    fn count(&self, table: BlobTable) -> PhantomResult<u64> {
        let transaction = self.database.begin_read().map_err(PhantomError::storage)?;
        let handle = transaction
            .open_table(table)
            .map_err(PhantomError::storage)?;
        handle.len().map_err(PhantomError::storage)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::models::{HttpMessage, JsonDiffEntry, OutboundExchange};
    use crate::DiffKind;

    struct ScratchFile(PathBuf);

    impl ScratchFile {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!("phantom-store-{}.redb", Uuid::new_v4())))
        }
    }

    impl Drop for ScratchFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn session(uri: &str) -> TrafficSession {
        let body = br#"{"amount":4200}"#.to_vec();
        TrafficSession {
            id: Uuid::now_v7(),
            created_at_ms: 1_760_000_000_000,
            trace_id: "trace".to_owned(),
            inbound_request: HttpMessage::request(
                "POST",
                uri,
                vec![("content-type".to_owned(), "application/json".to_owned())],
                body.clone(),
            ),
            inbound_response: HttpMessage::response(201, vec![], br#"{"ok":true}"#.to_vec()),
            outbound_calls: vec![OutboundExchange {
                id: Uuid::now_v7(),
                signature_hash: OutboundExchange::compute_signature(
                    "POST",
                    "https://api.stripe.com/v1/charges",
                    &body,
                ),
                target_host: "api.stripe.com".to_owned(),
                request: HttpMessage::request(
                    "POST",
                    "https://api.stripe.com/v1/charges",
                    vec![],
                    body,
                ),
                response: HttpMessage::response(200, vec![], vec![0xde, 0xad]),
                latency_us: 81_000,
            }],
            total_duration_us: 95_000,
        }
    }

    #[test]
    fn sessions_persist_and_list_newest_first() {
        let scratch = ScratchFile::new();
        let store = PhantomStore::open(&scratch.0).expect("store opens");
        let sessions: Vec<TrafficSession> = ["/a", "/b", "/c"].into_iter().map(session).collect();
        store
            .insert_session(&sessions[0])
            .expect("session persists");
        store
            .insert_sessions(&sessions[1..])
            .expect("batch persists");
        let listed = store.list_sessions(2).expect("sessions list");
        assert_eq!(listed, vec![sessions[2].clone(), sessions[1].clone()]);
        assert_eq!(
            store.get_session(sessions[0].id).expect("lookup succeeds"),
            Some(sessions[0].clone())
        );
        assert_eq!(
            store.get_session(Uuid::now_v7()).expect("lookup succeeds"),
            None
        );
        assert_eq!(store.session_count().expect("count succeeds"), 3);
    }

    #[test]
    fn delete_all_empties_sessions_and_reports() {
        let scratch = ScratchFile::new();
        let store = PhantomStore::open(&scratch.0).expect("store opens");
        let recorded = session("/checkout");
        store.insert_session(&recorded).expect("session persists");
        store
            .insert_report(&ReplayReport {
                id: Uuid::now_v7(),
                session_id: recorded.id,
                executed_at_ms: 1,
                original_status: 201,
                replay_status: 201,
                original_latency_us: 1,
                replay_latency_us: 1,
                status_matched: true,
                body_diffs: vec![JsonDiffEntry::new(
                    "$.ok",
                    DiffKind::Modified,
                    Some(serde_json::json!(true)),
                    None,
                )],
                outbound_diffs: Vec::new(),
                mocked_outbound_count: 1,
                passed: false,
            })
            .expect("report persists");
        assert_eq!(store.list_reports(10).expect("reports list").len(), 1);
        store.delete_all().expect("store clears");
        assert_eq!(store.session_count().expect("count succeeds"), 0);
        assert_eq!(store.report_count().expect("count succeeds"), 0);
        store
            .insert_session(&recorded)
            .expect("tables are usable after clearing");
    }
}
