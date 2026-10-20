use std::borrow::Cow;

use axum::body::Body;
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE, ETAG, IF_NONE_MATCH};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Response};
use rust_embed::{EmbeddedFile, RustEmbed};

#[derive(RustEmbed)]
#[folder = "../phantom-ui/dist/"]
#[allow_missing = true]
pub struct UiAssets;

const INDEX: &str = "index.html";

const MISSING_BUNDLE: &str = r#"<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Phantom</title></head>
<body style="margin:0;min-height:100vh;display:grid;place-items:center;background:#09090b;color:#e4e4e7;font-family:ui-monospace,SFMono-Regular,Menlo,monospace">
<main style="max-width:44rem;padding:2rem;line-height:1.6">
<h1 style="font-size:1.1rem;letter-spacing:.2em;color:#a78bfa">PHANTOM CONTROL PLANE IS RUNNING</h1>
<p>The WebAssembly console was not embedded into this binary.</p>
<p>Build it with <code style="color:#f0abfc">make ui</code> and recompile, or restart with <code style="color:#f0abfc">--ui-dir crates/phantom-ui/dist</code>.</p>
<p>The REST API is live under <code style="color:#f0abfc">/api</code> and the event stream under <code style="color:#f0abfc">/api/ws</code>.</p>
</main>
</body>
</html>"#;

pub fn bundle_present() -> bool {
    UiAssets::get(INDEX).is_some()
}

pub async fn serve_asset(uri: Uri, headers: HeaderMap) -> Response {
    let requested = uri.path().trim_start_matches('/');
    let path = if requested.is_empty() {
        INDEX
    } else {
        requested
    };
    if let Some(file) = UiAssets::get(path) {
        return asset_response(path, file, &headers);
    }
    let looks_like_file = path
        .rsplit('/')
        .next()
        .is_some_and(|segment| segment.contains('.'));
    if looks_like_file {
        return StatusCode::NOT_FOUND.into_response();
    }
    match UiAssets::get(INDEX) {
        Some(index) => asset_response(INDEX, index, &headers),
        None => Html(MISSING_BUNDLE).into_response(),
    }
}

fn asset_response(path: &str, file: EmbeddedFile, request_headers: &HeaderMap) -> Response {
    let etag = format!("\"{}\"", hex::encode(file.metadata.sha256_hash()));
    let cache_policy = if path == INDEX {
        "no-cache"
    } else {
        "public, max-age=31536000, immutable"
    };
    let unchanged = request_headers
        .get(IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|candidates| {
            candidates
                .split(',')
                .any(|candidate| candidate.trim() == etag)
        });

    let mut response = if unchanged {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        let body = match file.data {
            Cow::Borrowed(bytes) => Body::from(bytes),
            Cow::Owned(bytes) => Body::from(bytes),
        };
        let mut response = Response::new(body);
        let mime = mime_guess::from_path(path).first_or_octet_stream();
        if let Ok(content_type) = HeaderValue::from_str(mime.as_ref()) {
            response.headers_mut().insert(CONTENT_TYPE, content_type);
        }
        response
    };
    if let Ok(etag) = HeaderValue::from_str(&etag) {
        response.headers_mut().insert(ETAG, etag);
    }
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static(cache_policy));
    response
}
