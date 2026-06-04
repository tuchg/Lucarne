//! Env-gated Cloudflare API contract E2E for named tunnel provisioning.
//!
//! This is intentionally not a default network test. Run it only when you have
//! a disposable Cloudflare account/API token available:
//!
//! ```text
//! LUCARNE_CF_API_E2E=1 \
//! CLOUDFLARE_ACCOUNT_ID=... \
//! CLOUDFLARE_API_TOKEN=... \
//! cargo +nightly test -Zbuild-dir-new-layout -p lucarne-remote --test cloudflare_api_e2e
//! ```
//!
//! Required token permission, per Cloudflare's tunnel API docs: Cloudflare
//! Tunnel write/edit on the target account. The test creates a named tunnel,
//! fetches its connector token, and best-effort deletes it. It never starts
//! `cloudflared`, creates DNS records, or prints the token, so it verifies the
//! Cloudflare API contract rather than `Cloudflared::start`'s binary path.
//! Because no connector is started, the tunnel should have no active connections
//! when the cleanup DELETE runs.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::json;

const CF_API_BASE: &str = "https://api.cloudflare.com/client/v4";

#[derive(Debug)]
struct CfE2eEnv {
    account_id: String,
    api_token: String,
}

impl CfE2eEnv {
    fn load() -> Option<Self> {
        if std::env::var("LUCARNE_CF_API_E2E").ok().as_deref() != Some("1") {
            eprintln!("skipping Cloudflare API E2E: set LUCARNE_CF_API_E2E=1 to run it");
            return None;
        }

        let account_id = match std::env::var("CLOUDFLARE_ACCOUNT_ID") {
            Ok(value) if !value.trim().is_empty() => value,
            _ => {
                panic!(
                    "LUCARNE_CF_API_E2E=1 requires CLOUDFLARE_ACCOUNT_ID; \
                     unset LUCARNE_CF_API_E2E to skip this network test"
                );
            }
        };
        let api_token = match std::env::var("CLOUDFLARE_API_TOKEN") {
            Ok(value) if !value.trim().is_empty() => value,
            _ => {
                panic!(
                    "LUCARNE_CF_API_E2E=1 requires CLOUDFLARE_API_TOKEN with \
                     Cloudflare Tunnel write/edit permissions; \
                     unset LUCARNE_CF_API_E2E to skip this network test"
                );
            }
        };

        Some(Self {
            account_id,
            api_token,
        })
    }
}

#[derive(Debug, Deserialize)]
struct CfEnvelope<T> {
    success: bool,
    result: Option<T>,
    errors: Vec<CfApiMessage>,
}

#[derive(Debug, Deserialize)]
struct CfApiMessage {
    code: Option<i64>,
    message: String,
}

#[derive(Debug, Deserialize)]
struct CreatedTunnel {
    id: String,
    name: String,
}

async fn expect_cf_success<T: for<'de> Deserialize<'de>>(
    response: reqwest::Response,
    operation: &str,
) -> T {
    let status = response.status();
    let body = response
        .text()
        .await
        .unwrap_or_else(|err| format!("<could not read Cloudflare response body: {err}>"));
    assert!(
        status.is_success(),
        "{operation} returned HTTP {status}: {}",
        redact_cf_body(&body)
    );

    let envelope: CfEnvelope<T> = serde_json::from_str(&body).unwrap_or_else(|err| {
        panic!(
            "{operation} returned non-conforming Cloudflare envelope: {err}; body: {}",
            redact_cf_body(&body)
        )
    });
    assert!(
        envelope.success,
        "{operation} returned success=false: {}",
        format_cf_errors(&envelope.errors)
    );
    envelope
        .result
        .unwrap_or_else(|| panic!("{operation} returned success=true without result"))
}

fn format_cf_errors(errors: &[CfApiMessage]) -> String {
    if errors.is_empty() {
        return "<no Cloudflare error details>".to_string();
    }
    errors
        .iter()
        .map(|err| match err.code {
            Some(code) => format!("{code}: {}", err.message),
            None => err.message.clone(),
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn redact_cf_body(body: &str) -> String {
    let Ok(mut value) = serde_json::from_str::<serde_json::Value>(body) else {
        return body.to_string();
    };
    redact_json_tokens(&mut value);
    serde_json::to_string(&value).unwrap_or_else(|_| "<redacted Cloudflare body>".to_string())
}

fn redact_json_tokens(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, value) in map.iter_mut() {
                let sensitive = key.eq_ignore_ascii_case("token")
                    || key.eq_ignore_ascii_case("tunnel_token")
                    || key.eq_ignore_ascii_case("client_secret")
                    || key.eq_ignore_ascii_case("secret");
                if sensitive {
                    *value = serde_json::Value::String("<redacted>".to_string());
                } else {
                    redact_json_tokens(value);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                redact_json_tokens(item);
            }
        }
        _ => {}
    }
}

fn unique_tunnel_name() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    format!("lucarne-e2e-{}-{millis}", std::process::id())
}

#[tokio::test]
async fn cloudflare_api_can_create_token_and_delete_named_tunnel() {
    let Some(env) = CfE2eEnv::load() else {
        return;
    };

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build Cloudflare API client");
    let base = format!("{CF_API_BASE}/accounts/{}/cfd_tunnel", env.account_id);
    let name = unique_tunnel_name();

    let create_body = json!({
        "name": name,
        "config_src": "cloudflare",
    });
    let created: CreatedTunnel = expect_cf_success(
        client
            .post(&base)
            .bearer_auth(&env.api_token)
            .json(&create_body)
            .send()
            .await
            .expect("send Cloudflare create tunnel request"),
        "create Cloudflare tunnel",
    )
    .await;

    let cleanup = TunnelCleanup {
        client: client.clone(),
        api_token: env.api_token.clone(),
        url: format!("{base}/{}", created.id),
    };

    let mut failures = Vec::new();
    if created.name != name {
        failures.push(format!(
            "Cloudflare should echo the tunnel name; expected `{name}`, got `{}`",
            created.name
        ));
    }
    if created.id.trim().is_empty() {
        failures.push("Cloudflare create tunnel result must include id".to_string());
    }

    match client
        .get(format!("{base}/{}/token", created.id))
        .bearer_auth(&env.api_token)
        .send()
        .await
    {
        Ok(response) => {
            let token: String = expect_cf_success(response, "get Cloudflare tunnel token").await;
            if token.len() < 40 {
                failures.push(
                    "Cloudflare connector token should be present and non-trivial".to_string(),
                );
            }
        }
        Err(err) => failures.push(format!(
            "could not send Cloudflare get tunnel token request: {err}"
        )),
    }

    cleanup.delete().await;

    assert!(
        failures.is_empty(),
        "Cloudflare API E2E failures after create: {}",
        failures.join("; ")
    );
}

struct TunnelCleanup {
    client: reqwest::Client,
    api_token: String,
    url: String,
}

impl TunnelCleanup {
    async fn delete(self) {
        let response = self
            .client
            .delete(&self.url)
            .bearer_auth(&self.api_token)
            .send()
            .await
            .expect("send Cloudflare delete tunnel request");
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|err| format!("<could not read Cloudflare delete body: {err}>"));
        assert!(
            status.is_success() || status == StatusCode::NOT_FOUND,
            "delete Cloudflare tunnel returned HTTP {status}: {}",
            redact_cf_body(&body)
        );
    }
}
