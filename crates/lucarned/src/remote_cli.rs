//! Headless remote-access CLI for `lucarned remote start|stop|status`.
//!
//! This is intentionally a thin loopback control-plane client. The daemon still
//! owns the tunnel lifecycle in [`crate::remote`]; the CLI only asks the running
//! daemon to start, stop, or report status.

use lucarned_ctl::{RemoteCommand, RemoteCommandOptions};
use serde::Deserialize;
use tracing::{debug, warn};

const DEFAULT_CONTROL_PORT: u16 = 7801;

#[derive(Debug, Clone, Default, Deserialize)]
struct RemoteStatus {
    running: bool,
    provider: Option<String>,
    public_url: Option<String>,
    access_token: Option<String>,
}

pub fn run_remote_command(command: RemoteCommand) -> Result<(), String> {
    let (status, json) = match command {
        RemoteCommand::Start(options) => (call_start(&options)?, options.json),
        RemoteCommand::Stop(options) => (call_empty_body("stop", &options)?, options.json),
        RemoteCommand::Status(options) => (call_empty_body("status", &options)?, options.json),
    };
    print_status(&status, json)
}

fn call_start(options: &RemoteCommandOptions) -> Result<RemoteStatus, String> {
    let url = control_url(options.control_port, "start");
    let body = serde_json::json!({
        "provider": options.provider.clone().unwrap_or_default(),
        "fields": options.fields,
    });
    let client = reqwest::blocking::Client::new();
    send_control(client.post(&url).json(&body), &url)
}

fn call_empty_body(verb: &str, options: &RemoteCommandOptions) -> Result<RemoteStatus, String> {
    let url = control_url(options.control_port, verb);
    let client = reqwest::blocking::Client::new();
    let request = if verb == "status" {
        client.get(&url)
    } else {
        client.post(&url)
    };
    send_control(request, &url)
}

fn send_control(
    request: reqwest::blocking::RequestBuilder,
    url: &str,
) -> Result<RemoteStatus, String> {
    debug!(target: "lucarned::remote_cli", %url, "sending loopback remote control request");
    let resp = match request.send() {
        Ok(resp) => resp,
        Err(e) => {
            warn!(
                target: "lucarned::remote_cli",
                %url,
                error = %e,
                "failed to reach loopback remote control plane"
            );
            return Err(format!("failed to reach daemon at {url}: {e}"));
        }
    };
    if !resp.status().is_success() {
        let code = resp.status();
        let detail = resp.text().unwrap_or_default();
        warn!(
            target: "lucarned::remote_cli",
            %url,
            status = %code,
            "loopback remote control request failed"
        );
        return Err(format!("daemon returned {code}: {detail}"));
    }
    let status = resp.json::<RemoteStatus>().map_err(|e| {
        warn!(
            target: "lucarned::remote_cli",
            %url,
            error = %e,
            "failed to parse loopback remote control response"
        );
        format!("failed to parse daemon response: {e}")
    })?;
    debug!(
        target: "lucarned::remote_cli",
        %url,
        running = status.running,
        provider = status.provider.as_deref().unwrap_or(""),
        has_public_url = status.public_url.is_some(),
        has_access_token = status.access_token.is_some(),
        "received loopback remote control status"
    );
    Ok(status)
}

fn control_url(control_port: Option<u16>, verb: &str) -> String {
    format!(
        "http://127.0.0.1:{}/api/remote/{verb}",
        control_port.unwrap_or(DEFAULT_CONTROL_PORT)
    )
}

fn print_status(status: &RemoteStatus, json: bool) -> Result<(), String> {
    if json {
        let value = serde_json::json!({
            "running": status.running,
            "provider": status.provider,
            "public_url": status.public_url,
            "access_token": status.access_token,
        });
        println!("{value}");
        return Ok(());
    }

    if !status.running {
        println!("remote access: stopped");
        return Ok(());
    }
    let provider = status.provider.as_deref().unwrap_or("(unknown provider)");
    let public_url = status.public_url.as_deref().unwrap_or("(no public URL)");
    let token = match status.access_token.as_deref() {
        Some(token) if !token.is_empty() => format!("token: {token}"),
        _ => "token: none".to_string(),
    };
    println!("remote access: running via {provider} - {public_url} - {token}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_url_defaults_to_daemon_control_port() {
        assert_eq!(
            control_url(None, "status"),
            "http://127.0.0.1:7801/api/remote/status"
        );
        assert_eq!(
            control_url(Some(7901), "start"),
            "http://127.0.0.1:7901/api/remote/start"
        );
    }
}
