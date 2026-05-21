//! Permission sub-domain: `PermissionRequest`/`PermissionResolved`
//! events, the `Decision`/`Risk` enums, and the structured question /
//! answer / response types used by adapters that intercept tool calls.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Risk {
    #[default]
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PermissionRequest {
    pub req_id: String,
    pub tool: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "is_risk_default")]
    pub risk: Risk,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub questions: Vec<PermissionQuestion>,
}

fn is_risk_default(r: &Risk) -> bool {
    matches!(r, Risk::Low)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PermissionResolved {
    pub req_id: String,
    pub decision: Decision,
    #[serde(default)]
    pub response: PermissionResponse,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub by: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Decision {
    #[default]
    Allow,
    Deny,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PermissionQuestion {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub header: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub question: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<PermissionQuestionOption>,
    #[serde(default, skip_serializing_if = "super::is_false")]
    pub multi_select: bool,
    #[serde(default, skip_serializing_if = "super::is_false")]
    pub is_other: bool,
    #[serde(default, skip_serializing_if = "super::is_false")]
    pub is_secret: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PermissionQuestionOption {
    pub label: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PermissionAnswer {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub answers: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub text: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PermissionResponse {
    pub decision: Decision,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub answers: BTreeMap<String, PermissionAnswer>,
}

impl PermissionResponse {
    pub fn from_decision(d: Decision) -> Self {
        Self {
            decision: d,
            answers: BTreeMap::new(),
        }
    }
}
