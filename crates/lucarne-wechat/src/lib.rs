#![allow(
    clippy::chars_last_cmp,
    clippy::chars_next_cmp,
    clippy::clone_on_copy,
    clippy::explicit_auto_deref,
    clippy::large_enum_variant,
    clippy::manual_div_ceil,
    clippy::obfuscated_if_else,
    clippy::unnecessary_map_or
)]

//! WeChat notification bridge for lucarne.
//!
//! This crate intentionally implements only the WeChat user journey:
//! watched agent messages are delivered to WeChat users, quoted replies
//! continue the bound provider session, and scoped notification policy
//! is resolved by `LucarneCore`.

pub mod adapter;
pub mod context_store;
mod intervention;
pub mod onboarding;
pub mod service;

pub use adapter::{run_wechat_adapter, wechat_plugin, WechatAdapterPlugin, WechatConfig};
pub use service::{
    WechatError, WechatIncoming, WechatNotificationService, WechatSendReceipt,
    WechatServiceOptions, WechatTransport, WechatUserInteractionRequest,
};
