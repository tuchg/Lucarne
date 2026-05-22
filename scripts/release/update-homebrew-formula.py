#!/usr/bin/env python3
import argparse
import re
import sys
from pathlib import Path


VERSION_RE = re.compile(r"^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$")
SHA_RE = re.compile(r"^[0-9a-f]{64}$")

FORMULA_TEMPLATE = r'''class Lucarned < Formula
  desc "Local lucarne daemon for channel adapters and agent sessions"
  homepage "https://github.com/tuchg/Lucarne"
  version "__VERSION__"
  license "MIT"

  depends_on :macos

  stable do
    on_macos do
      on_arm do
        url "https://github.com/tuchg/Lucarne/releases/download/v__VERSION__/lucarned-v__VERSION__-aarch64-apple-darwin.tar.xz"
        sha256 "__ARM64_SHA__"
      end

      on_intel do
        url "https://github.com/tuchg/Lucarne/releases/download/v__VERSION__/lucarned-v__VERSION__-x86_64-apple-darwin.tar.xz"
        sha256 "__X86_64_SHA__"
      end
    end
  end

  head do
    url "https://github.com/tuchg/Lucarne.git", branch: "main"

    depends_on "pkg-config" => :build
    depends_on "rust" => :build
    depends_on "openssl@3"
  end

  def install
    if build.head?
      ENV["OPENSSL_DIR"] = Formula["openssl@3"].opt_prefix

      system "cargo", "install", "--path", "crates/lucarned", "--root", prefix, "--no-track"
    else
      bin.install "lucarned"
    end
  end

  service do
    run [opt_bin/"lucarned"]
    environment_variables PATH: ENV.fetch("HOMEBREW_PATH", std_service_path_env)
    keep_alive false
    log_path var/"log/lucarned/brew.out.log"
    error_log_path var/"log/lucarned/brew.err.log"
    working_dir HOMEBREW_PREFIX
  end

  def caveats
    <<~EOS
      lucarned creates ~/.lucarned/lucarned.yaml during setup.

      Basic setup:
        lucarned init
        brew services start lucarned

      lucarned init is interactive; run it in a terminal. It can validate
      Telegram settings and show a WeChat QR code login.

      Config can enable selected agents (omit agents to enable all compiled agents):
        agents:
          - codex
          - pi

      Config can enable channels before starting service, for example Telegram:
        channels:
          telegram:
            enabled: true
            token: "123456:REDACTED"
            entry_chat_id: 123456789

      Common commands:
        brew services start lucarned
        brew services stop lucarned
        brew services restart lucarned

      Logs:   ~/.lucarned/logs/lucarned.YYYY-MM-DD.log
      Config: ~/.lucarned/lucarned.yaml
      State:  ~/.lucarned/state.sqlite3
      Brew service logs:
        #{var}/log/lucarned/brew.out.log
        #{var}/log/lucarned/brew.err.log
    EOS
  end

  test do
    ENV["HOME"] = testpath
    ENV.delete "LUCARNE_CONFIG"
    ENV.delete "LUCARNED_CONFIG"
    ENV.delete "LUCARNE_STATE_DB"
    ENV.delete "LUCARNE_LOG_FILE"

    system bin/"lucarned"
    config = testpath/".lucarned/lucarned.yaml"
    assert_path_exists config
    assert_match "agents:", config.read
    assert_match "telegram:", config.read
    assert_match "wechat:", config.read
    assert_match "enabled: false", config.read
  end
end
'''


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Rewrite lucarned Homebrew formula for binary release.")
    parser.add_argument("formula", help="Path to Formula/lucarned.rb")
    parser.add_argument("--version", required=True, help="Release version, for example 0.1.0")
    parser.add_argument("--arm64-sha", required=True, help="SHA-256 for arm64 macOS tarball")
    parser.add_argument("--x86-64-sha", required=True, help="SHA-256 for x86_64 macOS tarball")
    return parser


def render_formula(version: str, arm64_sha: str, x86_64_sha: str) -> str:
    return (
        FORMULA_TEMPLATE.replace("__VERSION__", version)
        .replace("__ARM64_SHA__", arm64_sha)
        .replace("__X86_64_SHA__", x86_64_sha)
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if not VERSION_RE.fullmatch(args.version):
        print("--version must look like 0.1.0", file=sys.stderr)
        return 2
    if not SHA_RE.fullmatch(args.arm64_sha):
        print("--arm64-sha must be 64 lowercase hex characters", file=sys.stderr)
        return 2
    if not SHA_RE.fullmatch(args.x86_64_sha):
        print("--x86-64-sha must be 64 lowercase hex characters", file=sys.stderr)
        return 2

    formula = Path(args.formula)
    if not formula.exists():
        print(f"formula file does not exist: {formula}", file=sys.stderr)
        return 2

    formula.write_text(render_formula(args.version, args.arm64_sha, args.x86_64_sha), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
