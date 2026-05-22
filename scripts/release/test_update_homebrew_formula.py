#!/usr/bin/env python3
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


UPDATER = Path(__file__).with_name("update-homebrew-formula.py")
REPO_ROOT = Path(__file__).resolve().parents[2]
CHECKED_IN_FORMULA = REPO_ROOT / "Formula" / "lucarned.rb"
ARM_SHA = "a" * 64
X86_SHA = "b" * 64
SERVICE_PATH_ENV_LINE = 'environment_variables PATH: ENV.fetch("HOMEBREW_PATH", std_service_path_env)'


class UpdateHomebrewFormulaTest(unittest.TestCase):
    def run_updater(self, formula: Path, *extra_args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(UPDATER), str(formula), *extra_args],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    def test_rewrites_source_formula_to_binary_formula(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            formula = Path(tmpdir) / "lucarned.rb"
            formula.write_text(
                "class Lucarned < Formula\n"
                "  desc \"old\"\n"
                "  url \"https://example.invalid/source.tar.gz\"\n"
                "end\n",
                encoding="utf-8",
            )

            result = self.run_updater(
                formula,
                "--version",
                "0.2.3",
                "--arm64-sha",
                ARM_SHA,
                "--x86-64-sha",
                X86_SHA,
            )

            self.assertEqual(result.returncode, 0, msg=result.stderr)
            output = formula.read_text(encoding="utf-8")
            self.assertIn("class Lucarned < Formula", output)
            self.assertIn('desc "Local lucarne daemon for channel adapters and agent sessions"', output)
            self.assertIn('homepage "https://github.com/tuchg/Lucarne"', output)
            self.assertIn('version "0.2.3"', output)
            self.assertIn('license "MIT"', output)
            self.assertIn("depends_on :macos", output)
            self.assertIn("on_arm do", output)
            self.assertIn(
                'url "https://github.com/tuchg/Lucarne/releases/download/v0.2.3/lucarned-v0.2.3-aarch64-apple-darwin.tar.xz"',
                output,
            )
            self.assertIn(f'sha256 "{ARM_SHA}"', output)
            self.assertIn("on_intel do", output)
            self.assertIn(
                'url "https://github.com/tuchg/Lucarne/releases/download/v0.2.3/lucarned-v0.2.3-x86_64-apple-darwin.tar.xz"',
                output,
            )
            self.assertIn(f'sha256 "{X86_SHA}"', output)
            self.assertIn("head do", output)
            self.assertIn('depends_on "pkg-config" => :build', output)
            self.assertIn('depends_on "rust" => :build', output)
            self.assertIn('depends_on "openssl@3"', output)
            self.assertIn("if build.head?", output)
            self.assertNotIn('inreplace "Cargo.toml"', output)
            self.assertNotIn("wechat-ilink = { path", output)
            self.assertNotIn("wechat-ilink = \\{ path", output)
            self.assertIn('bin.install "lucarned"', output)
            self.assertNotIn('bin.install "bin/lucarned"', output)
            self.assertIn("brew services start lucarned", output)
            self.assertIn(SERVICE_PATH_ENV_LINE, output)
            self.assertIn('assert_match "enabled: false", config.read', output)

    def test_checked_in_formula_service_exports_invoking_path(self) -> None:
        output = CHECKED_IN_FORMULA.read_text(encoding="utf-8")

        self.assertIn(SERVICE_PATH_ENV_LINE, output)

    def test_homebrew_service_plist_embeds_invoking_path(self) -> None:
        brew = shutil.which("brew")
        if brew is None:
            self.skipTest("brew is not available")
        user_path = "/tmp/lucarne-user-bin:/opt/homebrew/bin:/usr/bin:/bin"
        env = os.environ.copy()
        env["PATH"] = user_path
        env["HOMEBREW_DEVELOPER"] = "1"
        ruby = (
            'require "formula"; '
            'formula = Formulary.factory(ARGV[0]); '
            'puts formula.service.to_plist'
        )

        result = subprocess.run(
            [brew, "ruby", "-e", ruby, str(CHECKED_IN_FORMULA)],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            env=env,
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("<key>EnvironmentVariables</key>", result.stdout)
        self.assertIn(f"<string>{user_path}</string>", result.stdout)

    def test_rejects_bad_arm64_sha(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            formula = Path(tmpdir) / "lucarned.rb"
            formula.write_text("class Lucarned < Formula\nend\n", encoding="utf-8")

            result = self.run_updater(
                formula,
                "--version",
                "0.2.3",
                "--arm64-sha",
                "not-a-sha",
                "--x86-64-sha",
                X86_SHA,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("--arm64-sha must be 64 lowercase hex characters", result.stderr)

    def test_rejects_bad_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            formula = Path(tmpdir) / "lucarned.rb"
            formula.write_text("class Lucarned < Formula\nend\n", encoding="utf-8")

            result = self.run_updater(
                formula,
                "--version",
                "release-0.2.3",
                "--arm64-sha",
                ARM_SHA,
                "--x86-64-sha",
                X86_SHA,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("--version must look like 0.1.0", result.stderr)


if __name__ == "__main__":
    unittest.main()
