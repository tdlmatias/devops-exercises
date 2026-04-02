#!/usr/bin/env python3
"""Update unofficial qBittorrent search plugins from the GitHub wiki list.

This script downloads the unofficial plugin list, resolves plugin source/download URLs,
then installs or updates local qBittorrent search plugins safely with backups.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import html
import json
import logging
import os
import platform
import re
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

WIKI_PAGE_URL = (
    "https://github.com/qbittorrent/search-plugins/wiki/Unofficial-search-plugins"
)
WIKI_RAW_URL = (
    "https://raw.githubusercontent.com/wiki/qbittorrent/search-plugins/"
    "Unofficial-search-plugins.md"
)
USER_AGENT = "qbittorrent-unofficial-plugin-updater/1.0"
REQUEST_TIMEOUT = 25


@dataclass
class PluginRecord:
    name: str
    source_url: str
    row_preview: str


@dataclass
class ActionResult:
    name: str
    status: str
    message: str


def fetch_text(url: str) -> str:
    """Fetch text content from URL with a basic user-agent and timeout."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="replace")


def fetch_json(url: str) -> dict:
    """Fetch JSON from URL and decode into a dictionary."""
    body = fetch_text(url)
    return json.loads(body)


def normalize_name(value: str) -> str:
    """Normalize plugin/file names for fuzzy matching."""
    return re.sub(r"[^a-z0-9]", "", value.lower())


def extract_links(markdown_cell: str) -> list[str]:
    """Extract markdown links in a single table cell."""
    return re.findall(r"\[[^\]]+\]\((https?://[^)\s]+)\)", markdown_cell)


def strip_markdown(text: str) -> str:
    """Remove basic markdown formatting to get plain text."""
    text = re.sub(r"!\[[^\]]*\]\([^)]*\)", "", text)
    text = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"[*_`~#>]", "", text)
    return " ".join(text.split()).strip()


def parse_markdown_wiki(markdown: str) -> list[PluginRecord]:
    """Parse plugin table rows from wiki markdown."""
    results: list[PluginRecord] = []
    for raw_line in markdown.splitlines():
        line = raw_line.strip()
        if line.count("|") < 4:
            continue
        if re.fullmatch(r"\|?\s*[-: ]+\|[-|: ]+", line):
            continue
        cols = [c.strip() for c in line.strip("|").split("|")]
        if len(cols) < 5:
            continue

        name = strip_markdown(cols[0])
        if not name or name.lower().startswith("search engine"):
            continue

        # Priority: download link column, then author/repository column, then name column.
        candidate_urls: list[str] = []
        candidate_urls.extend(extract_links(cols[4]))
        candidate_urls.extend(extract_links(cols[1]))
        candidate_urls.extend(extract_links(cols[0]))

        if not candidate_urls:
            continue

        results.append(
            PluginRecord(name=name, source_url=candidate_urls[0], row_preview=line[:200])
        )
    return deduplicate_plugins(results)


def parse_html_wiki(html_text: str) -> list[PluginRecord]:
    """Fallback parser if markdown source cannot be parsed."""
    rows = re.findall(r"<tr>(.*?)</tr>", html_text, flags=re.IGNORECASE | re.DOTALL)
    results: list[PluginRecord] = []
    for row in rows:
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, flags=re.IGNORECASE | re.DOTALL)
        if len(cells) < 5:
            continue

        cell_plain = [strip_markdown(html.unescape(re.sub(r"<[^>]+>", " ", c))) for c in cells]
        name = cell_plain[0].strip()
        if not name or name.lower().startswith("search engine"):
            continue

        hrefs = re.findall(r'href="(https?://[^"]+)"', row, flags=re.IGNORECASE)
        if not hrefs:
            continue

        results.append(PluginRecord(name=name, source_url=hrefs[0], row_preview=name))

    return deduplicate_plugins(results)


def deduplicate_plugins(records: Iterable[PluginRecord]) -> list[PluginRecord]:
    seen: dict[str, PluginRecord] = {}
    for rec in records:
        key = normalize_name(rec.name)
        if key and key not in seen:
            seen[key] = rec
    return sorted(seen.values(), key=lambda r: r.name.lower())


def detect_plugin_dir() -> Path:
    """Try platform-specific plugin directories, preferring existing ones."""
    home = Path.home()
    system = platform.system().lower()

    candidates: list[Path] = []
    if system == "windows":
        local_app = os.environ.get("LOCALAPPDATA")
        if local_app:
            base = Path(local_app) / "qBittorrent"
            candidates.extend([base / "nova3" / "engines", base / "nova" / "engines"])
    elif system == "darwin":
        base = home / "Library" / "Application Support" / "qBittorrent"
        candidates.extend([base / "nova3" / "engines", base / "nova" / "engines"])
    else:
        candidates.extend(
            [
                home / ".local" / "share" / "qBittorrent" / "nova3" / "engines",
                home / ".local" / "share" / "qBittorrent" / "nova" / "engines",
                home / ".config" / "qBittorrent" / "nova3" / "engines",
            ]
        )

    for path in candidates:
        if path.is_dir():
            return path

    # If nothing exists yet, return preferred path for the current OS.
    return candidates[0] if candidates else home / ".local" / "share" / "qBittorrent" / "nova3" / "engines"


def github_blob_to_raw(url: str) -> str:
    match = re.match(r"https://github\.com/([^/]+)/([^/]+)/blob/([^/]+)/(.*)", url)
    if not match:
        return url
    owner, repo, branch, path = match.groups()
    return f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"


def parse_github_repo_url(url: str) -> tuple[str, str, Optional[str], Optional[str]] | None:
    """Parse GitHub repo-like URLs into owner/repo/branch/path prefix."""
    parsed = urllib.parse.urlparse(url)
    if parsed.netloc.lower() != "github.com":
        return None

    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) < 2:
        return None
    owner, repo = parts[0], parts[1]

    branch = None
    prefix = None
    if len(parts) >= 4 and parts[2] in {"tree", "blob"}:
        branch = parts[3]
        prefix = "/".join(parts[4:])

    return owner, repo, branch, prefix


def github_default_branch(owner: str, repo: str) -> Optional[str]:
    api = f"https://api.github.com/repos/{owner}/{repo}"
    try:
        data = fetch_json(api)
    except Exception:
        return None
    return data.get("default_branch")


def github_tree_files(owner: str, repo: str, branch: str) -> list[str]:
    api = f"https://api.github.com/repos/{owner}/{repo}/git/trees/{branch}?recursive=1"
    data = fetch_json(api)
    out: list[str] = []
    for item in data.get("tree", []):
        if item.get("type") == "blob" and str(item.get("path", "")).endswith(".py"):
            out.append(item["path"])
    return out


def choose_plugin_file(paths: list[str], plugin_name: str, prefix: Optional[str]) -> Optional[str]:
    if prefix:
        pref = prefix.strip("/")
        paths = [p for p in paths if p.startswith(pref)]
    if not paths:
        return None

    cleaned = [p for p in paths if not re.search(r"(^|/)(test|tests|example|docs?)(/|$)", p, re.I)]
    if cleaned:
        paths = cleaned

    target = normalize_name(plugin_name)

    scored: list[tuple[int, str]] = []
    for path in paths:
        stem = normalize_name(Path(path).stem)
        score = 0
        if stem == target:
            score += 100
        elif target and target in stem:
            score += 60
        elif stem and stem in target:
            score += 40
        if "nova" in path.lower():
            score += 8
        if path.count("/") <= 2:
            score += 5
        scored.append((score, path))

    scored.sort(key=lambda x: (-x[0], x[1]))
    best_score, best_path = scored[0]
    if best_score <= 0 and len(paths) > 1:
        return None
    return best_path


def resolve_plugin_download_url(source_url: str, plugin_name: str) -> Optional[str]:
    """Resolve direct downloadable plugin URL (.py) from a wiki URL."""
    parsed = urllib.parse.urlparse(source_url)

    if parsed.scheme not in {"http", "https"}:
        return None

    # Direct raw python file URLs.
    if source_url.endswith(".py"):
        return github_blob_to_raw(source_url)
    if "raw.githubusercontent.com" in parsed.netloc and parsed.path.endswith(".py"):
        return source_url

    gh = parse_github_repo_url(source_url)
    if not gh:
        return None

    owner, repo, branch, prefix = gh

    # GitHub blob URL may still not end in .py due to query strings; force conversion if blob path has .py.
    if "/blob/" in source_url and prefix and prefix.endswith(".py"):
        return github_blob_to_raw(source_url)

    if not branch:
        branch = github_default_branch(owner, repo)
    if not branch:
        return None

    try:
        files = github_tree_files(owner, repo, branch)
    except Exception:
        return None

    chosen = choose_plugin_file(files, plugin_name=plugin_name, prefix=prefix)
    if not chosen:
        return None

    return f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{chosen}"


def download_bytes(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        return resp.read()


def safe_filename(plugin_name: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", plugin_name).strip("._")
    return slug.lower() or "plugin"


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def backup_file(file_path: Path, backup_dir: Path, dry_run: bool) -> Path:
    stamp = dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    backup_dir = backup_dir / stamp
    backup_path = backup_dir / file_path.name
    if dry_run:
        return backup_path

    backup_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(file_path, backup_path)
    return backup_path


def install_or_update_plugin(
    plugin: PluginRecord,
    plugin_dir: Path,
    dry_run: bool,
) -> ActionResult:
    """Install missing plugin or update existing one based on file content hash."""
    try:
        resolved_url = resolve_plugin_download_url(plugin.source_url, plugin.name)
    except Exception as exc:
        return ActionResult(plugin.name, "failed", f"URL resolve error: {exc}")

    if not resolved_url:
        return ActionResult(plugin.name, "skipped", "No valid plugin .py URL resolved")

    try:
        content = download_bytes(resolved_url)
    except urllib.error.URLError as exc:
        return ActionResult(plugin.name, "failed", f"Network/download error: {exc}")
    except Exception as exc:
        return ActionResult(plugin.name, "failed", f"Download error: {exc}")

    if not content.strip():
        return ActionResult(plugin.name, "skipped", "Downloaded file is empty")

    # Basic sanity check for qBittorrent plugin scripts.
    head = content[:4000].decode("utf-8", errors="ignore")
    if "class" not in head or "search" not in head.lower():
        return ActionResult(plugin.name, "skipped", "File does not look like a qBittorrent search plugin")

    file_name = safe_filename(plugin.name) + ".py"
    target = plugin_dir / file_name

    existing = target.read_bytes() if target.exists() else None
    new_hash = sha256_bytes(content)
    old_hash = sha256_bytes(existing) if existing is not None else None

    if old_hash == new_hash:
        return ActionResult(plugin.name, "skipped", "Already up to date")

    if dry_run:
        action = "update" if target.exists() else "install"
        return ActionResult(plugin.name, action, f"Would write {target.name} from {resolved_url}")

    try:
        plugin_dir.mkdir(parents=True, exist_ok=True)
    except PermissionError as exc:
        return ActionResult(plugin.name, "failed", f"Permission error creating plugin dir: {exc}")
    except OSError as exc:
        return ActionResult(plugin.name, "failed", f"Failed creating plugin dir: {exc}")

    if target.exists():
        try:
            backup_path = backup_file(target, plugin_dir / "backups", dry_run=False)
            logging.debug("Backed up %s -> %s", target, backup_path)
        except PermissionError as exc:
            return ActionResult(plugin.name, "failed", f"Permission error backing up file: {exc}")
        except OSError as exc:
            return ActionResult(plugin.name, "failed", f"Backup error: {exc}")

    try:
        target.write_bytes(content)
    except PermissionError as exc:
        return ActionResult(plugin.name, "failed", f"Permission denied writing plugin: {exc}")
    except OSError as exc:
        return ActionResult(plugin.name, "failed", f"Write failure: {exc}")

    return ActionResult(plugin.name, "updated" if existing is not None else "installed", resolved_url)


def load_unofficial_plugins() -> list[PluginRecord]:
    """Load plugin list from wiki markdown, fallback to HTML if needed."""
    parse_errors: list[str] = []

    try:
        markdown = fetch_text(WIKI_RAW_URL)
        plugins = parse_markdown_wiki(markdown)
        if plugins:
            return plugins
        parse_errors.append("markdown parser found no plugins")
    except Exception as exc:
        parse_errors.append(f"raw markdown fetch/parse failed: {exc}")

    try:
        html_text = fetch_text(WIKI_PAGE_URL)
        plugins = parse_html_wiki(html_text)
        if plugins:
            return plugins
        parse_errors.append("html parser found no plugins")
    except Exception as exc:
        parse_errors.append(f"html fetch/parse failed: {exc}")

    raise RuntimeError(
        "Could not parse unofficial plugin list. Wiki format may have changed. "
        + " | ".join(parse_errors)
    )


def filter_plugins(plugins: list[PluginRecord], only: Optional[str]) -> list[PluginRecord]:
    if not only:
        return plugins
    needle = normalize_name(only)
    filtered = [p for p in plugins if normalize_name(p.name) == needle]
    if filtered:
        return filtered
    # fallback partial match
    return [p for p in plugins if needle in normalize_name(p.name)]


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Install/update unofficial qBittorrent search plugins from GitHub wiki"
    )
    parser.add_argument("--dry-run", action="store_true", help="Show actions without writing files")
    parser.add_argument(
        "--plugin-dir",
        type=Path,
        default=None,
        help="Override qBittorrent plugin directory",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logs")
    parser.add_argument("--only", metavar="PLUGIN_NAME", help="Only process one plugin by name")
    return parser.parse_args(argv)


def summarize(results: list[ActionResult]) -> int:
    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1

    logging.info("Summary: %s", ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))

    for result in results:
        marker = {
            "updated": "UPDATED",
            "installed": "INSTALLED",
            "install": "DRY-RUN INSTALL",
            "update": "DRY-RUN UPDATE",
            "skipped": "SKIPPED",
            "failed": "FAILED",
        }.get(result.status, result.status.upper())
        logging.info("%-15s %-30s %s", marker, result.name, result.message)

    return 1 if counts.get("failed", 0) > 0 else 0


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)

    plugin_dir = args.plugin_dir or detect_plugin_dir()
    logging.info("Plugin directory: %s", plugin_dir)

    try:
        plugins = load_unofficial_plugins()
    except urllib.error.URLError as exc:
        logging.error("Network failure while loading wiki page: %s", exc)
        return 2
    except RuntimeError as exc:
        logging.error("Failed to parse wiki list: %s", exc)
        return 3
    except Exception as exc:
        logging.error("Unexpected startup error: %s", exc)
        return 4

    plugins = filter_plugins(plugins, args.only)
    if not plugins:
        logging.warning("No plugins matched the selection.")
        return 0

    logging.info("Discovered %d plugin entries", len(plugins))

    results: list[ActionResult] = []
    for plugin in plugins:
        try:
            result = install_or_update_plugin(plugin, plugin_dir=plugin_dir, dry_run=args.dry_run)
        except Exception as exc:
            result = ActionResult(plugin.name, "failed", f"Unhandled error: {exc}")
        results.append(result)
        time.sleep(0.05)  # tiny delay to be gentle with remote endpoints

    return summarize(results)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
