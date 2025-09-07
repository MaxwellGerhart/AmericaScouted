"""
Scrape NCAA team logos from a stats table and save each image
using the team name from the adjacent <a> tag as the filename.

Usage:
  python scrape_ncaa_logos.py \
      --url "https://www.ncaa.com/stats/soccer-men/d2/current/team/32" \
      --out logos_d2_gaa

Notes:
- The script will also attempt to follow pagination (1,2,3,4...) on the page.
- Handles lazy-loaded images (src or data-src), relative URLs, and name sanitization.
"""

import argparse
import os
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

def slugify(name: str) -> str:
    """Create filename slug similar to original but strip trailing periods to avoid blank extensions."""
    name = re.sub(r"\s+", " ", str(name)).strip()
    name = name.replace("/", "-")
    name = re.sub(r"[^A-Za-z0-9.\- _()&']", "", name)
    name = name.replace(" ", "_")
    name = name.rstrip('.')
    return name

def ensure_ext_from_url_or_headers(img_url: str, headers: dict, current: str) -> str:
    # If current already has a real extension (not just a trailing period), keep it; otherwise infer
    ext_existing = os.path.splitext(current)[1]
    if ext_existing and ext_existing != '.':
        return current
    # Try from URL path
    path = urlparse(img_url).path
    ext = os.path.splitext(path)[1].lower()
    if ext in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}:
        return current + ext
    # Try from content-type
    ctype = headers.get("Content-Type", "").lower()
    mapping = {
        "image/png": ".png",
        "image/jpeg": ".jpg",
        "image/jpg": ".jpg",
        "image/gif": ".gif",
        "image/webp": ".webp",
        "image/svg+xml": ".svg",
    }
    if ctype in mapping:
        return current + mapping[ctype]
    # Fallback
    return current + ".png"

def get_page(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def discover_pagination_urls(soup: BeautifulSoup, base_url: str) -> list[str]:
    """
    Try to find pagination links like 1 2 3 4 on the page.
    We'll include the base page first, then any discovered pages.
    """
    urls = {base_url}
    # Look for containers that likely hold pagination
    for a in soup.find_all("a"):
        text = (a.get_text() or "").strip()
        if text.isdigit() and len(text) <= 2 and a.get("href"):
            abs_url = urljoin(base_url, a["href"])
            # Only keep same path prefix to avoid wandering
            if urlparse(abs_url).path.split("/stats/")[0] == urlparse(base_url).path.split("/stats/")[0]:
                urls.add(abs_url)
    return sorted(urls)

def extract_rows(soup: BeautifulSoup) -> list[tuple[str, str]]:
    """
    Return a list of (img_url, team_name) from the table on the page.
    We look for table rows that have both an <img> and an <a> (team link).
    """
    results = []
    # General approach: any row with an <img> and an <a class="school">
    for tr in soup.select("tbody tr"):
        img = tr.find("img")
        a = tr.find("a")
        if not img or not a:
            continue
        # NCAA often lazy-loads images via data-src or src
        src = img.get("src") or img.get("data-src") or ""
        name = (a.get_text() or "").strip()
        if not src or not name:
            continue
        results.append((src, name))
    return results

def _is_svg(file_path: Path) -> bool:
    try:
        with open(file_path, 'rb') as f:
            start = f.read(200).lstrip()
        return start.startswith(b'<svg')
    except Exception:
        return False

def download_image(img_url: str, out_path: Path, skip_existing: bool = True):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if skip_existing:
        # Check existing with any common extension
        if out_path.exists():
            return out_path
        if not out_path.suffix:
            for ext in ('.svg', '.png', '.jpg', '.jpeg', '.gif', '.webp'):
                if out_path.with_suffix(ext).exists():
                    return out_path.with_suffix(ext)
    with requests.get(img_url, headers=HEADERS, stream=True, timeout=30) as r:
        r.raise_for_status()
        final_path = out_path
        if not final_path.suffix:
            final_path = Path(ensure_ext_from_url_or_headers(img_url, r.headers, out_path.as_posix()))
        with open(final_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    # If saved as non-svg but contents are svg, rename
    if final_path.suffix.lower() != '.svg' and _is_svg(final_path):
        svg_path = final_path.with_suffix('.svg')
        if not svg_path.exists():
            final_path.rename(svg_path)
            final_path = svg_path
    return final_path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True, help="NCAA stats page URL (e.g., DII GAA table).")
    # Default output now points inside the Flask static folder so the app can serve them directly.
    parser.add_argument(
        "--out",
        default=str(Path(__file__).resolve().parents[1] / "static" / "logos"),
        help="Output directory for images (default: project_root/static/logos).",
    )
    parser.add_argument("--no-paginate", action="store_true", help="Disable pagination discovery.")
    parser.add_argument("--fix-existing", action="store_true", help="Repair existing files lacking extensions or mis-labelled.")
    args = parser.parse_args()

    base_url = args.url
    out_dir = Path(args.out)

    if args.fix_existing:
        repaired = 0
        for fp in out_dir.glob('*'):
            if not fp.is_file():
                continue
            stem, ext = os.path.splitext(fp.name)
            if ext == '':
                # Detect svg else default png
                new_ext = '.svg' if _is_svg(fp) else '.png'
                new_path = fp.with_suffix(new_ext)
                if not new_path.exists():
                    fp.rename(new_path)
                    repaired += 1
            elif ext.lower() == '.png' and _is_svg(fp):
                svg_path = fp.with_suffix('.svg')
                if not svg_path.exists():
                    fp.rename(svg_path)
                    repaired += 1
        print(f"[FIX] Repaired {repaired} files")
        return

    print(f"[INFO] Fetching base page: {base_url}")
    soup = get_page(base_url)

    pages = [base_url]
    if not args.no_paginate:
        pages = discover_pagination_urls(soup, base_url)
    print(f"[INFO] Pages to scrape: {len(pages)}")
    for p in pages:
        print(f"[INFO] Scraping: {p}")
        psoup = soup if p == base_url else get_page(p)
        rows = extract_rows(psoup)
        print(f"[INFO] Found {len(rows)} rows on this page.")
        for src, name in rows:
            abs_src = urljoin(p, src)
            fname = slugify(name)
            outfile = out_dir / fname  # extension will be added if missing
            try:
                saved = download_image(abs_src, outfile)
                print(f"  - Saved: {saved.name}")
            except Exception as e:
                print(f"  ! Failed for {name} ({abs_src}): {e}")

if __name__ == "__main__":
    main()
