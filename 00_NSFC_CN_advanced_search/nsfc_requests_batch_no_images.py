# -*- coding: utf-8 -*-
"""
NSFC final project requests batch search script (without image downloads).

Features:
- Uses plain requests; does not launch a browser or download final report images.
- Includes the built-in 338-term "urban sustainability" search lexicon.
- Supports automatic pagination, resumable runs, deduplication by approval number,
  and per-project query hit tracking.
- By default, fetches abstracts, conclusion abstracts, and achievement lists from
  project details, then counts achievements locally.

Recommended examples:
    python nsfc_requests_batch_no_images.py --output-dir output_requests --year-field conclusionYear --start-year 2021 --end-year 2024
    python nsfc_requests_batch_no_images.py --output-dir output_requests --year-field ratifyYear --start-year 2016 --end-year 2021
    python nsfc_requests_batch_no_images.py --output-dir output_requests --limit-queries 3 --max-pages 1
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import importlib
import json
import math
import os
import random
import re
import sys
import threading
import time
import warnings
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from collections import Counter
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import requests


BASE_URL = "https://kd.nsfc.cn"
SEARCH_ENDPOINT = "/api/baseQuery/completionQueryResultsData"
DETAIL_ENDPOINT = "/api/baseQuery/conclusionProjectInfo/{detail_id}"
DES_KEY = b"IFROMC86"
DEFAULT_PROXY_API_URL = "http://api.tianqiip.com/getip?secret=exjoendz255mnpvb&num=2&type=txt&port=1&time=3&mr=1&sign=7260805cab81a959dae22856ca90a8cf"
DEFAULT_PROXY_WHITELIST_API_URL = "http://api.tianqiip.com/white/add?key=beibei4980&brand=2&sign=7260805cab81a959dae22856ca90a8cf&ip={}"


CITY_TERMS = [
    "城市",
    "城镇",
    "都市",
    "城市群",
    "都市圈",
    "社区",
    "建成区",
    "市政",
]

CORE_SUSTAINABILITY_TERMS = [
    "可持续",
    "低碳",
    "碳中和",
    "碳达峰",
    "气候适应",
    "气候变化",
    "韧性",
    "绿色基础设施",
    "环境治理",
    "生态城市",
]

ENVIRONMENT_CLIMATE_TERMS = [
    "空气污染",
    "热岛",
    "洪涝",
    "海绵城市",
    "雨洪",
    "水安全",
    "污水",
    "固废",
    "循环经济",
    "交通排放",
    "能源转型",
]

SOCIAL_EQUITY_TERMS = [
    "环境健康",
    "环境正义",
    "暴露",
    "脆弱性",
    "风险",
    "适应",
    "减灾",
    "老龄化",
    "宜居",
    "公共服务",
]

SPACE_INFRA_TERMS = [
    "基础设施",
    "土地利用",
    "街景",
    "遥感",
    "城市计算",
    "TOD",
    "蓝绿空间",
    "老旧小区",
    "城中村",
    "生态系统服务",
]

STANDALONE_URBAN_TERMS = [
    "绿色基础设施",
    "生态城市",
    "海绵城市",
    "城市计算",
    "城市群",
    "都市圈",
    "老旧小区",
    "城中村",
    "街景",
    "TOD",
]


ADVANCED_PROJECT_TYPES = [
    ("面上项目", "218"),
    ("重点项目", "220"),
    ("重大研究计划", "339"),
    ("联合基金项目", "579"),
    ("青年科学基金项目（C类）", "630"),
    ("地区科学基金项目", "631"),
    ("专项基金项目", "649"),
    ("数学天元基金项目", "80"),
]

ADVANCED_CODES_2ND = [
    "C01", "C02", "C03", "C04", "C05", "C06", "C07", "C08", "C09", "C10", "C11", "C12", "C13", "C14", "C15", "C16", "C17", "C18", "C19", "C20", "C21",
    "D01", "D02", "D03", "D04", "D05", "D06", "D07",
    "E01", "E02", "E03", "E04", "E05", "E06", "E07", "E08", "E09", "E10", "E11", "E12", "E13",
    "F01", "F02", "F03", "F04", "F05", "F06", "F07",
    "G01", "G02", "G03", "G04",
]

SEARCH_FIELD_LABELS = {
    "fuzzy": "综合检索",
    "projectName": "项目名称",
    "keywords": "项目关键词",
}


RESULT_FIELDS = [
    "detail_id",
    "approval_no",
    "project_name",
    "project_type",
    "depend_unit",
    "project_admin",
    "support_num",
    "ratify_year",
    "conclusion_year",
    "apply_code",
    "project_keywords",
    "research_start_date",
    "research_end_date",
    "project_abstract_cn",
    "project_abstract_en",
    "conclusion_abstract",
    "has_report",
    "journal_paper_count",
    "conference_paper_count",
    "book_count",
    "reward_count",
    "patent_count",
    "result_total_count",
    "results_list_json",
    "query_matches",
    "query_count",
    "search_fields",
    "search_payload_keywords",
    "search_project_types",
    "search_project_type_codes",
    "search_apply_codes",
    "search_year_filters",
    "source_url",
    "last_seen_at",
]


QUERY_FIELDS = [
    "query_id",
    "preset",
    "query_type",
    "city_term",
    "topic_group",
    "topic_term",
    "query_keyword",
    "query_label",
    "payload_keyword",
    "search_field",
    "year_field",
    "year_value",
    "project_type",
    "project_type_name",
    "apply_code",
]


PROGRESS_FIELDS = [
    "query_id",
    "query_keyword",
    "year_field",
    "year_value",
    "status",
    "total_records",
    "pages_fetched",
    "rows_seen",
    "new_projects",
    "updated_at",
    "message",
]


@dataclass(frozen=True)
class QuerySpec:
    query_id: str
    preset: str
    query_type: str
    city_term: str
    topic_group: str
    topic_term: str
    query_keyword: str
    query_label: str = ""
    payload_keyword: str = ""
    search_field: str = "fuzzy"
    year_field: str = ""
    year_value: str = ""
    project_type: str = ""
    project_type_name: str = ""
    apply_code: str = ""


@dataclass
class QueryRunResult:
    spec: QuerySpec
    rows_seen: int = 0
    pages_fetched: int = 0
    total_records: int = 0
    status: str = "done"
    message: str = ""
    projects: List[Dict[str, Any]] = field(default_factory=list)


class DecodeError(RuntimeError):
    pass


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def safe_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(str(value)))
    except Exception:
        return default


def safe_get(seq: Sequence[Any], index: int, default: str = "") -> Any:
    try:
        value = seq[index]
    except Exception:
        return default
    return default if value is None else value


def polite_sleep(base_delay: float, jitter: float) -> None:
    if base_delay <= 0 and jitter <= 0:
        return
    time.sleep(max(0.0, base_delay + random.uniform(0, max(0.0, jitter))))


PROXY_SUSPECT_HTTP = {403, 407, 429, 500, 502, 503, 504}
PROXY_ERROR_KEYWORDS = [
    "proxy",
    "tunnel",
    "connection reset",
    "connection aborted",
    "remote end closed",
    "timed out",
    "timeout",
    "10054",
    "407",
    "429",
    "503",
    "访问受限",
    "ip地址已被限制",
    "验证码",
    "人机验证",
    "decodeerror",
    "无法解析响应",
]


def normalize_proxy_url(proxy_ip: str) -> str:
    text = safe_text(proxy_ip)
    if not text:
        return ""
    if re.match(r"^https?://", text, re.I):
        return text
    return f"http://{text}"


def parse_proxy_lines(text: str) -> List[str]:
    proxies: List[str] = []
    for line in text.splitlines():
        item = line.strip()
        if not item or item.startswith("#"):
            continue
        if re.match(r"^https?://", item, re.I):
            item = re.sub(r"^https?://", "", item, flags=re.I)
        if re.match(r"^\d{1,3}(?:\.\d{1,3}){3}:\d{2,5}$", item):
            proxies.append(item)
    return list(dict.fromkeys(proxies))


class ProxyManager:
    def __init__(
        self,
        *,
        proxy_api_url: str = "",
        whitelist_api_url: str = "",
        proxy_file: str = "",
        expire_seconds: int = 180,
        rotate_guard_seconds: int = 10,
        cooldown_seconds: float = 3.0,
        cache_path: Optional[Path] = None,
        disabled_whitelist: bool = False,
    ):
        self.proxy_api_url = safe_text(proxy_api_url)
        self.whitelist_api_url = safe_text(whitelist_api_url)
        self.proxy_file = safe_text(proxy_file)
        self.expire_seconds = expire_seconds
        self.rotate_guard_seconds = rotate_guard_seconds
        self.cooldown_seconds = cooldown_seconds
        self.disabled_whitelist = disabled_whitelist
        self.cache_path = cache_path

        self.proxy_pool: List[str] = []
        self.failed_proxies: Set[str] = set()
        self.current_index = 0
        self.proxy_start_time = 0.0
        self.last_proxy_refresh_time = 0.0
        self.fail_count = 0
        self._lock = threading.Lock()
        self._last_whitelist_ip = ""

        self._direct = requests.Session()
        self._direct.trust_env = False
        self._direct.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "*/*",
                "Connection": "close",
            }
        )
        self._load_whitelist_cache()

    def has_source(self) -> bool:
        return bool(self.proxy_api_url or self.proxy_file)

    def _load_whitelist_cache(self) -> None:
        if not self.cache_path or not self.cache_path.exists():
            return
        try:
            data = json.loads(self.cache_path.read_text(encoding="utf-8"))
            self._last_whitelist_ip = safe_text(data.get("last_ip"))
        except Exception:
            self._last_whitelist_ip = ""

    def _save_whitelist_cache(self, ip: str) -> None:
        if not self.cache_path:
            return
        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.cache_path.write_text(
                json.dumps({"last_ip": ip, "update_time": now_iso()}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass

    def _get_public_ip(self) -> str:
        for url in ["https://api.ipify.org?format=json", "https://httpbin.org/ip"]:
            try:
                response = self._direct.get(url, timeout=(5, 15))
                if response.status_code != 200:
                    continue
                match = re.search(r"\b(\d{1,3}(?:\.\d{1,3}){3})\b", response.text or "")
                if match:
                    return match.group(1)
            except Exception:
                continue
        return ""

    def _format_whitelist_url(self, ip: str) -> str:
        template = self.whitelist_api_url
        if "{ip}" in template:
            return template.format(ip=ip)
        if "{}" in template:
            return template.format(ip)
        sep = "&" if "?" in template else "?"
        return f"{template}{sep}ip={ip}"

    def ensure_whitelisted(self, ip_from_provider_msg: str = "") -> bool:
        if self.disabled_whitelist or not self.whitelist_api_url:
            return False
        ip = safe_text(ip_from_provider_msg) or self._get_public_ip()
        if not ip:
            print("Warning: unable to get public IP; cannot automatically add the proxy whitelist.", file=sys.stderr)
            return False
        if ip == self._last_whitelist_ip:
            return True
        url = self._format_whitelist_url(ip)
        for attempt in range(3):
            try:
                response = self._direct.get(url, timeout=(5, 30))
                text = safe_text(response.text)
                if response.status_code == 200 and (
                    "成功" in text or "ok" in text.lower() or "true" in text.lower() or "1007" in text or "已存在" in text
                ):
                    self._last_whitelist_ip = ip
                    self._save_whitelist_cache(ip)
                    print(f"Proxy whitelist confirmed: {ip}", file=sys.stderr)
                    return True
                print(f"Warning: whitelist API returned {response.status_code}: {text[:160]}", file=sys.stderr)
            except Exception as exc:
                print(f"Warning: failed to add whitelist ({attempt + 1}/3): {exc}", file=sys.stderr)
            time.sleep(2 + attempt)
        return False

    def _fetch_from_file(self) -> List[str]:
        if not self.proxy_file:
            return []
        path = Path(self.proxy_file)
        if not path.exists():
            print(f"Warning: proxy file does not exist: {path}", file=sys.stderr)
            return []
        try:
            return parse_proxy_lines(path.read_text(encoding="utf-8-sig"))
        except Exception as exc:
            print(f"Warning: failed to read proxy file: {exc}", file=sys.stderr)
            return []

    def _fetch_from_api(self) -> List[str]:
        if not self.proxy_api_url:
            return []
        for attempt in range(10):
            try:
                response = self._direct.get(self.proxy_api_url, timeout=(5, 20))
                text = safe_text(response.text)
                if response.status_code in {400, 403} and ("白名单" in text or "whitelist" in text.lower()):
                    match = re.search(r"当前IP\((\d+\.\d+\.\d+\.\d+)\)", text)
                    ip_in_msg = match.group(1) if match else ""
                    print(f"Proxy API reported a whitelist issue; current IP: {ip_in_msg or 'not parsed'}", file=sys.stderr)
                    if self.ensure_whitelisted(ip_in_msg):
                        time.sleep(6)
                        continue
                    return []
                if response.status_code != 200:
                    raise RuntimeError(f"Proxy API HTTP {response.status_code}: {text[:120]}")
                proxies = parse_proxy_lines(text)
                if proxies:
                    return proxies
                raise RuntimeError(f"Proxy API did not return host:port; response head: {text[:120]}")
            except Exception as exc:
                wait_seconds = min(30, 2 ** attempt)
                print(f"Warning: failed to fetch proxies ({attempt + 1}/10): {exc}; waiting {wait_seconds}s", file=sys.stderr)
                time.sleep(wait_seconds)
        return []

    def _fetch_proxies(self) -> List[str]:
        proxies = self._fetch_from_api()
        if proxies:
            return proxies
        return self._fetch_from_file()

    def get_proxy(self, force_refresh: bool = False) -> Optional[str]:
        with self._lock:
            now = time.time()
            expired = (now - self.proxy_start_time) > max(1, self.expire_seconds - self.rotate_guard_seconds)
            available = [proxy for proxy in self.proxy_pool if proxy not in self.failed_proxies]

            if force_refresh and now - self.last_proxy_refresh_time < self.cooldown_seconds and available:
                self.current_index = (self.current_index + 1) % len(self.proxy_pool)
                for _ in range(len(self.proxy_pool)):
                    candidate = self.proxy_pool[self.current_index % len(self.proxy_pool)]
                    self.current_index = (self.current_index + 1) % len(self.proxy_pool)
                    if candidate not in self.failed_proxies:
                        return candidate
                time.sleep(max(0.0, self.cooldown_seconds - (now - self.last_proxy_refresh_time)))

            if force_refresh or not self.proxy_pool or expired or not available:
                proxies = self._fetch_proxies()
                if proxies:
                    self.proxy_pool = proxies
                    self.failed_proxies.clear()
                    self.current_index = 0
                    self.proxy_start_time = time.time()
                    self.last_proxy_refresh_time = time.time()
                    self.fail_count = 0
                    print(f"Proxy pool refreshed: {len(proxies)} proxies", file=sys.stderr)
                else:
                    self.proxy_pool = []
                    self.failed_proxies.clear()
                    return None

            for _ in range(len(self.proxy_pool)):
                candidate = self.proxy_pool[self.current_index % len(self.proxy_pool)]
                self.current_index = (self.current_index + 1) % len(self.proxy_pool)
                if candidate not in self.failed_proxies:
                    return candidate
            return None

    def mark_proxy_failed(self, proxy: str) -> None:
        proxy = safe_text(proxy)
        if not proxy:
            return
        with self._lock:
            self.failed_proxies.add(proxy)
            available = len([item for item in self.proxy_pool if item not in self.failed_proxies])
            print(f"Marked proxy as failed: {proxy}; available remaining {available}", file=sys.stderr)

    def mark_success(self) -> None:
        with self._lock:
            self.fail_count = 0

    def get_remaining_seconds(self) -> float:
        if not self.proxy_pool:
            return float("inf")
        elapsed = time.time() - self.proxy_start_time
        return max(0.0, self.expire_seconds - self.rotate_guard_seconds - elapsed)


def is_proxy_related_message(message: str) -> bool:
    text = safe_text(message).lower()
    if not text:
        return False
    if any(f"http {status}" in text for status in PROXY_SUSPECT_HTTP):
        return True
    return any(keyword.lower() in text for keyword in PROXY_ERROR_KEYWORDS)


def load_des_cipher():
    errors: List[str] = []
    try:
        from Crypto.Cipher import DES  # type: ignore

        return ("pycryptodome", DES, DES_KEY)
    except Exception as exc:
        errors.append(f"pycryptodome DES unavailable: {exc}")

    try:
        from cryptography.hazmat.backends import default_backend  # type: ignore
        from cryptography.hazmat.primitives.ciphers import Cipher, modes  # type: ignore

        candidate_modules = [
            "cryptography.hazmat.decrepit.ciphers.algorithms",
            "cryptography.hazmat.primitives.ciphers.algorithms",
        ]
        for module_name in candidate_modules:
            try:
                algorithms = importlib.import_module(module_name)
            except Exception as exc:
                errors.append(f"{module_name} unavailable: {exc}")
                continue
            for algorithm_name, key in [("DES", DES_KEY), ("TripleDES", DES_KEY * 3)]:
                algorithm = getattr(algorithms, algorithm_name, None)
                if algorithm is None:
                    continue
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        Cipher(algorithm(key), modes.ECB(), backend=default_backend()).decryptor()
                    return ("cryptography", (Cipher, modes, default_backend, algorithm), key)
                except Exception as exc:
                    errors.append(f"{module_name}.{algorithm_name} failed: {exc}")

    except Exception as exc:
        errors.append(f"cryptography import failed: {exc}")

    return ("pure_python", errors, DES_KEY)


DES_BACKEND = None


DES_IP = [
    58, 50, 42, 34, 26, 18, 10, 2,
    60, 52, 44, 36, 28, 20, 12, 4,
    62, 54, 46, 38, 30, 22, 14, 6,
    64, 56, 48, 40, 32, 24, 16, 8,
    57, 49, 41, 33, 25, 17, 9, 1,
    59, 51, 43, 35, 27, 19, 11, 3,
    61, 53, 45, 37, 29, 21, 13, 5,
    63, 55, 47, 39, 31, 23, 15, 7,
]

DES_FP = [
    40, 8, 48, 16, 56, 24, 64, 32,
    39, 7, 47, 15, 55, 23, 63, 31,
    38, 6, 46, 14, 54, 22, 62, 30,
    37, 5, 45, 13, 53, 21, 61, 29,
    36, 4, 44, 12, 52, 20, 60, 28,
    35, 3, 43, 11, 51, 19, 59, 27,
    34, 2, 42, 10, 50, 18, 58, 26,
    33, 1, 41, 9, 49, 17, 57, 25,
]

DES_E = [
    32, 1, 2, 3, 4, 5,
    4, 5, 6, 7, 8, 9,
    8, 9, 10, 11, 12, 13,
    12, 13, 14, 15, 16, 17,
    16, 17, 18, 19, 20, 21,
    20, 21, 22, 23, 24, 25,
    24, 25, 26, 27, 28, 29,
    28, 29, 30, 31, 32, 1,
]

DES_P = [
    16, 7, 20, 21, 29, 12, 28, 17,
    1, 15, 23, 26, 5, 18, 31, 10,
    2, 8, 24, 14, 32, 27, 3, 9,
    19, 13, 30, 6, 22, 11, 4, 25,
]

DES_PC1 = [
    57, 49, 41, 33, 25, 17, 9,
    1, 58, 50, 42, 34, 26, 18,
    10, 2, 59, 51, 43, 35, 27,
    19, 11, 3, 60, 52, 44, 36,
    63, 55, 47, 39, 31, 23, 15,
    7, 62, 54, 46, 38, 30, 22,
    14, 6, 61, 53, 45, 37, 29,
    21, 13, 5, 28, 20, 12, 4,
]

DES_PC2 = [
    14, 17, 11, 24, 1, 5,
    3, 28, 15, 6, 21, 10,
    23, 19, 12, 4, 26, 8,
    16, 7, 27, 20, 13, 2,
    41, 52, 31, 37, 47, 55,
    30, 40, 51, 45, 33, 48,
    44, 49, 39, 56, 34, 53,
    46, 42, 50, 36, 29, 32,
]

DES_SHIFTS = [1, 1, 2, 2, 2, 2, 2, 2, 1, 2, 2, 2, 2, 2, 2, 1]

DES_SBOXES = [
    [
        [14, 4, 13, 1, 2, 15, 11, 8, 3, 10, 6, 12, 5, 9, 0, 7],
        [0, 15, 7, 4, 14, 2, 13, 1, 10, 6, 12, 11, 9, 5, 3, 8],
        [4, 1, 14, 8, 13, 6, 2, 11, 15, 12, 9, 7, 3, 10, 5, 0],
        [15, 12, 8, 2, 4, 9, 1, 7, 5, 11, 3, 14, 10, 0, 6, 13],
    ],
    [
        [15, 1, 8, 14, 6, 11, 3, 4, 9, 7, 2, 13, 12, 0, 5, 10],
        [3, 13, 4, 7, 15, 2, 8, 14, 12, 0, 1, 10, 6, 9, 11, 5],
        [0, 14, 7, 11, 10, 4, 13, 1, 5, 8, 12, 6, 9, 3, 2, 15],
        [13, 8, 10, 1, 3, 15, 4, 2, 11, 6, 7, 12, 0, 5, 14, 9],
    ],
    [
        [10, 0, 9, 14, 6, 3, 15, 5, 1, 13, 12, 7, 11, 4, 2, 8],
        [13, 7, 0, 9, 3, 4, 6, 10, 2, 8, 5, 14, 12, 11, 15, 1],
        [13, 6, 4, 9, 8, 15, 3, 0, 11, 1, 2, 12, 5, 10, 14, 7],
        [1, 10, 13, 0, 6, 9, 8, 7, 4, 15, 14, 3, 11, 5, 2, 12],
    ],
    [
        [7, 13, 14, 3, 0, 6, 9, 10, 1, 2, 8, 5, 11, 12, 4, 15],
        [13, 8, 11, 5, 6, 15, 0, 3, 4, 7, 2, 12, 1, 10, 14, 9],
        [10, 6, 9, 0, 12, 11, 7, 13, 15, 1, 3, 14, 5, 2, 8, 4],
        [3, 15, 0, 6, 10, 1, 13, 8, 9, 4, 5, 11, 12, 7, 2, 14],
    ],
    [
        [2, 12, 4, 1, 7, 10, 11, 6, 8, 5, 3, 15, 13, 0, 14, 9],
        [14, 11, 2, 12, 4, 7, 13, 1, 5, 0, 15, 10, 3, 9, 8, 6],
        [4, 2, 1, 11, 10, 13, 7, 8, 15, 9, 12, 5, 6, 3, 0, 14],
        [11, 8, 12, 7, 1, 14, 2, 13, 6, 15, 0, 9, 10, 4, 5, 3],
    ],
    [
        [12, 1, 10, 15, 9, 2, 6, 8, 0, 13, 3, 4, 14, 7, 5, 11],
        [10, 15, 4, 2, 7, 12, 9, 5, 6, 1, 13, 14, 0, 11, 3, 8],
        [9, 14, 15, 5, 2, 8, 12, 3, 7, 0, 4, 10, 1, 13, 11, 6],
        [4, 3, 2, 12, 9, 5, 15, 10, 11, 14, 1, 7, 6, 0, 8, 13],
    ],
    [
        [4, 11, 2, 14, 15, 0, 8, 13, 3, 12, 9, 7, 5, 10, 6, 1],
        [13, 0, 11, 7, 4, 9, 1, 10, 14, 3, 5, 12, 2, 15, 8, 6],
        [1, 4, 11, 13, 12, 3, 7, 14, 10, 15, 6, 8, 0, 5, 9, 2],
        [6, 11, 13, 8, 1, 4, 10, 7, 9, 5, 0, 15, 14, 2, 3, 12],
    ],
    [
        [13, 2, 8, 4, 6, 15, 11, 1, 10, 9, 3, 14, 5, 0, 12, 7],
        [1, 15, 13, 8, 10, 3, 7, 4, 12, 5, 6, 11, 0, 14, 9, 2],
        [7, 11, 4, 1, 9, 12, 14, 2, 0, 6, 10, 13, 15, 3, 5, 8],
        [2, 1, 14, 7, 4, 10, 8, 13, 15, 12, 9, 0, 3, 5, 6, 11],
    ],
]


def des_permute(value: int, table: Sequence[int], input_bits: int) -> int:
    output = 0
    for position in table:
        output = (output << 1) | ((value >> (input_bits - position)) & 1)
    return output


def des_left_rotate_28(value: int, shift: int) -> int:
    return ((value << shift) & 0x0FFFFFFF) | (value >> (28 - shift))


def des_subkeys(key: bytes) -> List[int]:
    key_int = int.from_bytes(key, "big")
    permuted = des_permute(key_int, DES_PC1, 64)
    c = (permuted >> 28) & 0x0FFFFFFF
    d = permuted & 0x0FFFFFFF
    keys = []
    for shift in DES_SHIFTS:
        c = des_left_rotate_28(c, shift)
        d = des_left_rotate_28(d, shift)
        keys.append(des_permute((c << 28) | d, DES_PC2, 56))
    return keys


def des_feistel(right: int, subkey: int) -> int:
    expanded = des_permute(right, DES_E, 32) ^ subkey
    output = 0
    for box_index in range(8):
        chunk = (expanded >> (42 - 6 * box_index)) & 0x3F
        row = ((chunk & 0x20) >> 4) | (chunk & 0x01)
        col = (chunk >> 1) & 0x0F
        output = (output << 4) | DES_SBOXES[box_index][row][col]
    return des_permute(output, DES_P, 32)


def des_decrypt_block(block: bytes, keys: Sequence[int]) -> bytes:
    value = int.from_bytes(block, "big")
    permuted = des_permute(value, DES_IP, 64)
    left = (permuted >> 32) & 0xFFFFFFFF
    right = permuted & 0xFFFFFFFF
    for subkey in reversed(keys):
        left, right = right, left ^ des_feistel(right, subkey)
    plain = des_permute((right << 32) | left, DES_FP, 64)
    return plain.to_bytes(8, "big")


def pure_python_des_ecb_decrypt(raw: bytes, key: bytes) -> bytes:
    if len(key) != 8:
        raise ValueError("DES key must be 8 bytes")
    if len(raw) % 8:
        raise ValueError("DES ciphertext length must be a multiple of 8")
    keys = des_subkeys(key)
    return b"".join(des_decrypt_block(raw[i : i + 8], keys) for i in range(0, len(raw), 8))


def decrypt_des_base64(text: str) -> str:
    global DES_BACKEND
    if DES_BACKEND is None:
        DES_BACKEND = load_des_cipher()

    backend_name, backend_obj, key = DES_BACKEND
    raw = base64.b64decode(text)
    if backend_name == "pycryptodome":
        cipher = backend_obj.new(key, backend_obj.MODE_ECB)
        decrypted = cipher.decrypt(raw)
    elif backend_name == "pure_python":
        decrypted = pure_python_des_ecb_decrypt(raw, key)
    else:
        Cipher, modes, default_backend, algorithm = backend_obj
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            decryptor = Cipher(algorithm(key), modes.ECB(), backend=default_backend()).decryptor()
            decrypted = decryptor.update(raw) + decryptor.finalize()

    if not decrypted:
        return ""
    pad = decrypted[-1]
    if 1 <= pad <= 8:
        decrypted = decrypted[:-pad]
    return decrypted.decode("utf-8")


def decode_nsfc_response(response: requests.Response) -> Dict[str, Any]:
    text = response.text.strip()
    if not text:
        raise DecodeError("empty response")
    try:
        return response.json()
    except Exception:
        pass

    try:
        decrypted = decrypt_des_base64(text)
        return json.loads(decrypted)
    except Exception as exc:
        snippet = text[:120].replace("\n", " ")
        raise DecodeError(f"Failed to parse response: {type(exc).__name__}: {exc}; response_head={snippet}") from exc


def make_session(proxy_ip: str = "") -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    proxy_ip = safe_text(proxy_ip)
    if proxy_ip:
        proxy_url = normalize_proxy_url(proxy_ip)
        session.proxies = {"http": proxy_url, "https": proxy_url}
    setattr(session, "_nsfc_proxy_ip", proxy_ip)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Origin": BASE_URL,
            "Referer": f"{BASE_URL}/finalProjectInit?advanced=true",
            "Authorization": "Bearer false",
        }
    )
    response = session.get(f"{BASE_URL}/finalProjectInit?advanced=true", timeout=30)
    if response.status_code in PROXY_SUSPECT_HTTP:
        raise RuntimeError(f"Initial page HTTP {response.status_code}")
    return session


THREAD_LOCAL = threading.local()


def get_session_proxy(session: requests.Session) -> str:
    return safe_text(getattr(session, "_nsfc_proxy_ip", ""))


def create_session_for_args(args: argparse.Namespace, force_refresh_proxy: bool = False) -> requests.Session:
    proxy_manager: Optional[ProxyManager] = getattr(args, "proxy_manager", None)
    attempts = args.proxy_session_attempts if proxy_manager else 1
    last_error: Optional[BaseException] = None
    for attempt in range(attempts):
        proxy_ip = ""
        if proxy_manager:
            proxy_ip = proxy_manager.get_proxy(force_refresh=force_refresh_proxy or attempt > 0) or ""
            if not proxy_ip:
                raise RuntimeError("Proxy is enabled, but no proxy is available. Check --proxy-api-url or --proxy-file.")
        try:
            session = make_session(proxy_ip)
            if proxy_ip:
                print(f"Using proxy: {proxy_ip}", file=sys.stderr)
            return session
        except Exception as exc:
            last_error = exc
            if proxy_manager and proxy_ip:
                proxy_manager.mark_proxy_failed(proxy_ip)
                time.sleep(min(5, attempt + 1))
                continue
            raise
    raise RuntimeError(f"Failed to create request session: {last_error}")


def get_worker_session(args: argparse.Namespace, force_refresh_proxy: bool = False) -> requests.Session:
    session = getattr(THREAD_LOCAL, "session", None)
    proxy_manager: Optional[ProxyManager] = getattr(args, "proxy_manager", None)
    if proxy_manager and proxy_manager.get_remaining_seconds() <= 0:
        force_refresh_proxy = True
    if session is None or force_refresh_proxy:
        session = create_session_for_args(args, force_refresh_proxy=force_refresh_proxy)
        THREAD_LOCAL.session = session
    return session


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    retries: int,
    timeout: int,
    **kwargs: Any,
) -> requests.Response:
    last_error: Optional[BaseException] = None
    for attempt in range(retries + 1):
        try:
            response = session.request(method, url, timeout=timeout, **kwargs)
            if response.status_code in {429, 500, 502, 503, 504} and attempt < retries:
                wait_seconds = min(60, 2 ** attempt + random.uniform(0, attempt + 2))
                time.sleep(wait_seconds)
                continue
            return response
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= retries:
                break
            wait_seconds = min(60, 2 ** attempt + random.uniform(0, attempt + 2))
            time.sleep(wait_seconds)
    raise RuntimeError(f"Request failed: {url}: {last_error}")


def build_topic_terms() -> List[Tuple[str, str]]:
    terms: List[Tuple[str, str]] = []
    terms.extend(("core", term) for term in CORE_SUSTAINABILITY_TERMS)
    terms.extend(("environment_climate", term) for term in ENVIRONMENT_CLIMATE_TERMS)
    terms.extend(("social_equity", term) for term in SOCIAL_EQUITY_TERMS)
    terms.extend(("space_infra", term) for term in SPACE_INFRA_TERMS)
    return terms


def build_builtin_queries(search_field: str, year_field: str, years: Sequence[str]) -> List[QuerySpec]:
    base_queries: List[QuerySpec] = []
    query_no = 1
    for city in CITY_TERMS:
        for group, topic in build_topic_terms():
            base_queries.append(
                QuerySpec(
                    query_id=f"q{query_no:04d}",
                    preset="city-sustainability",
                    query_type="city_topic_combination",
                    city_term=city,
                    topic_group=group,
                    topic_term=topic,
                    query_keyword=f"{city} {topic}",
                    search_field=search_field,
                )
            )
            query_no += 1

    for term in STANDALONE_URBAN_TERMS:
        base_queries.append(
            QuerySpec(
                query_id=f"q{query_no:04d}",
                preset="city-sustainability",
                query_type="standalone_urban_semantic_term",
                city_term="",
                topic_group="standalone",
                topic_term=term,
                query_keyword=term,
                search_field=search_field,
            )
        )
        query_no += 1

    if not years or not year_field:
        return base_queries

    expanded: List[QuerySpec] = []
    for spec in base_queries:
        for year in years:
            expanded.append(
                QuerySpec(
                    query_id=f"{spec.query_id}_{year}",
                    preset=spec.preset,
                    query_type=spec.query_type,
                    city_term=spec.city_term,
                    topic_group=spec.topic_group,
                    topic_term=spec.topic_term,
                    query_keyword=spec.query_keyword,
                    search_field=spec.search_field,
                    year_field=year_field,
                    year_value=str(year),
                )
            )
    return expanded


def selected_keyword_fields(search_field: str, search_mode: str) -> List[str]:
    if search_mode == "fuzzy":
        return ["fuzzy"]
    if search_field in {"", "both", "fuzzy"}:
        return ["projectName", "keywords"]
    return [search_field]


def advanced_payload_keyword(spec: QuerySpec, mode: str) -> str:
    if mode == "full":
        return spec.query_keyword
    if spec.query_type == "city_topic_combination" and spec.topic_term:
        return spec.topic_term
    return spec.query_keyword


def build_local_match_specs(args: argparse.Namespace) -> List[QuerySpec]:
    if args.keywords_file:
        return load_queries_from_file(Path(args.keywords_file), args.search_field, "", [])
    return build_builtin_queries(args.search_field, "", [])


def compact_anchor_terms(terms: Sequence[str]) -> List[str]:
    anchors: List[str] = []
    for term in terms:
        text = safe_text(term)
        if not text:
            continue
        if any(existing in text for existing in anchors):
            continue
        anchors.append(text)
    return anchors


def stable_id_part(value: str) -> str:
    text = safe_text(value)
    if not text:
        return "empty"
    return hashlib.md5(text.encode("utf-8")).hexdigest()[:8]


def build_grid_scan_anchors(args: argparse.Namespace) -> List[str]:
    if args.keywords_file:
        custom_specs = load_queries_from_file(Path(args.keywords_file), args.search_field, "", [])
        terms = [spec.query_keyword for spec in custom_specs]
        return compact_anchor_terms(terms) if args.anchor_mode == "compact" else list(dict.fromkeys(terms))

    if args.anchor_mode == "full":
        return list(dict.fromkeys([*CITY_TERMS, *STANDALONE_URBAN_TERMS]))

    city_anchors = compact_anchor_terms(CITY_TERMS)
    standalone_anchors = [
        term
        for term in STANDALONE_URBAN_TERMS
        if not any(anchor in term for anchor in city_anchors)
    ]
    return list(dict.fromkeys([*city_anchors, *compact_anchor_terms(standalone_anchors)]))


def build_grid_scan_specs(year_field: str, years: Sequence[str], args: argparse.Namespace) -> List[QuerySpec]:
    years_to_use = list(years) if year_field and years else [""]
    specs: List[QuerySpec] = []
    anchors = build_grid_scan_anchors(args)
    fields = selected_keyword_fields(args.search_field, args.search_mode)
    for anchor_index, anchor in enumerate(anchors, start=1):
        for year in years_to_use:
            for field_name in fields:
                field_alias = "name" if field_name == "projectName" else "kw"
                for project_type_name, project_type_value in ADVANCED_PROJECT_TYPES:
                    for apply_code in ADVANCED_CODES_2ND:
                        if (
                            args.skip_tianyuan_non_a
                            and project_type_name == "数学天元基金项目"
                            and apply_code
                            and not apply_code.startswith("A")
                        ):
                            continue
                        query_id = f"scan_a{anchor_index:03d}_{stable_id_part(anchor)}_{year or 'all'}_{field_alias}_{project_type_value or 'all'}_{apply_code or 'all'}"
                        specs.append(
                            QuerySpec(
                                query_id=query_id,
                                preset="grid-scan",
                                query_type="grid_scan",
                                city_term="",
                                topic_group="grid-scan",
                                topic_term=anchor,
                                query_keyword=anchor,
                                query_label=f"锚词:{anchor}",
                                payload_keyword=anchor,
                                search_field=field_name,
                                year_field=year_field if year else "",
                                year_value=str(year) if year else "",
                                project_type=project_type_value,
                                project_type_name=project_type_name,
                                apply_code=apply_code,
                            )
                        )
    return specs


def build_smart_grid_payload_terms(args: argparse.Namespace) -> List[str]:
    match_specs = build_local_match_specs(args)
    terms: List[str] = []
    for spec in match_specs:
        if spec.query_type == "city_topic_combination":
            terms.extend([spec.city_term, spec.topic_term])
            if args.advanced_keyword_mode == "full":
                terms.append(spec.query_keyword)
        else:
            terms.append(spec.topic_term or spec.query_keyword)
    return list(dict.fromkeys(term for term in terms if term))


def build_smart_grid_specs(year_field: str, years: Sequence[str], args: argparse.Namespace) -> List[QuerySpec]:
    years_to_use = list(years) if year_field and years else [""]
    fields = selected_keyword_fields(args.search_field, args.search_mode)
    payload_terms = build_smart_grid_payload_terms(args)
    specs: List[QuerySpec] = []
    for term_index, payload_term in enumerate(payload_terms, start=1):
        for year in years_to_use:
            for field_name in fields:
                field_alias = "name" if field_name == "projectName" else "kw"
                for project_type_name, project_type_value in ADVANCED_PROJECT_TYPES:
                    for apply_code in ADVANCED_CODES_2ND:
                        if (
                            args.skip_tianyuan_non_a
                            and project_type_name == "数学天元基金项目"
                            and apply_code
                            and not apply_code.startswith("A")
                        ):
                            continue
                        query_id = f"smart_t{term_index:03d}_{stable_id_part(payload_term)}_{year or 'all'}_{field_alias}_{project_type_value or 'all'}_{apply_code or 'all'}"
                        specs.append(
                            QuerySpec(
                                query_id=query_id,
                                preset="smart-grid",
                                query_type="smart_grid",
                                city_term="",
                                topic_group="smart-grid",
                                topic_term=payload_term,
                                query_keyword=payload_term,
                                query_label=f"去重词:{payload_term}",
                                payload_keyword=payload_term,
                                search_field=field_name,
                                year_field=year_field if year else "",
                                year_value=str(year) if year else "",
                                project_type=project_type_value,
                                project_type_name=project_type_name,
                                apply_code=apply_code,
                            )
                        )
    return specs


def expand_search_specs(specs: Sequence[QuerySpec], args: argparse.Namespace) -> List[QuerySpec]:
    fields = selected_keyword_fields(args.search_field, args.search_mode)
    expanded: List[QuerySpec] = []

    for spec in specs:
        if args.search_mode == "fuzzy":
            expanded.append(
                QuerySpec(
                    query_id=f"{spec.query_id}_fuzzy",
                    preset=spec.preset,
                    query_type=spec.query_type,
                    city_term=spec.city_term,
                    topic_group=spec.topic_group,
                    topic_term=spec.topic_term,
                    query_keyword=spec.query_keyword,
                    query_label=spec.query_keyword,
                    payload_keyword=spec.query_keyword,
                    search_field="fuzzy",
                    year_field=spec.year_field,
                    year_value=spec.year_value,
                )
            )
            continue

        payload_keyword = advanced_payload_keyword(spec, args.advanced_keyword_mode)
        project_types = ADVANCED_PROJECT_TYPES if args.advanced_grid else [("", "")]
        apply_codes = ADVANCED_CODES_2ND if args.advanced_grid else [""]
        for field in fields:
            field_alias = "name" if field == "projectName" else "kw"
            for project_type_name, project_type_value in project_types:
                for apply_code in apply_codes:
                    if (
                        args.skip_tianyuan_non_a
                        and project_type_name == "数学天元基金项目"
                        and apply_code
                        and not apply_code.startswith("A")
                    ):
                        continue
                    suffix_parts = [field_alias]
                    if project_type_value:
                        suffix_parts.append(project_type_value)
                    if apply_code:
                        suffix_parts.append(apply_code)
                    expanded.append(
                        QuerySpec(
                            query_id=f"{spec.query_id}_{'_'.join(suffix_parts)}",
                            preset=spec.preset,
                            query_type=spec.query_type,
                            city_term=spec.city_term,
                            topic_group=spec.topic_group,
                            topic_term=spec.topic_term,
                            query_keyword=spec.query_keyword,
                            query_label=spec.query_keyword,
                            payload_keyword=payload_keyword,
                            search_field=field,
                            year_field=spec.year_field,
                            year_value=spec.year_value,
                            project_type=project_type_value,
                            project_type_name=project_type_name,
                            apply_code=apply_code,
                        )
                    )
    return expanded


def load_queries_from_file(path: Path, search_field: str, year_field: str, years: Sequence[str]) -> List[QuerySpec]:
    queries: List[str] = []
    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                return []
            candidate_fields = [
                "query_keyword",
                "keyword",
                "keywords",
                "search_keyword",
                "检索词",
                "关键词",
                reader.fieldnames[0],
            ]
            field = next((name for name in candidate_fields if name in reader.fieldnames), reader.fieldnames[0])
            for row in reader:
                value = safe_text(row.get(field))
                if value:
                    queries.append(value)
    else:
        with path.open("r", encoding="utf-8-sig") as handle:
            for line in handle:
                value = line.strip()
                if value and not value.startswith("#"):
                    queries.append(value)

    specs: List[QuerySpec] = []
    years_to_use = list(years) if year_field and years else [""]
    for idx, keyword in enumerate(dict.fromkeys(queries), start=1):
        for year in years_to_use:
            suffix = f"_{year}" if year else ""
            specs.append(
                QuerySpec(
                    query_id=f"custom{idx:04d}{suffix}",
                    preset="custom",
                    query_type="custom_keyword",
                    city_term="",
                    topic_group="custom",
                    topic_term=keyword,
                    query_keyword=keyword,
                    search_field=search_field,
                    year_field=year_field if year else "",
                    year_value=str(year) if year else "",
                )
            )
    return specs


def build_years(args: argparse.Namespace) -> List[str]:
    if args.year_field == "none":
        return []
    if args.years:
        years: List[str] = []
        for item in args.years:
            years.extend(part.strip() for part in item.split(",") if part.strip())
        return list(dict.fromkeys(years))
    if args.start_year and args.end_year:
        return [str(year) for year in range(args.start_year, args.end_year + 1)]
    return []


def build_search_payload(spec: QuerySpec, page_num: int, page_size: int) -> Dict[str, Any]:
    payload_keyword = spec.payload_keyword or spec.query_keyword
    payload: Dict[str, Any] = {
        "code": spec.apply_code,
        "fuzzyKeyword": "",
        "complete": True,
        "isFuzzySearch": spec.search_field == "fuzzy",
        "conclusionYear": "",
        "dependUnit": "",
        "keywords": "",
        "pageNum": page_num,
        "pageSize": page_size,
        "personInCharge": "",
        "projectName": "",
        "projectType": spec.project_type,
        "subPType": "",
        "psPType": "",
        "ratifyNo": "",
        "ratifyYear": "",
        "order": "enddate",
        "ordering": "desc",
        "codeScreening": "",
        "dependUnitScreening": "",
        "keywordsScreening": "",
        "projectTypeNameScreening": "",
    }
    if spec.search_field == "projectName":
        payload["projectName"] = payload_keyword
    elif spec.search_field == "keywords":
        payload["keywords"] = payload_keyword
    else:
        payload["fuzzyKeyword"] = payload_keyword

    if spec.year_field in {"conclusionYear", "ratifyYear"} and spec.year_value:
        payload[spec.year_field] = spec.year_value
    return payload


def parse_date_range(value: str) -> Tuple[str, str]:
    text = safe_text(value)
    if not text:
        return "", ""
    dates = re.findall(r"\d{4}-\d{2}-\d{2}", text)
    if len(dates) >= 2:
        return dates[0], dates[1]
    if len(dates) == 1:
        return dates[0], ""
    return "", ""


def normalize_result_type(value: str) -> str:
    text = safe_text(value)
    mapping = {
        "期刊": "期刊论文",
        "期刊论文": "期刊论文",
        "会议": "会议论文",
        "会议论文": "会议论文",
        "专著": "专著",
        "学术专著": "专著",
        "著作": "专著",
        "奖励": "奖励",
        "科研奖励": "奖励",
        "专利": "专利",
    }
    return mapping.get(text, text)


def count_achievement_types(results_list: Any) -> Dict[str, int]:
    counter: Counter[str] = Counter()
    if not isinstance(results_list, list):
        return {}
    for item in results_list:
        result = item.get("result") if isinstance(item, dict) else None
        if not isinstance(result, list):
            continue
        result_type = normalize_result_type(safe_get(result, 3))
        if result_type:
            counter[result_type] += 1
    return dict(counter)


def result_row_to_project(row: Sequence[Any]) -> Dict[str, Any]:
    detail_id = safe_text(safe_get(row, 0))
    approval_no = safe_text(safe_get(row, 2))
    return {
        "detail_id": detail_id,
        "approval_no": approval_no,
        "project_name": safe_text(safe_get(row, 1)),
        "project_type": safe_text(safe_get(row, 3)),
        "depend_unit": safe_text(safe_get(row, 4)),
        "project_admin": safe_text(safe_get(row, 5)),
        "support_num": safe_text(safe_get(row, 6)),
        "ratify_year": safe_text(safe_get(row, 7)),
        "conclusion_year": safe_text(safe_get(row, 15)),
        "apply_code": safe_text(safe_get(row, 14)),
        "project_keywords": safe_text(safe_get(row, 8)).replace("；", ";"),
        "research_start_date": "",
        "research_end_date": "",
        "project_abstract_cn": "",
        "project_abstract_en": "",
        "conclusion_abstract": "",
        "has_report": safe_text(safe_get(row, 13)),
        "journal_paper_count": "",
        "conference_paper_count": "",
        "book_count": "",
        "reward_count": "",
        "patent_count": "",
        "result_total_count": "",
        "results_list_json": "",
        "query_matches": "",
        "query_count": "0",
        "search_fields": "",
        "search_payload_keywords": "",
        "search_project_types": "",
        "search_project_type_codes": "",
        "search_apply_codes": "",
        "search_year_filters": "",
        "source_url": f"{BASE_URL}/finalDetails?id={detail_id}" if detail_id else "",
        "last_seen_at": now_iso(),
    }


def append_unique_value(row: Dict[str, Any], field: str, value: str) -> None:
    text = safe_text(value)
    if not text:
        return
    existing = [item for item in safe_text(row.get(field)).split("|") if item]
    if text not in existing:
        existing.append(text)
    row[field] = "|".join(existing)


def project_match_text(project: Dict[str, Any]) -> str:
    return "".join(
        safe_text(project.get(field))
        for field in ["project_name", "project_keywords", "project_abstract_cn", "conclusion_abstract"]
    )


def single_match_terms(match_specs: Sequence[QuerySpec]) -> List[str]:
    terms: List[str] = []
    for match_spec in match_specs:
        if match_spec.query_type == "city_topic_combination":
            terms.extend([match_spec.city_term, match_spec.topic_term])
        else:
            terms.append(match_spec.topic_term or match_spec.query_keyword)
    return list(dict.fromkeys(term for term in terms if safe_text(term)))


def local_keyword_match_labels(project: Dict[str, Any], args: argparse.Namespace) -> List[str]:
    text = project_match_text(project)
    if not text:
        return []
    match_specs: Sequence[QuerySpec] = getattr(args, "local_match_specs", [])
    labels: List[str] = []

    # Combination terms are only used as supplemental labels, no longer as the sole filter condition.
    for match_spec in match_specs:
        label = match_spec.query_label or match_spec.query_keyword
        if not label:
            continue
        if match_spec.query_type == "city_topic_combination":
            if match_spec.city_term and match_spec.topic_term and match_spec.city_term in text and match_spec.topic_term in text:
                labels.append(label)
        else:
            keyword = match_spec.topic_term or match_spec.query_keyword
            if keyword and keyword in text:
                labels.append(label)

    # Single-keyword hits are also retained to avoid treating city+topic combinations as exclusion rules.
    for term in single_match_terms(match_specs):
        if term in text:
            single_label = f"单关键词:{term}"
            if single_label not in labels and term not in labels:
                labels.append(single_label)
    return list(dict.fromkeys(labels))


def merge_query_match(project: Dict[str, Any], spec: QuerySpec) -> None:
    match = spec.query_label or spec.query_keyword
    details = []
    if spec.payload_keyword and spec.payload_keyword != match:
        details.append(f"search={spec.payload_keyword}")
    if spec.search_field:
        details.append(f"field={SEARCH_FIELD_LABELS.get(spec.search_field, spec.search_field)}")
    if spec.year_value:
        details.append(f"{spec.year_field}:{spec.year_value}")
    if spec.project_type_name:
        details.append(f"type={spec.project_type_name}")
    if spec.apply_code:
        details.append(f"code={spec.apply_code}")
    if details:
        match = f"{match}@{','.join(details)}"
    existing = [item for item in safe_text(project.get("query_matches")).split("|") if item]
    if match not in existing:
        existing.append(match)
    project["query_matches"] = "|".join(existing)
    project["query_count"] = str(len(existing))
    append_unique_value(project, "search_fields", SEARCH_FIELD_LABELS.get(spec.search_field, spec.search_field))
    append_unique_value(project, "search_payload_keywords", spec.payload_keyword or spec.query_keyword)
    append_unique_value(project, "search_project_types", spec.project_type_name)
    append_unique_value(project, "search_project_type_codes", spec.project_type)
    append_unique_value(project, "search_apply_codes", spec.apply_code)
    if spec.year_value:
        append_unique_value(project, "search_year_filters", f"{spec.year_field}:{spec.year_value}")


def merge_local_scan_matches(project: Dict[str, Any], spec: QuerySpec, labels: Sequence[str]) -> None:
    mode_label = "去重关键词网格" if spec.query_type == "smart_grid" else "本地筛选"
    details = [f"mode={mode_label}"]
    if spec.year_value:
        details.append(f"{spec.year_field}:{spec.year_value}")
    if spec.project_type_name:
        details.append(f"type={spec.project_type_name}")
    if spec.apply_code:
        details.append(f"code={spec.apply_code}")
    suffix = f"@{','.join(details)}"
    existing = [item for item in safe_text(project.get("query_matches")).split("|") if item]
    for label in labels:
        match = f"{label}{suffix}"
        if match not in existing:
            existing.append(match)
    project["query_matches"] = "|".join(existing)
    project["query_count"] = str(len(existing))
    append_unique_value(project, "search_fields", SEARCH_FIELD_LABELS.get(spec.search_field, spec.search_field))
    append_unique_value(project, "search_payload_keywords", spec.payload_keyword or "锚词扫描")
    append_unique_value(project, "search_project_types", spec.project_type_name)
    append_unique_value(project, "search_project_type_codes", spec.project_type)
    append_unique_value(project, "search_apply_codes", spec.apply_code)
    if spec.year_value:
        append_unique_value(project, "search_year_filters", f"{spec.year_field}:{spec.year_value}")


def project_matches_query(project: Dict[str, Any], spec: QuerySpec, args: argparse.Namespace) -> bool:
    if not args.local_filter:
        return True
    if spec.query_type in {"grid_scan", "smart_grid"}:
        labels = local_keyword_match_labels(project, args)
        project["_matched_query_labels"] = labels
        return bool(labels)
    if spec.query_type != "city_topic_combination":
        return True
    if not spec.city_term or not spec.topic_term:
        return True
    text = project_match_text(project)
    return spec.city_term in text and spec.topic_term in text


def apply_detail(project: Dict[str, Any], detail: Dict[str, Any]) -> None:
    if not detail:
        return
    project["project_name"] = safe_text(detail.get("projectName")) or project.get("project_name", "")
    project["approval_no"] = safe_text(detail.get("ratifyNo")) or project.get("approval_no", "")
    project["apply_code"] = safe_text(detail.get("code")) or project.get("apply_code", "")
    project["project_admin"] = safe_text(detail.get("projectAdmin")) or project.get("project_admin", "")
    project["depend_unit"] = safe_text(detail.get("dependUnit")) or project.get("depend_unit", "")
    project["support_num"] = safe_text(detail.get("supportNum")) or project.get("support_num", "")
    project["project_keywords"] = safe_text(detail.get("projectKeywordC")) or project.get("project_keywords", "")
    project["project_abstract_cn"] = safe_text(detail.get("projectAbstractC"))
    project["project_abstract_en"] = safe_text(detail.get("projectAbstractE"))
    project["conclusion_abstract"] = safe_text(detail.get("conclusionAbstract"))
    project["has_report"] = str(bool(detail.get("hasReport")))

    start_date, end_date = parse_date_range(safe_text(detail.get("researchTimeScope")))
    project["research_start_date"] = start_date
    project["research_end_date"] = end_date

    if start_date and not project.get("ratify_year"):
        project["ratify_year"] = start_date[:4]
    if end_date and not project.get("conclusion_year"):
        project["conclusion_year"] = end_date[:4]

    results_list = detail.get("resultsList")
    counts = count_achievement_types(results_list)
    project["journal_paper_count"] = str(counts.get("期刊论文", 0))
    project["conference_paper_count"] = str(counts.get("会议论文", 0))
    project["book_count"] = str(counts.get("专著", 0))
    project["reward_count"] = str(counts.get("奖励", 0))
    project["patent_count"] = str(counts.get("专利", 0))
    project["result_total_count"] = str(len(results_list) if isinstance(results_list, list) else 0)
    if isinstance(results_list, list):
        project["results_list_json"] = json.dumps(results_list, ensure_ascii=False, separators=(",", ":"))


def read_csv_index(path: Path, key_field: str) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows: Dict[str, Dict[str, Any]] = {}
        for row in reader:
            key = safe_text(row.get(key_field))
            if key:
                rows[key] = dict(row)
        return rows


def read_completed_queries(path: Path) -> set[str]:
    if not path.exists():
        return set()
    completed: set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if safe_text(row.get("status")) == "done":
                completed.add(safe_text(row.get("query_id")))
    return completed


def manifest_mode(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                return safe_text(row.get("preset"))
    except Exception:
        return ""
    return ""


def write_csv(path: Path, rows: Iterable[Dict[str, Any]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    tmp = path.with_name(f"{path.stem}.{stamp}.tmp")
    with tmp.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fields), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})

    last_error: Optional[PermissionError] = None
    for attempt in range(8):
        try:
            tmp.replace(path)
            return
        except PermissionError as exc:
            last_error = exc
            time.sleep(0.5 * (attempt + 1))

    fallback = path.with_name(f"{path.stem}_autosave_{datetime.now().strftime('%Y%m%d_%H%M%S')}{path.suffix}")
    try:
        tmp.replace(fallback)
    except PermissionError as exc:
        raise last_error or exc from exc
    print(f"Warning: cannot overwrite {path}; it may be open in Excel/WPS; saved as {fallback}", file=sys.stderr)


def append_progress(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PROGRESS_FIELDS, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerow({field: row.get(field, "") for field in PROGRESS_FIELDS})


def search_page(
    session: requests.Session,
    spec: QuerySpec,
    page_num: int,
    page_size: int,
    args: argparse.Namespace,
) -> Tuple[int, List[Sequence[Any]], str]:
    payload = build_search_payload(spec, page_num, page_size)
    response = request_with_retry(
        session,
        "POST",
        f"{BASE_URL}{SEARCH_ENDPOINT}",
        json=payload,
        retries=args.retries,
        timeout=args.timeout,
    )
    if response.status_code != 200:
        return 0, [], f"HTTP {response.status_code}"
    data = decode_nsfc_response(response)
    if safe_int(data.get("code"), 0) != 200:
        return 0, [], safe_text(data.get("message")) or "API returned a non-200 code"
    payload_data = data.get("data") or {}
    if not isinstance(payload_data, dict):
        return 0, [], "data is not an object"
    total = safe_int(payload_data.get("itotalRecords"), 0)
    rows = payload_data.get("resultsData") or []
    if not isinstance(rows, list):
        rows = []
    return total, rows, ""


def fetch_detail(session: requests.Session, detail_id: str, args: argparse.Namespace) -> Dict[str, Any]:
    if not detail_id:
        return {}
    response = request_with_retry(
        session,
        "POST",
        f"{BASE_URL}{DETAIL_ENDPOINT.format(detail_id=detail_id)}",
        data="",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        retries=args.retries,
        timeout=args.timeout,
    )
    if response.status_code != 200:
        if response.status_code in PROXY_SUSPECT_HTTP:
            raise RuntimeError(f"HTTP {response.status_code}")
        return {}
    data = decode_nsfc_response(response)
    if safe_int(data.get("code"), 0) != 200:
        message = safe_text(data.get("message")) or "detail API returned a non-200 code"
        if is_proxy_related_message(message):
            raise RuntimeError(message)
        return {}
    detail = data.get("data")
    return detail if isinstance(detail, dict) else {}


MERGE_IF_MISSING_FIELDS = [
    "detail_id",
    "project_name",
    "project_type",
    "depend_unit",
    "project_admin",
    "support_num",
    "ratify_year",
    "conclusion_year",
    "apply_code",
    "project_keywords",
    "has_report",
    "source_url",
]


def spec_summary(spec: QuerySpec) -> str:
    return (
        f"{spec.query_id} {spec.query_keyword} "
        f"search={spec.payload_keyword or spec.query_keyword} "
        f"field={SEARCH_FIELD_LABELS.get(spec.search_field, spec.search_field)} "
        f"type={spec.project_type_name} code={spec.apply_code} {spec.year_field}:{spec.year_value}"
    )


def refresh_proxy_session(args: argparse.Namespace, session: requests.Session, reason: str) -> requests.Session:
    proxy_manager: Optional[ProxyManager] = getattr(args, "proxy_manager", None)
    if not proxy_manager:
        return session
    proxy_ip = get_session_proxy(session)
    if proxy_ip:
        print(f"Proxy error; switching: {proxy_ip}; reason: {safe_text(reason)[:120]}", file=sys.stderr)
        proxy_manager.mark_proxy_failed(proxy_ip)
    polite_sleep(args.proxy_cooldown_seconds, args.jitter)
    return get_worker_session(args, force_refresh_proxy=True)


def run_query_spec(spec: QuerySpec, args: argparse.Namespace) -> QueryRunResult:
    result = QueryRunResult(spec=spec)
    session = get_worker_session(args)
    try:
        for page_num in range(args.max_pages):
            page_message = ""
            total = 0
            rows: List[Sequence[Any]] = []
            for attempt in range(args.proxy_retries_per_request + 1):
                try:
                    total, rows, page_message = search_page(session, spec, page_num, args.page_size, args)
                except Exception as exc:
                    page_message = f"{type(exc).__name__}: {exc}"
                if not page_message:
                    proxy_manager: Optional[ProxyManager] = getattr(args, "proxy_manager", None)
                    if proxy_manager:
                        proxy_manager.mark_success()
                    break
                if (
                    attempt < args.proxy_retries_per_request
                    and getattr(args, "proxy_manager", None)
                    and is_proxy_related_message(page_message)
                ):
                    session = refresh_proxy_session(args, session, page_message)
                    continue
                break
            if page_message:
                result.status = "error"
                result.message = page_message
                break
            result.total_records = total
            result.pages_fetched += 1
            if not rows:
                break
            result.rows_seen += len(rows)
            for row in rows:
                if not isinstance(row, list):
                    continue
                project = result_row_to_project(row)
                if not safe_text(project.get("approval_no")):
                    continue
                if not project_matches_query(project, spec, args):
                    continue
                result.projects.append(project)
            if total and (page_num + 1) * args.page_size >= min(total, args.max_records_per_query):
                break
            polite_sleep(args.delay, args.jitter)
    except Exception as exc:
        result.status = "error"
        result.message = f"{type(exc).__name__}: {exc}"
    if args.workers > 1:
        polite_sleep(args.delay, args.jitter)
    return result


def merge_project(
    projects: Dict[str, Dict[str, Any]], project: Dict[str, Any], spec: QuerySpec
) -> Tuple[bool, Dict[str, Any]]:
    approval_no = safe_text(project.get("approval_no"))
    if not approval_no:
        return False, {}
    existing = projects.get(approval_no)
    is_new = existing is None
    if existing is None:
        existing = {field: project.get(field, "") for field in RESULT_FIELDS}
        projects[approval_no] = existing
    else:
        for field_name in MERGE_IF_MISSING_FIELDS:
            if project.get(field_name) and not existing.get(field_name):
                existing[field_name] = project[field_name]
    matched_labels = project.get("_matched_query_labels")
    if isinstance(matched_labels, list) and matched_labels:
        merge_local_scan_matches(existing, spec, matched_labels)
    else:
        merge_query_match(existing, spec)
    existing["last_seen_at"] = now_iso()
    return is_new, existing


def crawl(args: argparse.Namespace) -> None:
    output_dir = Path(args.output_dir)
    results_path = output_dir / "nsfc_results_requests_no_images.csv"
    query_manifest_path = output_dir / "nsfc_query_list_requests_no_images.csv"
    progress_path = output_dir / "nsfc_query_progress_requests_no_images.csv"

    years = build_years(args)
    year_field = "" if args.year_field == "none" else args.year_field
    if args.smart_grid:
        setattr(args, "local_match_specs", build_local_match_specs(args))
        specs = build_smart_grid_specs(year_field, years, args)
    elif args.grid_scan:
        setattr(args, "local_match_specs", build_local_match_specs(args))
        specs = build_grid_scan_specs(year_field, years, args)
    else:
        setattr(args, "local_match_specs", [])
        if args.keywords_file:
            specs = load_queries_from_file(Path(args.keywords_file), args.search_field, year_field, years)
        else:
            specs = build_builtin_queries(args.search_field, year_field, years)

    if args.limit_queries:
        specs = specs[: args.limit_queries]
    if not args.grid_scan and not args.smart_grid:
        specs = expand_search_specs(specs, args)
    all_specs = list(specs)
    spec_positions = {spec.query_id: idx for idx, spec in enumerate(all_specs, start=1)}
    total_specs = len(all_specs)
    completed_count = 0
    if args.skip_done:
        completed = read_completed_queries(progress_path)
        completed_count = sum(1 for spec in specs if spec.query_id in completed)
        specs = [spec for spec in specs if spec.query_id not in completed]

    current_manifest_mode = manifest_mode(query_manifest_path)
    desired_manifest_mode = "smart-grid" if args.smart_grid else "grid-scan" if args.grid_scan else ""
    mode_changed = bool(
        current_manifest_mode
        and (
            (desired_manifest_mode and current_manifest_mode != desired_manifest_mode)
            or (not desired_manifest_mode and current_manifest_mode in {"grid-scan", "smart-grid"})
        )
    )
    if args.refresh_query_manifest or mode_changed or not query_manifest_path.exists():
        write_csv(query_manifest_path, (asdict(spec) for spec in all_specs), QUERY_FIELDS)
    else:
        print(f"Query manifest already exists; skipping rewrite: {query_manifest_path}")
    if args.dry_run:
        print(f"[dry-run] Query manifest written: {query_manifest_path}")
        print(f"[dry-run] Query count: {total_specs}")
        return

    projects = read_csv_index(results_path, "approval_no") if args.resume else {}
    print(f"Output directory: {output_dir}")
    print(f"Queries: {total_specs}; completed skipped: {completed_count}; remaining: {len(specs)}; existing projects: {len(projects)}; search_mode={args.search_mode}; search_field={args.search_field}")
    if not specs:
        print(f"No remaining queries. Project table: {results_path}")
        print(f"Query manifest: {query_manifest_path}")
        print(f"Progress table: {progress_path}")
        return

    setattr(args, "proxy_manager", None)
    if args.use_proxy:
        proxy_manager = ProxyManager(
            proxy_api_url=args.proxy_api_url,
            whitelist_api_url=args.proxy_whitelist_api_url,
            proxy_file=args.proxy_file,
            expire_seconds=args.proxy_expire_seconds,
            rotate_guard_seconds=args.proxy_rotate_guard_seconds,
            cooldown_seconds=args.proxy_cooldown_seconds,
            cache_path=output_dir / "proxy_whitelist_cache.json",
            disabled_whitelist=args.proxy_no_auto_whitelist,
        )
        if not proxy_manager.has_source():
            raise SystemExit("Proxy is enabled via --use-proxy, but --proxy-api-url, NSFC_PROXY_API_URL, and --proxy-file are all missing.")
        if args.proxy_whitelist_api_url and not args.proxy_no_auto_whitelist:
            proxy_manager.ensure_whitelisted()
        setattr(args, "proxy_manager", proxy_manager)
        print("Proxy pool: enabled")
    else:
        print("Proxy pool: disabled")

    completed_runtime = 0
    detail_session = create_session_for_args(args) if args.fetch_details else None

    def consume_result(result: QueryRunResult) -> None:
        nonlocal completed_runtime, detail_session
        spec = result.spec
        spec_index = spec_positions.get(spec.query_id, 0)
        new_projects = 0
        has_project_changes = bool(result.projects)
        for project in result.projects:
            is_new, existing = merge_project(projects, project, spec)
            if is_new:
                new_projects += 1
            if (
                args.fetch_details
                and detail_session is not None
                and existing.get("detail_id")
                and not existing.get("project_abstract_cn")
            ):
                detail: Dict[str, Any] = {}
                detail_message = ""
                for attempt in range(args.proxy_retries_per_request + 1):
                    try:
                        polite_sleep(args.detail_delay, args.jitter)
                        detail = fetch_detail(detail_session, safe_text(existing.get("detail_id")), args)
                        break
                    except Exception as exc:
                        detail_message = f"{type(exc).__name__}: {exc}"
                        if (
                            attempt < args.proxy_retries_per_request
                            and getattr(args, "proxy_manager", None)
                            and is_proxy_related_message(detail_message)
                        ):
                            detail_session = refresh_proxy_session(args, detail_session, detail_message)
                            continue
                        print(f"Warning: detail fetch failed {existing.get('approval_no')}: {detail_message}", file=sys.stderr)
                        break
                if detail:
                    apply_detail(existing, detail)
                    has_project_changes = True
        if has_project_changes:
            write_csv(results_path, projects.values(), RESULT_FIELDS)
        append_progress(
            progress_path,
            {
                "query_id": spec.query_id,
                "query_keyword": spec.query_keyword,
                "year_field": spec.year_field,
                "year_value": spec.year_value,
                "status": result.status,
                "total_records": result.total_records,
                "pages_fetched": result.pages_fetched,
                "rows_seen": result.rows_seen,
                "new_projects": new_projects,
                "updated_at": now_iso(),
                "message": result.message,
            },
        )
        completed_runtime += 1
        if result.message:
            print(f"[{spec_index}/{total_specs}] {spec_summary(spec)}")
            print(f"  -> {result.status}: {result.message}")
        else:
            print(f"[{spec_index}/{total_specs}] {spec_summary(spec)}")
            print(
                f"  -> total={result.total_records}, pages={result.pages_fetched}, "
                f"rows={result.rows_seen}, new={new_projects}, unique={len(projects)}"
            )

    try:
        if args.workers <= 1:
            for spec in specs:
                consume_result(run_query_spec(spec, args))
                polite_sleep(args.delay, args.jitter)
        else:
            print(f"Concurrent search workers={args.workers}; to reduce pressure, try 4-8 first.")
            spec_iter = iter(specs)
            pending = {}

            def submit_next(executor: ThreadPoolExecutor) -> bool:
                try:
                    next_spec = next(spec_iter)
                except StopIteration:
                    return False
                pending[executor.submit(run_query_spec, next_spec, args)] = next_spec
                return True

            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                for _ in range(args.workers * 2):
                    if not submit_next(executor):
                        break
                while pending:
                    done, _ = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        spec = pending.pop(future)
                        try:
                            result = future.result()
                        except Exception as exc:
                            result = QueryRunResult(
                                spec=spec,
                                status="error",
                                message=f"{type(exc).__name__}: {exc}",
                            )
                        consume_result(result)
                        submit_next(executor)
    except KeyboardInterrupt:
        print("User interrupted; saving current results...")
        write_csv(results_path, projects.values(), RESULT_FIELDS)
        raise

    write_csv(results_path, projects.values(), RESULT_FIELDS)
    print(f"Done. Project table: {results_path}")
    print(f"Query manifest: {query_manifest_path}")
    print(f"Progress table: {progress_path}")


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="NSFC final project requests batch search script (without image downloads).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--output-dir", default="output_requests_no_images", help="Output directory")
    parser.add_argument("--keywords-file", default="", help="Custom keyword TXT/CSV; leave empty to use the built-in 338-term urban sustainability lexicon")
    parser.add_argument("--search-mode", choices=["advanced", "fuzzy"], default="advanced", help="Search mode: advanced uses advanced search fields; fuzzy uses the front-end comprehensive search box")
    parser.add_argument("--search-field", choices=["both", "projectName", "keywords", "fuzzy"], default="both", help="Advanced search fields: both runs project name and project keywords")
    parser.add_argument("--advanced-keyword-mode", choices=["topic", "full"], default="topic", help="Advanced search keyword mode: topic searches only topic terms for city+topic combinations and filters locally; full searches full combination terms")
    parser.add_argument("--advanced-grid", action="store_true", help="Match the original nsfc-paqu.py by splitting searches across funding category x second-level application code x keyword field")
    parser.add_argument("--grid-scan", action="store_true", help="Efficient mode: fetch lists by year x funding category x application code, then locally filter with the keyword lexicon to avoid a keyword Cartesian product")
    parser.add_argument("--smart-grid", action="store_true", help="Recommended efficient mode: search by deduplicated project-name/project-keyword payload terms x year x funding category x application code, then locally map full keywords")
    parser.add_argument("--anchor-mode", choices=["compact", "full"], default="compact", help="grid-scan anchor mode: compact removes long anchors covered by short anchors; full uses all city/standalone anchors")
    parser.add_argument("--skip-tianyuan-non-a", action="store_true", default=True, help="Under advanced-grid, only run category A application codes for the Mathematics Tianyuan Fund")
    parser.add_argument("--no-skip-tianyuan-non-a", dest="skip_tianyuan_non_a", action="store_false", help="Under advanced-grid, also run all application codes for the Mathematics Tianyuan Fund")
    parser.add_argument("--local-filter", action="store_true", default=True, help="In advanced topic mode, require local co-occurrence of city and topic terms")
    parser.add_argument("--no-local-filter", dest="local_filter", action="store_false", help="Disable local city+topic co-occurrence filtering")
    parser.add_argument("--year-field", choices=["none", "conclusionYear", "ratifyYear"], default="none", help="Year filter field")
    parser.add_argument("--years", nargs="*", help="Year list, e.g. 2021 2022 or 2021,2022")
    parser.add_argument("--start-year", type=int, help="Start year for range; requires --end-year")
    parser.add_argument("--end-year", type=int, help="End year for range; requires --start-year")
    parser.add_argument("--page-size", type=positive_int, default=10, help="Records per page; website front-end default is 10")
    parser.add_argument("--max-pages", type=positive_int, default=10, help="Maximum pages to fetch per query")
    parser.add_argument("--max-records-per-query", type=positive_int, default=100, help="Maximum records to process per query")
    parser.add_argument("--limit-queries", type=positive_int, help="Run only the first N queries for small-sample testing")
    parser.add_argument("--delay", type=float, default=0.8, help="Base wait seconds between search pages")
    parser.add_argument("--detail-delay", type=float, default=0.3, help="Base wait seconds before detail requests")
    parser.add_argument("--jitter", type=float, default=0.5, help="Additional random wait seconds")
    parser.add_argument("--timeout", type=int, default=30, help="Per-request timeout in seconds")
    parser.add_argument("--retries", type=int, default=5, help="HTTP retry count")
    parser.add_argument("--workers", type=positive_int, default=1, help="Concurrent search threads; 1 means single-threaded. Start with 4 or 6 for testing")
    parser.add_argument("--use-proxy", action="store_true", help="Enable proxy pool; requires --proxy-api-url, an environment variable, or --proxy-file")
    parser.add_argument("--no-proxy", dest="use_proxy", action="store_false", help="Disable proxy pool")
    parser.add_argument("--proxy-api-url", default=os.environ.get("NSFC_PROXY_API_URL", DEFAULT_PROXY_API_URL), help="Proxy extraction API URL; can also be overridden with NSFC_PROXY_API_URL")
    parser.add_argument("--proxy-whitelist-api-url", default=os.environ.get("NSFC_PROXY_WHITELIST_API_URL", DEFAULT_PROXY_WHITELIST_API_URL), help="Proxy whitelist API URL; supports {ip} or {} placeholders; can also be overridden with NSFC_PROXY_WHITELIST_API_URL")
    parser.add_argument("--proxy-file", default="", help="Local proxy list file, one host:port per line; used when no proxy API is available")
    parser.add_argument("--proxy-expire-seconds", type=positive_int, default=180, help="Proxy validity seconds")
    parser.add_argument("--proxy-rotate-guard-seconds", type=positive_int, default=10, help="Proactively refresh this many seconds before proxy expiry")
    parser.add_argument("--proxy-cooldown-seconds", type=float, default=3.0, help="Cooldown seconds after switching proxies")
    parser.add_argument("--proxy-session-attempts", type=positive_int, default=5, help="Maximum proxy attempts when creating a session")
    parser.add_argument("--proxy-retries-per-request", type=positive_int, default=3, help="Maximum proxy/limit-induced proxy switches per page request")
    parser.add_argument("--proxy-no-auto-whitelist", action="store_true", help="Disable automatic proxy whitelist addition")
    parser.add_argument("--resume", action="store_true", default=True, help="Read existing results and resume")
    parser.add_argument("--no-resume", dest="resume", action="store_false", help="Ignore existing results and rewrite")
    parser.add_argument("--skip-done", action="store_true", default=True, help="Skip query_id values already marked done in the progress table")
    parser.add_argument("--no-skip-done", dest="skip_done", action="store_false", help="Do not skip completed queries; force a rescan from the start of the query manifest")
    parser.add_argument("--fetch-details", action="store_true", default=True, help="Fetch detail abstracts and achievement lists, but not report images")
    parser.add_argument("--no-fetch-details", dest="fetch_details", action="store_false", help="Fetch only search result lists, not details")
    parser.add_argument("--flush-every-query", action="store_true", default=True, help="Save results after each query")
    parser.add_argument("--refresh-query-manifest", action="store_true", help="Force query manifest rewrite; by default, an existing manifest is skipped to save time")
    parser.add_argument("--dry-run", action="store_true", help="Generate query manifest only; do not request the website")
    args = parser.parse_args(argv)
    if (args.start_year is None) ^ (args.end_year is None):
        parser.error("--start-year and --end-year must be provided together")
    if args.start_year and args.end_year and args.start_year > args.end_year:
        parser.error("--start-year cannot be greater than --end-year")
    if args.search_mode == "fuzzy" and args.search_field != "fuzzy":
        args.search_field = "fuzzy"
    if args.search_mode == "advanced" and args.search_field == "fuzzy":
        args.search_field = "both"
    active_grid_modes = sum(1 for value in [args.advanced_grid, args.grid_scan, args.smart_grid] if value)
    if active_grid_modes > 1:
        parser.error("--advanced-grid, --grid-scan, and --smart-grid are mutually exclusive")
    if args.grid_scan or args.smart_grid:
        if args.max_pages == 10:
            args.max_pages = 200
        if args.max_records_per_query == 100:
            args.max_records_per_query = 10000
    if args.smart_grid:
        project_type_count = len(ADVANCED_PROJECT_TYPES)
        if args.skip_tianyuan_non_a and not any(code.startswith("A") for code in ADVANCED_CODES_2ND):
            project_type_count -= 1
        years_count = len(build_years(args)) if args.year_field != "none" else 1
        term_count = len(build_smart_grid_payload_terms(args))
        field_count = len(selected_keyword_fields(args.search_field, args.search_mode))
        grid_specs = project_type_count * len(ADVANCED_CODES_2ND) * years_count * term_count * field_count
        print(
            f"Note: smart-grid will run about {grid_specs} deduplicated keyword grid queries "
            f"(deduplicated search terms {term_count} x fields {field_count}); "
            f"this greatly reduces duplicate requests compared with advanced-grid.",
            file=sys.stderr,
        )
    elif args.grid_scan:
        project_type_count = len(ADVANCED_PROJECT_TYPES)
        if args.skip_tianyuan_non_a and not any(code.startswith("A") for code in ADVANCED_CODES_2ND):
            project_type_count -= 1
        years_count = len(build_years(args)) if args.year_field != "none" else 1
        anchor_count = len(build_grid_scan_anchors(args))
        field_count = len(selected_keyword_fields(args.search_field, args.search_mode))
        grid_specs = project_type_count * len(ADVANCED_CODES_2ND) * years_count * anchor_count * field_count
        print(
            f"Note: grid-scan will run about {grid_specs} anchor grid queries "
            f"(anchors {anchor_count} x fields {field_count}); "
            f"ensure --max-pages/--max-records-per-query are large enough to cover each grid.",
            file=sys.stderr,
        )
    elif args.advanced_grid:
        project_type_count = len(ADVANCED_PROJECT_TYPES)
        if args.skip_tianyuan_non_a and not any(code.startswith("A") for code in ADVANCED_CODES_2ND):
            project_type_count -= 1
        grid_size = project_type_count * len(ADVANCED_CODES_2ND) * len(selected_keyword_fields(args.search_field, args.search_mode))
        print(f"Note: advanced-grid will expand each base query into about {grid_size} requests; confirm the runtime.", file=sys.stderr)
    return args


if __name__ == "__main__":
    crawl(parse_args())
