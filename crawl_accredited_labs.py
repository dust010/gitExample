"""
crawl_accredited_labs.py
RoHS 시험 공인기관 자동 크롤러 + Google Sheets 업데이트

지원 인정기구:
  - KOLAS  (한국)
  - CNAS   (중국)
  - UKAS   (영국)
  - A2LA   (미국)
  - DAkkS  (독일)
  - ILAC   (국제 — 글로벌 대형 기관 보완용)

사용법:
  pip install requests beautifulsoup4 gspread google-auth
  python crawl_accredited_labs.py

Google Sheets 설정:
  1. Google Cloud Console → 서비스 계정 생성 → JSON 키 다운로드
  2. 키 파일을 이 스크립트와 같은 폴더에 'service_account.json' 으로 저장
  3. SHEET_ID 를 본인 시트 ID로 변경
  4. 시트를 서비스 계정 이메일에 편집 권한 공유

GitHub Actions 자동화:
  .github/workflows/crawl.yml 참고
"""

import requests
import time
import json
import re
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup

# ── Google Sheets 연동 ────────────────────────────────────
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False
    print("[WARN] gspread 미설치 — Sheets 업로드 생략, CSV만 저장")

# ══════════════════════════════════════════════════════════
#  설정
# ══════════════════════════════════════════════════════════
SHEET_ID        = "1goQC7vO7SNeO5zUQwW718If_iml4kkKfZQ19ROWSX-M"          # ← 본인 시트 ID로 변경
SHEET_NAME      = "AccreditedLabs"
SERVICE_ACCOUNT = "service_account.json"
OUTPUT_CSV      = "accredited_labs.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════
#  공통 유틸
# ══════════════════════════════════════════════════════════
def safe_get(url: str, params: dict = None, timeout: int = 30) -> requests.Response | None:
    """요청 실패 시 None 반환 (크롤러 전체가 멈추지 않도록)"""
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception as e:
        log.warning(f"  GET 실패: {url} → {e}")
        return None

def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

# ══════════════════════════════════════════════════════════
#  각 인정기구 크롤러
# ══════════════════════════════════════════════════════════

def crawl_kolas() -> list[dict]:
    """
    KOLAS (한국인정기구) — https://www.kolas.go.kr
    시험기관 검색: /menu.es?mid=a10203010000
    RoHS 관련: 전기전자 분야 시험기관 필터
    """
    log.info("[KOLAS] 크롤링 시작...")
    results = []
    base = "https://www.kolas.go.kr"

    # KOLAS 공개 검색 페이지 (전기전자 시험분야)
    url = f"{base}/menu.es"
    params = {
        "mid": "a10203010000",
        "testField": "전기전자",
        "pageIndex": 1
    }

    page = 1
    while True:
        params["pageIndex"] = page
        r = safe_get(url, params=params)
        if not r:
            break

        soup = BeautifulSoup(r.text, "html.parser")
        rows = soup.select("table tbody tr")
        if not rows:
            break

        found = 0
        for row in rows:
            cols = row.select("td")
            if len(cols) < 4:
                continue
            name        = cols[0].get_text(strip=True)
            acc_no      = cols[1].get_text(strip=True)
            field       = cols[2].get_text(strip=True)
            location    = cols[3].get_text(strip=True)

            if not name:
                continue

            results.append({
                "name":         name,
                "name_en":      "",
                "country":      "KR",
                "accreditor":   "KOLAS",
                "acc_number":   acc_no,
                "field":        field,
                "location":     location,
                "website":      "",
                "updated":      now_utc(),
                "source_url":   url,
            })
            found += 1

        log.info(f"  [KOLAS] 페이지 {page}: {found}건")
        if found < 10:  # 마지막 페이지
            break
        page += 1
        time.sleep(1)

    log.info(f"  [KOLAS] 총 {len(results)}건 수집")
    return results


def crawl_cnas() -> list[dict]:
    """
    CNAS (중국합격평정국가인정위원회) — https://www.cnas.org.cn
    공개 검색 API 활용
    """
    log.info("[CNAS] 크롤링 시작...")
    results = []

    # CNAS 공개 기관 검索 API
    api_url = "https://www.cnas.org.cn/jgcx/cjgcx.do"

    page = 1
    page_size = 50

    while True:
        payload = {
            "page":     page,
            "pageSize": page_size,
            "orgType":  "L",    # L = 시험기관(Laboratory)
            "field":    "电子电气",  # 전기전자 분야
        }
        try:
            r = requests.post(
                api_url,
                data=payload,
                headers={**HEADERS, "Content-Type": "application/x-www-form-urlencoded"},
                timeout=30
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.warning(f"  [CNAS] API 오류 page={page}: {e}")
            break

        items = data.get("rows") or data.get("data") or []
        if not items:
            break

        for item in items:
            name    = item.get("orgName") or item.get("name", "")
            name_en = item.get("orgNameEn") or item.get("nameEn", "")
            acc_no  = item.get("certNo") or item.get("accNo", "")
            loc     = item.get("province") or item.get("area", "")

            if not name:
                continue

            results.append({
                "name":         name,
                "name_en":      name_en,
                "country":      "CN",
                "accreditor":   "CNAS",
                "acc_number":   acc_no,
                "field":        "电子电气",
                "location":     loc,
                "website":      "",
                "updated":      now_utc(),
                "source_url":   api_url,
            })

        log.info(f"  [CNAS] 페이지 {page}: {len(items)}건")
        if len(items) < page_size:
            break
        page += 1
        time.sleep(1.5)

    log.info(f"  [CNAS] 총 {len(results)}건 수집")
    return results


def crawl_ukas() -> list[dict]:
    """
    UKAS (영국인정기구) — https://www.ukas.com
    공개 디렉토리 검색
    """
    log.info("[UKAS] 크롤링 시작...")
    results = []

    # UKAS 공개 검색 (Testing Laboratories)
    base = "https://www.ukas.com"
    url  = f"{base}/find-an-accredited-organisation/search-results/"
    params = {
        "type":    "testing",
        "keyword": "",
        "page":    1,
    }

    page = 1
    while True:
        params["page"] = page
        r = safe_get(url, params=params)
        if not r:
            break

        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".organisation-card, .search-result-item, article.lab")
        if not cards:
            # 대안: 테이블 구조
            cards = soup.select("table tbody tr")

        if not cards:
            log.info(f"  [UKAS] 페이지 {page}: 항목 없음 — 종료")
            break

        found = 0
        for card in cards:
            name_el = card.select_one("h3, h2, .name, td:first-child")
            acc_el  = card.select_one(".acc-number, .reference, td:nth-child(2)")
            name    = name_el.get_text(strip=True) if name_el else ""
            acc_no  = acc_el.get_text(strip=True)  if acc_el  else ""

            if not name:
                continue

            results.append({
                "name":         name,
                "name_en":      name,
                "country":      "GB",
                "accreditor":   "UKAS",
                "acc_number":   acc_no,
                "field":        "Testing",
                "location":     "UK",
                "website":      "",
                "updated":      now_utc(),
                "source_url":   url,
            })
            found += 1

        log.info(f"  [UKAS] 페이지 {page}: {found}건")
        # 다음 페이지 링크 확인
        next_btn = soup.select_one("a[rel='next'], .pagination .next a")
        if not next_btn:
            break
        page += 1
        time.sleep(1)

    log.info(f"  [UKAS] 총 {len(results)}건 수집")
    return results


def crawl_a2la() -> list[dict]:
    """
    A2LA (미국인정기구) — https://www.a2la.org
    공개 디렉토리 API
    """
    log.info("[A2LA] 크롤링 시작...")
    results = []

    # A2LA 공개 검색 API
    api_url = "https://www.a2la.org/accreditation/searchresults.cfm"
    params = {
        "ProgramID": "1",      # 1 = Testing
        "FieldID":   "24",     # Electrical/Electronic
        "txtPage":   1,
    }

    page = 1
    while True:
        params["txtPage"] = page
        r = safe_get(api_url, params=params)
        if not r:
            break

        soup = BeautifulSoup(r.text, "html.parser")
        rows = soup.select("table.results tbody tr, .search-result")
        if not rows:
            break

        found = 0
        for row in rows:
            cols = row.select("td")
            if len(cols) < 2:
                continue
            name    = cols[0].get_text(strip=True)
            acc_no  = cols[1].get_text(strip=True) if len(cols) > 1 else ""
            loc     = cols[2].get_text(strip=True) if len(cols) > 2 else ""

            if not name:
                continue

            results.append({
                "name":         name,
                "name_en":      name,
                "country":      "US",
                "accreditor":   "A2LA",
                "acc_number":   acc_no,
                "field":        "Electrical/Electronic Testing",
                "location":     loc,
                "website":      "",
                "updated":      now_utc(),
                "source_url":   api_url,
            })
            found += 1

        log.info(f"  [A2LA] 페이지 {page}: {found}건")
        next_btn = soup.select_one("a.next, .pagination a[href*='next']")
        if not next_btn or found == 0:
            break
        page += 1
        time.sleep(1)

    log.info(f"  [A2LA] 총 {len(results)}건 수집")
    return results


def crawl_dakks() -> list[dict]:
    """
    DAkkS (독일인정기구) — https://www.dakks.de
    공개 데이터베이스
    """
    log.info("[DAkkS] 크롤링 시작...")
    results = []

    # DAkkS 공개 검색
    url = "https://www.dakks.de/en/accredited-bodies/search"
    params = {
        "field": "testing",
        "page":  0,
    }

    page = 0
    while True:
        params["page"] = page
        r = safe_get(url, params=params)
        if not r:
            break

        soup = BeautifulSoup(r.text, "html.parser")
        rows = soup.select(".views-row, .search-result-row, table tbody tr")
        if not rows:
            break

        found = 0
        for row in rows:
            name_el = row.select_one("h3, h2, .views-field-title, td:first-child")
            acc_el  = row.select_one(".views-field-field-akkreditierungsnummer, td:nth-child(2)")
            name    = name_el.get_text(strip=True) if name_el else ""
            acc_no  = acc_el.get_text(strip=True)  if acc_el  else ""

            if not name:
                continue

            results.append({
                "name":         name,
                "name_en":      name,
                "country":      "DE",
                "accreditor":   "DAkkS",
                "acc_number":   acc_no,
                "field":        "Testing",
                "location":     "Germany",
                "website":      "",
                "updated":      now_utc(),
                "source_url":   url,
            })
            found += 1

        log.info(f"  [DAkkS] 페이지 {page}: {found}건")
        next_btn = soup.select_one("a[rel='next'], li.pager-next a")
        if not next_btn or found == 0:
            break
        page += 1
        time.sleep(1)

    log.info(f"  [DAkkS] 총 {len(results)}건 수집")
    return results


def get_major_labs_baseline() -> list[dict]:
    """
    글로벌 대형 공인기관 하드코딩 베이스라인
    크롤링 실패 시에도 최소한 이 기관들은 인식 가능하도록 보장
    각 기관은 ILAC MLA 가입 인정기구의 인정을 받은 기관
    """
    return [
        # ── 글로벌 대형 기관 ─────────────────────────────────
        {"name": "Intertek Testing Services",        "name_en": "Intertek",              "country": "GLOBAL", "accreditor": "ILAC-MLA", "acc_number": "multi",  "field": "RoHS/ELV/REACH", "location": "Global", "website": "intertek.com",       "updated": now_utc(), "source_url": "baseline"},
        {"name": "Intertek Testing Services Korea",  "name_en": "Intertek Korea",        "country": "KR",     "accreditor": "KOLAS",    "acc_number": "",       "field": "RoHS/ELV/REACH", "location": "Seoul",  "website": "intertek.co.kr",     "updated": now_utc(), "source_url": "baseline"},
        {"name": "SGS",                              "name_en": "SGS",                   "country": "GLOBAL", "accreditor": "ILAC-MLA", "acc_number": "multi",  "field": "RoHS/ELV/REACH", "location": "Global", "website": "sgs.com",            "updated": now_utc(), "source_url": "baseline"},
        {"name": "SGS Korea",                        "name_en": "SGS Korea",             "country": "KR",     "accreditor": "KOLAS",    "acc_number": "",       "field": "RoHS/ELV/REACH", "location": "Korea",  "website": "sgs.co.kr",          "updated": now_utc(), "source_url": "baseline"},
        {"name": "Bureau Veritas",                   "name_en": "Bureau Veritas",        "country": "GLOBAL", "accreditor": "ILAC-MLA", "acc_number": "multi",  "field": "RoHS/ELV/REACH", "location": "Global", "website": "bureauveritas.com",  "updated": now_utc(), "source_url": "baseline"},
        {"name": "TÜV Rheinland",                   "name_en": "TUV Rheinland",         "country": "DE",     "accreditor": "DAkkS",    "acc_number": "",       "field": "RoHS/ELV/REACH", "location": "Germany","website": "tuv.com",            "updated": now_utc(), "source_url": "baseline"},
        {"name": "TÜV SÜD",                         "name_en": "TUV SUD",               "country": "DE",     "accreditor": "DAkkS",    "acc_number": "",       "field": "RoHS/ELV/REACH", "location": "Germany","website": "tuvsud.com",         "updated": now_utc(), "source_url": "baseline"},
        {"name": "TÜV Nord",                         "name_en": "TUV Nord",              "country": "DE",     "accreditor": "DAkkS",    "acc_number": "",       "field": "RoHS/ELV/REACH", "location": "Germany","website": "tuvnord.com",        "updated": now_utc(), "source_url": "baseline"},
        {"name": "UL",                               "name_en": "UL Solutions",          "country": "US",     "accreditor": "A2LA",     "acc_number": "",       "field": "RoHS/ELV/REACH", "location": "USA",    "website": "ul.com",             "updated": now_utc(), "source_url": "baseline"},
        {"name": "Eurofins",                         "name_en": "Eurofins",              "country": "GLOBAL", "accreditor": "ILAC-MLA", "acc_number": "multi",  "field": "RoHS/ELV/REACH", "location": "Global", "website": "eurofins.com",       "updated": now_utc(), "source_url": "baseline"},
        {"name": "DEKRA",                            "name_en": "DEKRA",                 "country": "DE",     "accreditor": "DAkkS",    "acc_number": "",       "field": "RoHS/ELV/REACH", "location": "Germany","website": "dekra.com",          "updated": now_utc(), "source_url": "baseline"},
        {"name": "CTI",                              "name_en": "CTI",                   "country": "TW",     "accreditor": "TAF",      "acc_number": "",       "field": "RoHS/ELV/REACH", "location": "Taiwan", "website": "cti.com.tw",         "updated": now_utc(), "source_url": "baseline"},
        {"name": "CESI",                             "name_en": "CESI",                  "country": "CN",     "accreditor": "CNAS",     "acc_number": "",       "field": "RoHS/ELV/REACH", "location": "China",  "website": "cesi.cn",            "updated": now_utc(), "source_url": "baseline"},
        {"name": "CTC",                              "name_en": "CTC",                   "country": "CN",     "accreditor": "CNAS",     "acc_number": "",       "field": "RoHS/ELV/REACH", "location": "China",  "website": "",                   "updated": now_utc(), "source_url": "baseline"},
        {"name": "TICW",                             "name_en": "TICW",                  "country": "CN",     "accreditor": "CNAS",     "acc_number": "",       "field": "RoHS/ELV/REACH", "location": "China",  "website": "",                   "updated": now_utc(), "source_url": "baseline"},
        {"name": "KTL",                              "name_en": "Korea Testing Laboratory","country":"KR",    "accreditor": "KOLAS",    "acc_number": "",       "field": "RoHS/ELV/REACH", "location": "Korea",  "website": "ktl.re.kr",          "updated": now_utc(), "source_url": "baseline"},
        {"name": "KIMM",                             "name_en": "KIMM",                  "country": "KR",     "accreditor": "KOLAS",    "acc_number": "",       "field": "RoHS/ELV/REACH", "location": "Korea",  "website": "kimm.re.kr",         "updated": now_utc(), "source_url": "baseline"},
        {"name": "KRISS",                            "name_en": "KRISS",                 "country": "KR",     "accreditor": "KOLAS",    "acc_number": "",       "field": "RoHS/ELV/REACH", "location": "Korea",  "website": "kriss.re.kr",        "updated": now_utc(), "source_url": "baseline"},
    ]


# ══════════════════════════════════════════════════════════
#  중복 제거
# ══════════════════════════════════════════════════════════
def deduplicate(labs: list[dict]) -> list[dict]:
    seen = set()
    unique = []
    for lab in labs:
        key = (lab["name"].strip().lower(), lab["accreditor"])
        if key not in seen:
            seen.add(key)
            unique.append(lab)
    return unique


# ══════════════════════════════════════════════════════════
#  CSV 저장
# ══════════════════════════════════════════════════════════
def save_csv(labs: list[dict], path: str):
    import csv
    cols = ["name","name_en","country","accreditor","acc_number","field","location","website","updated","source_url"]
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(labs)
    log.info(f"CSV 저장: {path} ({len(labs)}건)")


# ══════════════════════════════════════════════════════════
#  Google Sheets 업데이트
# ══════════════════════════════════════════════════════════
def update_google_sheets(labs: list[dict]):
    if not GSPREAD_AVAILABLE:
        log.warning("gspread 없음 — Sheets 업로드 생략")
        return

    import os
    if not os.path.exists(SERVICE_ACCOUNT):
        log.warning(f"서비스 계정 키 없음: {SERVICE_ACCOUNT} — Sheets 업로드 생략")
        return

    try:
        log.info("Google Sheets 업로드 중...")
        scopes = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds  = Credentials.from_service_account_file(SERVICE_ACCOUNT, scopes=scopes)
        client = gspread.authorize(creds)
        sheet  = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)

        cols = ["name","name_en","country","accreditor","acc_number",
                "field","location","website","updated","source_url"]

        # 헤더 + 데이터 한 번에 업데이트
        rows = [cols] + [[lab.get(c, "") for c in cols] for lab in labs]
        sheet.clear()
        sheet.update(rows, value_input_option="RAW")

        # 헤더 행 볼드 처리
        sheet.format("A1:J1", {"textFormat": {"bold": True}})

        log.info(f"Google Sheets 업로드 완료: {len(labs)}건 → 시트 '{SHEET_NAME}'")

    except Exception as e:
        log.error(f"Google Sheets 업로드 실패: {e}")


# ══════════════════════════════════════════════════════════
#  메인
# ══════════════════════════════════════════════════════════
def main():
    log.info("=" * 55)
    log.info("  RoHS 공인기관 크롤러 시작")
    log.info("=" * 55)

    all_labs = []

    # 1. 베이스라인 (항상 포함)
    baseline = get_major_labs_baseline()
    all_labs.extend(baseline)
    log.info(f"베이스라인: {len(baseline)}건 로드")

    # 2. 각 인정기구 크롤링
    crawlers = [
        ("KOLAS",  crawl_kolas),
        ("CNAS",   crawl_cnas),
        ("UKAS",   crawl_ukas),
        ("A2LA",   crawl_a2la),
        ("DAkkS",  crawl_dakks),
    ]

    for name, fn in crawlers:
        try:
            labs = fn()
            all_labs.extend(labs)
        except Exception as e:
            log.error(f"[{name}] 크롤링 실패: {e}")
        time.sleep(2)

    # 3. 중복 제거
    all_labs = deduplicate(all_labs)
    log.info(f"\n총 수집: {len(all_labs)}건 (중복 제거 후)")

    # 4. CSV 저장
    save_csv(all_labs, OUTPUT_CSV)

    # 5. Google Sheets 업로드
    update_google_sheets(all_labs)

    log.info("\n완료!")


if __name__ == "__main__":
    main()
