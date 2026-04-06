#!/usr/bin/env python3
"""
update_latest.py — 새 날짜 데이터를 latest.json에 추가하고 60일 초과분을 archive.json에 보관

사용법:
  python3 update_latest.py --date 20260401
  python3 update_latest.py --date 20260401 --push   # git commit & push까지
  python3 update_latest.py --date 20260401 --dry-run # 변경 없이 결과만 출력
"""

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone

# ── 경로 설정 ───────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, '..', 'stock-trader', 'data')
LATEST_PATH = os.path.join(SCRIPT_DIR, 'consensus', 'latest.json')
ARCHIVE_PATH = os.path.join(SCRIPT_DIR, 'consensus', 'archive.json')
INFLUENCERS_PATH = os.path.join(DATA_DIR, 'meta', 'influencers.json')

ARCHIVE_DAYS = 60  # latest.json에서 유지할 최대 일수


# ── 인플루언서 이름 맵 ──────────────────────────────────────────────────────
def load_influencer_map() -> dict:
    with open(INFLUENCERS_PATH, encoding='utf-8') as f:
        data = json.load(f)
    return {inf['id']: inf['name'] for inf in data['influencers']}


# ── 기존 데이터에서 섹터 맵 빌드 ────────────────────────────────────────────
def build_sector_map(latest: dict, archive: dict) -> dict:
    sector_map = {}
    for source in [latest, archive]:
        for section in source.get('date_sections', []):
            for stock in section.get('stocks', []):
                code = stock.get('stock_code')
                sector = stock.get('sector')
                if code and sector and code not in sector_map:
                    sector_map[code] = sector
    return sector_map


# ── combined_reasoning 파싱 ─────────────────────────────────────────────────
def parse_reasonings(combined: str) -> dict:
    result = {}
    parts = re.split(r' \| (?=\[)', combined.strip())
    for part in parts:
        m = re.match(r'\[(\w+)\] (.+)', part.strip(), re.DOTALL)
        if m:
            inf_id, text = m.group(1), m.group(2).strip()
            result[inf_id] = result.get(inf_id, '') + (' ' if inf_id in result else '') + text
    return result


# ── consensus_stocks 형식 → date_section 변환 ────────────────────────────────
def convert_consensus_to_section(consensus: dict, influencer_map: dict, sector_map: dict) -> dict:
    target_date = consensus['analysis_date']  # "2026-03-31"
    d = datetime.strptime(target_date, '%Y-%m-%d')
    weekdays_ko = ['월', '화', '수', '목', '금', '토', '일']
    weekday = weekdays_ko[d.weekday()]
    label = f"{d.month}월 {d.day}일 ({weekday})"

    stocks = []
    for cs in consensus.get('consensus_stocks', []):
        reasonings = parse_reasonings(cs.get('combined_reasoning', ''))
        rec_type = 'strong_buy' if cs.get('consensus_ratio', 0) >= 0.4 else 'buy'

        tp_range = cs.get('target_price_range') or []
        target_price = None
        if len(tp_range) == 2 and tp_range[0] and tp_range[1]:
            target_price = round((tp_range[0] + tp_range[1]) / 2, 2)

        for inf_id in cs.get('recommenders', []):
            stocks.append({
                'stock_name': cs['stock_name'],
                'stock_code': cs['stock_code'],
                'market': cs['market'],
                'sector': sector_map.get(cs['stock_code']),
                'influencer_name': influencer_map.get(inf_id, inf_id),
                'influencer_id': inf_id,
                'recommendation_type': rec_type,
                'reasoning': reasonings.get(inf_id, ''),
                'target_price': target_price,
                'content_url': None,
                'price_info': cs.get('price_info'),
            })

    return {'date': target_date, 'label': label, 'stocks': stocks}


# ── date_sections 형식 그대로 사용 ──────────────────────────────────────────
def extract_section_from_date_sections(data: dict, target_date: str) -> dict | None:
    for section in data.get('date_sections', []):
        if section['date'] == target_date:
            return section
    return None


# ── latest.json 로드 (없으면 빈 구조) ──────────────────────────────────────
def load_json(path: str, default: dict) -> dict:
    if os.path.exists(path):
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    return default


# ── 메인 ────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description='latest.json 업데이트')
    parser.add_argument('--date', required=True, help='날짜 (YYYYMMDD 또는 YYYY-MM-DD)')
    parser.add_argument('--push', action='store_true', help='완료 후 git commit & push')
    parser.add_argument('--dry-run', action='store_true', help='변경하지 않고 결과만 출력')
    args = parser.parse_args()

    # 날짜 정규화
    raw_date = args.date.replace('-', '')
    if len(raw_date) != 8 or not raw_date.isdigit():
        print(f'[ERROR] 날짜 형식 오류: {args.date} (YYYYMMDD 또는 YYYY-MM-DD 사용)')
        sys.exit(1)
    target_date = f'{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}'
    print(f'[INFO] 대상 날짜: {target_date}')

    # 원격 최신 데이터를 먼저 pull (로컬이 뒤처져 있으면 기존 데이터를 덮어쓸 수 있으므로)
    print('[INFO] git pull origin main ...')
    os.chdir(SCRIPT_DIR)
    result = subprocess.run(['git', 'pull', 'origin', 'main'], capture_output=True, text=True)
    if result.returncode != 0:
        print(f'[WARN] git pull 실패 (오프라인?): {result.stderr.strip()}')
    else:
        pull_msg = result.stdout.strip().split('\n')[-1]
        print(f'[INFO] git pull 완료: {pull_msg}')

    # 메타 데이터 로드
    influencer_map = load_influencer_map()
    latest = load_json(LATEST_PATH, {'date_sections': []})
    archive = load_json(ARCHIVE_PATH, {'date_sections': []})
    sector_map = build_sector_map(latest, archive)

    # 이미 있으면 스킵
    existing_dates = {s['date'] for s in latest.get('date_sections', [])}
    if target_date in existing_dates:
        print(f'[SKIP] {target_date} 는 이미 latest.json에 있습니다.')
        sys.exit(0)

    # 소스 1: 파이프라인이 생성한 latest.json (date_sections 기반, 해당 날짜 영상만 포함)
    # 소스 2: consensus_YYYYMMDD.json (fallback — consensus_stocks는 60일 윈도우 전체이므로 비권장)
    pipeline_latest_path = os.path.join(DATA_DIR, 'consensus', 'latest.json')
    new_section = None

    # 우선: 파이프라인 latest.json에서 해당 날짜 섹션 추출 (정확한 데이터 + content_url 포함)
    if os.path.exists(pipeline_latest_path):
        with open(pipeline_latest_path, encoding='utf-8') as f:
            pipeline_data = json.load(f)
        new_section = extract_section_from_date_sections(pipeline_data, target_date)
        if new_section:
            print(f'[INFO] 파이프라인 latest.json에서 {target_date} 섹션 추출 (정확한 날짜별 데이터)')

    # fallback: consensus_YYYYMMDD.json
    if not new_section:
        consensus_path = os.path.join(DATA_DIR, 'consensus', f'consensus_{raw_date}.json')
        if not os.path.exists(consensus_path):
            print(f'[ERROR] 소스 파일 없음: {consensus_path}')
            sys.exit(1)

        with open(consensus_path, encoding='utf-8') as f:
            source = json.load(f)

        if 'date_sections' in source:
            print('[INFO] date_sections 형식 감지 → 해당 날짜 섹션 추출')
            new_section = extract_section_from_date_sections(source, target_date)
        elif 'consensus_stocks' in source:
            print('[WARN] consensus_stocks 형식 — 60일 윈도우 전체 데이터입니다. 파이프라인 재실행을 권장합니다.')
            new_section = convert_consensus_to_section(source, influencer_map, sector_map)

        if not new_section:
            print(f'[ERROR] {target_date} 섹션을 찾을 수 없습니다.')
            sys.exit(1)

    print(f'[INFO] 새 섹션: {len(new_section["stocks"])}개 엔트리 ({len({s["stock_code"] for s in new_section["stocks"]})}개 종목)')

    # latest.json에 삽입 (날짜 내림차순 유지)
    sections = latest.get('date_sections', [])
    sections.append(new_section)
    sections.sort(key=lambda s: s['date'], reverse=True)

    # 60일 기준 분리
    cutoff = (datetime.now(timezone.utc) - timedelta(days=ARCHIVE_DAYS)).strftime('%Y-%m-%d')
    keep = [s for s in sections if s['date'] >= cutoff]
    expired = [s for s in sections if s['date'] < cutoff]

    if expired:
        print(f'[INFO] {len(expired)}개 섹션 → archive.json 이동: {[s["date"] for s in expired]}')
        archive_sections = archive.get('date_sections', [])
        existing_archive_dates = {s['date'] for s in archive_sections}
        for s in expired:
            if s['date'] not in existing_archive_dates:
                archive_sections.append(s)
        archive_sections.sort(key=lambda s: s['date'], reverse=True)
        archive['date_sections'] = archive_sections

    latest['date_sections'] = keep
    latest['last_updated'] = target_date
    latest['total_influencers_analyzed'] = len(influencer_map)

    print(f'[INFO] latest.json 최종: {[s["date"] for s in keep]}')

    if args.dry_run:
        print('[DRY-RUN] 변경 없이 종료')
        return

    # 파일 저장
    with open(LATEST_PATH, 'w', encoding='utf-8') as f:
        json.dump(latest, f, ensure_ascii=False, indent=2)
    print(f'[SAVED] {LATEST_PATH}')

    if expired:
        with open(ARCHIVE_PATH, 'w', encoding='utf-8') as f:
            json.dump(archive, f, ensure_ascii=False, indent=2)
        print(f'[SAVED] {ARCHIVE_PATH}')

    # git commit & push
    if args.push:
        os.chdir(SCRIPT_DIR)
        files = ['consensus/latest.json']
        if expired:
            files.append('consensus/archive.json')
        subprocess.run(['git', 'add'] + files, check=True)
        msg = f'data: update {target_date} ({len(keep)}개 섹션, {len(new_section["stocks"])}개 엔트리)'
        subprocess.run(['git', 'commit', '-m', msg], check=True)
        subprocess.run(['git', 'push', 'origin', 'main'], check=True)
        print('[PUSHED] git push 완료')


if __name__ == '__main__':
    main()
