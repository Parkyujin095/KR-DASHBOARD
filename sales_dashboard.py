# -*- coding: utf-8 -*-
"""
Sales Dashboard
- 엑셀 sales_dashboard.xlsm에서 cc, kr_sales_leads, kr_sales_performance, kr_sales_call 시트 로드
- 기간: 일별(오늘), 주별(이번주), 월별(이번달), L3M, L6M
- 지표 계산 후 HTML 단일 파일로 대시보드 출력
"""
import os
import sys
import json
from datetime import datetime, timedelta, time
from calendar import monthrange

import warnings
import pandas as pd
import numpy as np

warnings.filterwarnings("ignore", message="Data Validation extension", category=UserWarning, module="openpyxl")

# =========================
# CONFIG
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXCEL_PATH = os.path.join(BASE_DIR, "sales_dashboard.xlsm")
OUTPUT_HTML = os.path.join(BASE_DIR, "sales_dashboard.html")

# 목표: 현재 대비 20% 향상 (추후 설정 예정)
GOAL_BUMP = 1.20

# Trials=시착중(단계 진행 중+시착일). Orders=Visited+시착일. OR=Orders/Visits.
# Closed_Orders=종료일 기간 내, Visited+시착일 일자+종료(유실/성공) 동일 행. SR=Sales/Closed_Orders.
# Call Scoring: 추후 데이터 제공 시 반영. 현재 미구현, UI 공간만 확보.
# 팀: cc 시트 team = 팀장이자 팀명. 표시명 = "팀 " + 이름(team[1:], 예: 김수언→팀 수언). cc = 담당자(팀원).
# PIP: 두진경 팀 제외 · cc hire_date 6개월+ · 선정 풀 내 **참조 기간** 성공 매출 합 기준 하위 비율.
# 분기별로 참조 월·모니터링 기간만 바꾸면 됨 (아래 연·월 튜플).
# PIP·접근위험 선정: 해당 3개월(예: 10~12월) 성공 매출 합 기준 정렬.
PIP_PIP_FRACTION = 0.10  # 하위 10% = PIP 대상
PIP_RISK_FRACTION = 0.25  # 하위 25% 라인까지 = 접근 위험 상한 (PIP 10% 제외 구간)


def pip_quarters_from_ref_dt(ref_dt):
    """ref_dt 기준 현재 분기(모니터링)와 이전 분기(선정 기준) 반환.
    분기: Q1=1~3월, Q2=4~6월, Q3=7~9월, Q4=10~12월.
    PIP 시작일 = 현재 분기 1일 (1/1, 4/1, 7/1, 10/1).
    예) ref_dt=2026-05 → 선정=Q1(1~3월), 모니터링=Q2(4~6월), 시작=2026-04-01."""
    y, m = ref_dt.year, ref_dt.month
    cur_q_start_m = ((m - 1) // 3) * 3 + 1  # 1, 4, 7, 10 중 하나
    cur_q = [(y, cur_q_start_m + i) for i in range(3)]
    prev_q_start_m = cur_q_start_m - 3
    prev_q_y = y
    if prev_q_start_m < 1:
        prev_q_start_m += 12
        prev_q_y -= 1
    prev_q = [(prev_q_y, prev_q_start_m + i) for i in range(3)]
    pip_start_date = datetime(y, cur_q_start_m, 1)
    return cur_q, prev_q, pip_start_date


def _pip_reference_triple_and_labels(ref_dt):
    """PIP 선정 기준: 이전 분기 3개월 (과거→최근 순) 및 표 헤더 라벨."""
    _, prev_q, _ = pip_quarters_from_ref_dt(ref_dt)
    labels = [f"{mm}월" for _, mm in prev_q]
    return prev_q, labels


def _pip_reference_range_label(triple):
    if not triple:
        return ""
    y0, m0 = triple[0]
    y1, m1 = triple[-1]
    return f"{y0}.{m0:02d} ~ {y1}.{m1:02d}"


def _col(df, *candidates):
    """후보 컬럼명 중 존재하는 첫 번째 반환."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _col_amount(df):
    """매출 계산용 총액(액수) 열 반환. '총액 통화'는 제외."""
    if "총액" in df.columns:
        return "총액"
    for c in df.columns:
        s = _safe_str(c)
        if not s or "통화" in s:
            continue
        if s == "총액" or ("총액" in s and "통화" not in s):
            return c
    return None


def _safe_str(x):
    if x is None or (isinstance(x, float) and (pd.isna(x) or (hasattr(np, 'nan') and x != x))):
        return ""
    s = str(x).strip()
    if not s or s.lower() in ("nan", "none", "#n/a", "null"):
        return ""
    return s


def _safe_float(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return 0.0
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def _parse_date(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, datetime):
        return x
    try:
        return pd.to_datetime(x, errors="coerce")
    except Exception:
        return None


def _to_date_series(ser):
    return pd.to_datetime(ser, errors="coerce")


# ---------- 기간 ----------
def period_range(ref_dt, period_key):
    """ref_dt 기준 기간 (start, end). end는 포함."""
    y, m, d = ref_dt.year, ref_dt.month, ref_dt.day
    if period_key == "daily":
        start = datetime(y, m, d)
        end = start.replace(hour=23, minute=59, second=59, microsecond=999999)
        return start, end
    if period_key == "weekly":
        # ISO week: 월요일 시작
        weekday = ref_dt.isoweekday()  # 1=Mon, 7=Sun
        mon = ref_dt - timedelta(days=weekday - 1)
        start = mon.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=6, hours=23, minutes=59, seconds=59)
        return start, end
    if period_key == "monthly":
        _, last = monthrange(y, m)
        start = datetime(y, m, 1)
        end = datetime(y, m, last, 23, 59, 59)
        return start, end
    if period_key == "L3M":
        # 이번달 포함 최근 3개월
        start = datetime(y, m, 1) - timedelta(days=60)  # 대략 2달 전 1일
        start = start.replace(day=1)
        _, last = monthrange(y, m)
        end = datetime(y, m, last, 23, 59, 59)
        return start, end
    if period_key == "L6M":
        start = datetime(y, m, 1) - timedelta(days=180)
        start = start.replace(day=1)
        _, last = monthrange(y, m)
        end = datetime(y, m, last, 23, 59, 59)
        return start, end
    return None, None


def in_period(dt_ser, start, end):
    if dt_ser is None or start is None or end is None:
        return pd.Series(False, index=dt_ser.index if dt_ser is not None else range(0))
    t = pd.to_datetime(dt_ser, errors="coerce")
    return (t >= pd.Timestamp(start)) & (t <= pd.Timestamp(end))


# ---------- 엑셀 로드 ----------
def load_excel(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"엑셀 파일을 찾을 수 없습니다: {path}")
    cc = pd.read_excel(path, sheet_name="cc", header=0)
    leads = pd.read_excel(path, sheet_name="kr_sales_leads", header=0)
    perf = pd.read_excel(path, sheet_name="kr_sales_performance", header=0)
    call = pd.read_excel(path, sheet_name="kr_sales_call", header=0)
    try:
        supply = pd.read_excel(path, sheet_name="supply_price", header=0)
    except Exception:
        supply = pd.DataFrame(columns=["product", "supply_price"])
    return cc, leads, perf, call, supply


# ---------- 리드 지표 (kr_sales_leads, 리드작성 시각 기준) ----------
def compute_lead_metrics(leads_df, start, end, owner_name=None):
    date_col = _col(leads_df, "리드작성 시각")
    owner_col = _col(leads_df, "리드 소유자")
    if date_col is None:
        return {"lead": 0, "recall_excluded_leads": 0, "recall_count": 0, "qualified_count": 0}
    dt = _to_date_series(leads_df[date_col])
    mask = in_period(dt, start, end)
    df = leads_df.loc[mask].copy()
    if owner_name and owner_col is not None:
        df = df[df[owner_col].astype(str).str.strip() == owner_name]
    if df.empty:
        return {"lead": 0, "recall_excluded_leads": 0, "recall_count": 0, "qualified_count": 0}

    id_col = _col(df, "리드 ID")
    status_col = _col(df, "리드 상태")
    if id_col is None or status_col is None:
        return {"lead": 0, "recall_excluded_leads": 0, "recall_count": 0, "qualified_count": 0}

    # lead: Closed + Qualified + Recall(콜백시간확정) unique 리드 ID
    def is_lead_status(s):
        v = _safe_str(s)
        if v == "Closed" or v == "Qualified":
            return True
        if "Recall" in v or "콜백시간확정" in v:
            return True
        return False
    df["_lead_ok"] = df[status_col].map(is_lead_status)
    df_lead = df[df["_lead_ok"]]
    lead = df_lead[id_col].nunique() if not df_lead.empty else 0

    # recall 제외: Closed + Qualified 만
    def is_recall_excluded(s):
        v = _safe_str(s)
        return v == "Closed" or v == "Qualified"
    df["_recall_excl"] = df[status_col].map(is_recall_excluded)
    df_recall_excl = df[df["_recall_excl"]]
    recall_excluded_leads = df_recall_excl[id_col].nunique() if not df_recall_excl.empty else 0

    # Recall 수: Recall(콜백시간확정)
    def is_recall(s):
        v = _safe_str(s)
        return "Recall" in v or "콜백시간확정" in v
    df["_recall"] = df[status_col].map(is_recall)
    df_r = df[df["_recall"]]
    recall_count = df_r[id_col].nunique() if not df_r.empty else 0

    # 기회 전환 수: Qualified unique 리드 ID
    df["_qual"] = df[status_col].astype(str).str.strip().eq("Qualified")
    df_q = df[df["_qual"]]
    qualified_count = df_q[id_col].nunique() if not df_q.empty else 0

    return {
        "lead": int(lead),
        "recall_excluded_leads": int(recall_excluded_leads),
        "recall_count": int(recall_count),
        "qualified_count": int(qualified_count),
    }


# ---------- 통화 지표 (kr_sales_call, 통화 시작 시간t 기준) ----------
def compute_call_metrics(call_df, start, end, owner_name=None):
    date_col = _col(call_df, "통화 시작 시간t", "통화 시작 시간")
    owner_col = _col(call_df, "담당자")
    if date_col is None:
        return {"call_count": 0, "call_time_seconds": 0, "avg_call_time_seconds": 0}
    dt = _to_date_series(call_df[date_col])
    mask = in_period(dt, start, end)
    df = call_df.loc[mask].copy()
    if owner_name and owner_col is not None:
        df = df[df[owner_col].astype(str).str.strip() == owner_name]
    if df.empty:
        return {"call_count": 0, "call_time_seconds": 0, "avg_call_time_seconds": 0}

    act_id = _col(df, "활동 id", "활동 id")
    talk_col = _col(df, "통화 대화 시간")
    call_type_col = _col(df, "통화 유형")
    if talk_col is None:
        return {"call_count": 0, "call_time_seconds": 0, "avg_call_time_seconds": 0}

    talk = pd.to_numeric(df[talk_col], errors="coerce").fillna(0)
    if act_id:
        df = df.copy()
        df["_talk"] = talk
        df["_type"] = df[call_type_col].astype(str).str.strip() if call_type_col else ""
        # 인바운드: 통화 대화 시간 >= 1, 아웃바운드: >= 0
        valid = (
            ((df["_type"].str.contains("인바운드", na=False)) & (df["_talk"] >= 1))
            | ((df["_type"].str.contains("아웃바운드", na=False)) & (df["_talk"] >= 0))
        )
        count = df.loc[valid, act_id].nunique()
    else:
        valid = talk >= 0
        count = int(valid.sum())
    total_sec = float(talk.sum())
    avg_sec = total_sec / count if count else 0
    return {
        "call_count": int(count),
        "call_time_seconds": total_sec,
        "avg_call_time_seconds": avg_sec,
    }


def format_duration(seconds):
    """초 -> 가능한 최대 단위 (일, 시, 분, 초)."""
    s = int(round(seconds))
    if s <= 0:
        return "0초"
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    parts = []
    if d:
        parts.append(f"{d}일")
    if h:
        parts.append(f"{h}시간")
    if m:
        parts.append(f"{m}분")
    if s or not parts:
        parts.append(f"{s}초")
    return " ".join(parts)


# ---------- oppts (kr_sales_performance, 작성 일자 기준) ----------
def compute_oppts(perf_df, start, end, owner_cc_id=None):
    """compute_appts_quote_created와 동일 — '작성 일자' 기준 고유 기회 ID 수."""
    return compute_appts_quote_created(perf_df, start, end, owner_cc_id=owner_cc_id)


def _mask_sichak_filled(df, sich_col):
    """시착일에 **파싱 가능한 일자(날짜)**가 있는 행만 True.
    빈 셀·날짜가 아닌 문자·임의 텍스트는 False (이전 str_ok 제거: '비어있지 않음'≠일자)."""
    if sich_col is None or sich_col not in df.columns:
        return pd.Series(False, index=df.index)
    ser = df[sich_col]
    # '2025-10-22', datetime, Timestamp 등
    dt = pd.to_datetime(ser, errors="coerce")
    ok = dt.notna()
    # 엑셀 일련번호(float)로만 읽힌 날짜 (pandas가 자동 변환 못 한 경우)
    num = pd.to_numeric(ser, errors="coerce")
    serial_like = num.notna() & (num >= 20000) & (num <= 60000)
    # pandas 2.x+: unit은 'D'(일) — 소문자 'd'는 TypeError
    try:
        serial_dt = pd.to_datetime(num, unit="D", origin="1899-12-30", errors="coerce")
    except (TypeError, ValueError):
        serial_dt = pd.Series(pd.NaT, index=ser.index)
    ok = ok | (serial_like & serial_dt.notna())
    return ok.reindex(df.index).fillna(False)


def _normalize_opp_id_scalar(x):
    """기회 ID를 비교 가능한 문자열로 (엑셀 숫자·공백 대응)."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip()
    if not s or s.lower() in ("nan", "none", "null", "#n/a"):
        return None
    # "12345.0" → "12345"
    if len(s) > 2 and s.endswith(".0") and s[:-2].replace("-", "").isdigit():
        s = s[:-2]
    return s


def _unique_opp_ids_from_mask(df, id_col, row_mask):
    """같은 행에서 row_mask가 True인 행만 사용해 기회 ID **고유 개수** 및 **집합** 산출.
    모든 비즈니스 조건은 행 단위 AND(마스크 하나로 결합) 후 unique."""
    if df.empty or not row_mask.any():
        return 0, set()
    sub = df.loc[row_mask, id_col]
    sub = sub.dropna()
    if sub.empty:
        return 0, set()
    normalized = sub.map(_normalize_opp_id_scalar)
    normalized = normalized.dropna()
    uniq = normalized.unique()
    ids = {u for u in uniq if u and str(u).lower() not in ("nan", "none", "null")}
    return len(ids), ids


# ---------- visits, trials, orders (일시) / closed_orders (종료) ----------
def compute_visits_trials(perf_df, start, end, owner_cc_id=None):
    """visits: 일시 기간 내 + 견적서 이름에 '최초' 포함 → **고유 기회 ID** 수.
    Orders: 시착일이 기간 내 + 진행상황 not empty and != '주문전취소' → **고유 기회 ID** 수.
    Trials: 시착일이 기간 내 + 단계=진행 중 + 시착일 유효 → **고유 기회 ID** 수.
    반환: visits, trials, order_ids(set)."""
    date_col    = _col(perf_df, "일시")
    id_col      = _col(perf_df, "기회 ID (18)", "기회 ID")
    est_col     = _col(perf_df, "견적서 이름")
    stage_col   = _col(perf_df, "단계")
    sich_col    = _col(perf_df, "시착일")
    prog_col    = _col(perf_df, "진행 상황")
    user_id_col = _col(perf_df, "User ID (18-digits)")
    if date_col is None or id_col is None:
        return 0, 0, set()

    # uid 필터 헬퍼
    def _apply_uid(df_in):
        if owner_cc_id and str(owner_cc_id).strip() and user_id_col is not None:
            return df_in[df_in[user_id_col].astype(str).str.strip() == str(owner_cc_id).strip()]
        return df_in

    # ── Visits: 일시 기간 내 + 견적서 이름에 '최초' 포함 + 상태='Visited' ──
    status_col_vt = _col(perf_df, "상태")
    dt   = _to_date_series(perf_df[date_col])
    mask = in_period(dt, start, end)
    df   = _apply_uid(perf_df.loc[mask].copy())
    visits = 0
    if not df.empty:
        est_ok = (
            df[est_col].astype(str).str.contains("최초", na=False)
            if (est_col and est_col in df.columns)
            else pd.Series(False, index=df.index)
        )
        st_ok = (
            df[status_col_vt].astype(str).str.strip().eq("Visited")
            if (status_col_vt and status_col_vt in df.columns)
            else pd.Series(False, index=df.index)
        )
        visit_ok = est_ok & st_ok
        visits, _ = _unique_opp_ids_from_mask(df, id_col, visit_ok)

    # ── Orders: 시착일 기준 + 진행상황 조건 ──────────────────────────────
    order_ids = set()
    trials    = 0
    if not sich_col or sich_col not in perf_df.columns:
        return visits, 0, set()

    sich_dt      = _to_date_series(perf_df[sich_col])
    sich_mask    = in_period(sich_dt, start, end)
    df_sich      = _apply_uid(perf_df.loc[sich_mask].copy())
    if not df_sich.empty:
        ord_mask = _mask_order_qualifying(df_sich, prog_col)
        _, order_ids = _unique_opp_ids_from_mask(df_sich, id_col, ord_mask)

        # ── Trials: 시착일 기준 + 단계=진행 중 + 시착일 유효 ──────────
        if stage_col:
            sich_ok    = _mask_sichak_filled(df_sich, sich_col)
            stg        = df_sich[stage_col].astype(str).str.strip()
            trial_mask = sich_ok & stg.eq("진행 중")
            trials, _  = _unique_opp_ids_from_mask(df_sich, id_col, trial_mask)

    return visits, trials, order_ids


def compute_closed_orders(perf_df, start, end, owner_cc_id=None):
    """인센확정일자(QID)가 기간 내이고, Orders 조건(진행상황 not empty & != 주문전취소)을
    만족하면서 단계가 '종료 및 유실' 또는 '종료 및 성공'인 고유 기회 ID 수.
    (Orders 중 최종 종료된 건 — 유실+성공 합산)"""
    date_col    = _col(perf_df, "QID", "인센확정일자") or _col(perf_df, "종료")
    id_col      = _col(perf_df, "기회 ID (18)", "기회 ID")
    prog_col    = _col(perf_df, "진행 상황")
    stage_col   = _col(perf_df, "단계")
    user_id_col = _col(perf_df, "User ID (18-digits)")
    if not date_col or not id_col or not prog_col or not stage_col:
        return 0
    dt   = _to_date_series(perf_df[date_col])
    mask = in_period(dt, start, end)
    df   = perf_df.loc[mask].copy()
    if owner_cc_id and str(owner_cc_id).strip() and user_id_col is not None:
        df = df[df[user_id_col].astype(str).str.strip() == str(owner_cc_id).strip()]
    if df.empty:
        return 0
    # Orders 동일 조건: 진행상황 not empty and != '주문전취소'
    order_qual = _mask_order_qualifying(df, prog_col)
    # 단계 종료 조건
    st       = df[stage_col].astype(str)
    stage_ok = st.str.contains("종료 및 유실", na=False) | st.str.contains("종료 및 성공", na=False)
    row_mask = order_qual & stage_ok
    n, _ = _unique_opp_ids_from_mask(df, id_col, row_mask)
    return n


def prev_period_range(ref_dt, period_key):
    """전일/전주/전월 등 직전 동일 길이 기간 (L3M·L6M은 약 3·6개월 앞선 기준일)."""
    y, m, d = ref_dt.year, ref_dt.month, ref_dt.day
    if period_key == "daily":
        return period_range(datetime(y, m, d) - timedelta(days=1), "daily")
    if period_key == "weekly":
        mon = ref_dt - timedelta(days=ref_dt.isoweekday() - 1)
        return period_range(mon - timedelta(days=7), "weekly")
    if period_key == "monthly":
        return period_range(datetime(y, m, 1) - timedelta(days=1), "monthly")
    if period_key == "L3M":
        return period_range(ref_dt - timedelta(days=92), "L3M")
    if period_key == "L6M":
        return period_range(ref_dt - timedelta(days=183), "L6M")
    return None, None


def _rates_for_range(leads_df, perf_df, call_df, start, end):
    if start is None or end is None:
        return None
    lead_m = compute_lead_metrics(leads_df, start, end)
    oppts = compute_oppts(perf_df, start, end)
    vr_m = compute_visits_vr(perf_df, start, end)
    visits, vr_denom = vr_m["visited"], vr_m["vr_denom"]
    _, trials, order_ids = compute_visits_trials(perf_df, start, end)
    orders = len(order_ids)
    closed_orders = compute_closed_orders(perf_df, start, end)
    sales_m = compute_sales_metrics(perf_df, start, end)
    qualified = lead_m["qualified_count"]
    lead = lead_m["lead"]
    recall_excl = lead_m["recall_excluded_leads"]
    cr2 = (qualified / lead * 100) if lead else 0
    cr2_cc = (qualified / recall_excl * 100) if recall_excl else 0
    vr = (visits / vr_denom * 100) if vr_denom else 0
    or_ = (orders / visits * 100) if visits else 0
    sr = (sales_m["sales"] / closed_orders * 100) if closed_orders else 0
    cr3 = (sales_m["sales"] / oppts * 100) if oppts else 0
    sales, devices = sales_m["sales"], sales_m["devices"]
    bin_rate = (devices / sales) if sales else 0
    return {
        "cr2": round(cr2, 2),
        "cr2_cc": round(cr2_cc, 2),
        "cr3": round(cr3, 2),
        "vr": round(vr, 2),
        "or_": round(or_, 2),
        "sr": round(sr, 2),
        "bin_rate": round(bin_rate, 2),
    }


# ---------- sales, devices, lost, lost_devices (종료 기준, 상태(1) 사용) ----------
# incentive_dashboard 로직 참고. 이 파일에서는 '상태' = Visited/No Show, '상태(1)' = Draft/Closed/Final Quote
def _status1(row):
    return _safe_str(row.get("상태(1)"))


def _stage(row):
    return _safe_str(row.get("단계"))


def _progress(row):
    return _safe_str(row.get("진행 상황"))


def is_success_sales(row):
    """성공: 단계=종료 및 성공, 진행 상황에 '구매', 상태(1)=Final Quote."""
    return (
        "종료 및 성공" in _stage(row)
        and "구매" in _progress(row)
        and _status1(row) == "Final Quote"
    )


def is_fail_sales(row):
    """실패: 단계=종료 및 유실, 상태(1)=Rejected 또는 Closed."""
    return "종료 및 유실" in _stage(row) and _status1(row) in ("Rejected", "Closed")


# ── 벡터화 헬퍼 (iterrows 대체) ────────────────────────────────────────────
def _mask_success_sales_vec(df):
    """DataFrame 전체에 대해 성공 판매 행 마스크를 한 번에 계산."""
    stage_col   = _col(df, "단계")
    status1_col = _col(df, "상태(1)")
    prog_col    = _col(df, "진행 상황")
    if not stage_col or not status1_col or not prog_col:
        return pd.Series(False, index=df.index)
    return (
        df[stage_col].astype(str).str.contains("종료 및 성공", na=False)
        & df[prog_col].astype(str).str.contains("구매", na=False)
        & df[status1_col].astype(str).str.strip().eq("Final Quote")
    )


def _mask_fail_sales_vec(df):
    """DataFrame 전체에 대해 실패 판매 행 마스크를 한 번에 계산."""
    stage_col   = _col(df, "단계")
    status1_col = _col(df, "상태(1)")
    if not stage_col or not status1_col:
        return pd.Series(False, index=df.index)
    return (
        df[stage_col].astype(str).str.contains("종료 및 유실", na=False)
        & df[status1_col].astype(str).str.strip().isin(["Rejected", "Closed"])
    )


def _build_supply_price_dict(supply_price_df):
    """supply_price 시트에서 제품명→공급가(원) dict 생성."""
    if supply_price_df is None or supply_price_df.empty:
        return {}
    prod_col = _col(supply_price_df, "product")
    sp_col   = _col(supply_price_df, "supply_price")
    if not prod_col or not sp_col:
        return {}
    result = {}
    for _, row in supply_price_df.iterrows():
        pname = _safe_str(row.get(prod_col, "")).strip()
        try:
            price = float(row[sp_col]) if pd.notna(row[sp_col]) else 0.0
        except (ValueError, TypeError):
            price = 0.0
        if pname:
            result[pname] = price
    return result


def compute_supply_cost_total(perf_df, start, end, sp_dict, owner_cc_id=None):
    """인센확정일자(QID) 기준 성공 매출 건의 공급가 합산 (보청기 좌·우 각각 매칭).
    기회 ID 기준 중복 제거 후 합산."""
    if not sp_dict:
        return 0.0
    user_id_col = _col(perf_df, "User ID (18-digits)")
    close_col   = _col(perf_df, "QID", "인센확정일자") or _col(perf_df, "종료")
    opp_id_col  = _col(perf_df, "기회 ID (18)", "기회 ID")
    left_col    = _col(perf_df, "보청기 - 좌", "보청기-좌")
    right_col   = _col(perf_df, "보청기 - 우", "보청기-우")
    if not close_col:
        return 0.0
    close_dt    = _to_date_series(perf_df[close_col])
    period_mask = in_period(close_dt, start, end)
    succ_mask   = _mask_success_sales_vec(perf_df)
    if owner_cc_id and str(owner_cc_id).strip() and user_id_col:
        uid_mask = perf_df[user_id_col].astype(str).str.strip() == str(owner_cc_id).strip()
    else:
        uid_mask = pd.Series(True, index=perf_df.index)
    sub = perf_df[period_mask & succ_mask & uid_mask].copy()
    # 기회 ID 기준 중복 제거 (동일 기회가 여러 행이면 첫 번째 행만 사용)
    if opp_id_col and opp_id_col in sub.columns:
        sub = sub.drop_duplicates(subset=[opp_id_col])
    total = 0.0
    if left_col and left_col in sub.columns:
        for val in sub[left_col].dropna():
            pname = _safe_str(val).strip()
            if pname:
                total += sp_dict.get(pname, 0.0)
    if right_col and right_col in sub.columns:
        for val in sub[right_col].dropna():
            pname = _safe_str(val).strip()
            if pname:
                total += sp_dict.get(pname, 0.0)
    return total


def _count_devices(df_sub, left_col, right_col):
    """보청기-좌/우 하나라도 채워진 행의 대수 합산 (좌+우 각각 독립 카운트).
    notna() + 빈문자열 제외로 nullable dtype 안전하게 처리."""
    n = 0
    for col in [left_col, right_col]:
        if col and col in df_sub.columns:
            not_null = df_sub[col].notna()
            not_empty = df_sub[col].astype(str).str.strip().ne("")
            n += int((not_null & not_empty).sum())
    return n


def compute_device_counts(perf_df, start, end, owner_cc_id=None):
    """주문대수·반품대수 계산.

    Step 1 — qualifying IDs: 동의서 작성일(TAD)이 기간 내 + 진행상황 not empty and != '주문전취소'
    Step 2 — 해당 IDs의 모든 행 조회 (TAD 필터 제거): 기기 정보는 다른 행에 있을 수 있음
    주문대수: 각 qualifying ID의 모든 행 중 보청기-좌 채워진 ID 수 + 보청기-우 채워진 ID 수
    반품대수: qualifying IDs 중 단계='종료 및 유실'인 ID에 대해 동일 방식으로 집계
    """
    user_id_col = _col(perf_df, "User ID (18-digits)")
    tad_col     = _col(perf_df, "TAD", "동의서 작성일")
    prog_col    = _col(perf_df, "진행 상황")
    stage_col   = _col(perf_df, "단계")
    opp_id_col  = _col(perf_df, "기회 ID (18)", "기회 ID")
    left_col    = _col(perf_df, "보청기 - 좌", "보청기-좌")
    right_col   = _col(perf_df, "보청기 - 우", "보청기-우")

    if not tad_col or not prog_col or not opp_id_col:
        return 0, 0

    # uid 마스크
    if owner_cc_id and str(owner_cc_id).strip() and user_id_col:
        uid_mask = perf_df[user_id_col].astype(str).str.strip() == str(owner_cc_id).strip()
    else:
        uid_mask = pd.Series(True, index=perf_df.index)

    # Step 1: qualifying IDs (동의서 작성일(TAD) 기간 내 + 진행상황 조건)
    tad_dt      = _to_date_series(perf_df[tad_col])
    period_mask = in_period(tad_dt, start, end)
    base        = perf_df[period_mask & uid_mask]
    if base.empty:
        return 0, 0
    qual_ids = set(
        base[_mask_order_qualifying(base, prog_col)][opp_id_col]
        .map(_normalize_opp_id_scalar).dropna()
    )
    if not qual_ids:
        return 0, 0

    # Step 2: qualifying IDs의 모든 행 (시착일 필터 없이, uid 필터만)
    all_uid_rows   = perf_df[uid_mask].copy()
    norm_id_series = all_uid_rows[opp_id_col].map(_normalize_opp_id_scalar)
    all_qual_rows  = all_uid_rows[norm_id_series.isin(qual_ids)]

    def _filled(series):
        return series.notna() & series.astype(str).str.strip().ne("")

    def _devices_for_rows(rows):
        """qualifying 행들에서 ID별 보청기-좌/우 채워진 여부 → 대수 합산."""
        if rows.empty:
            return 0
        temp = pd.DataFrame({"_id": rows[opp_id_col].map(_normalize_opp_id_scalar)})
        temp["_l"] = _filled(rows[left_col]).values  if left_col  and left_col  in rows.columns else False
        temp["_r"] = _filled(rows[right_col]).values if right_col and right_col in rows.columns else False
        per_id = temp.groupby("_id")[["_l", "_r"]].any()
        return int(per_id["_l"].sum()) + int(per_id["_r"].sum())

    n_ordered = _devices_for_rows(all_qual_rows)

    # 반품: qualifying IDs 중 어느 행이라도 단계='종료 및 유실'인 ID
    n_returned = 0
    if stage_col and stage_col in all_qual_rows.columns:
        stg_s      = all_qual_rows[stage_col].astype(str)
        return_ids = set(
            all_qual_rows[stg_s.str.contains("종료 및 유실", na=False)][opp_id_col]
            .map(_normalize_opp_id_scalar).dropna()
        )
        if return_ids:
            return_rows = all_qual_rows[
                all_qual_rows[opp_id_col].map(_normalize_opp_id_scalar).isin(return_ids)
            ]
            n_returned = _devices_for_rows(return_rows)

    return n_ordered, n_returned


def compute_margin(perf_df, leads_df, cc_name, cc_id, triple, sp_dict):
    """L3M(triple) 기간의 월평균 마진 계산.

    월평균 마진 = 월평균 매출 - 월평균 비용 합계
    1. marketing_cost  = (L3M 리드수 × 31,000) / 3
    2. fitting_fee     = 월평균 매출 × 0.25
    3. sales_incentive = 월평균 매출 × 0.18 / 3
    4. supply_monthly  = L3M 공급가 합 / 3
    5. return_fee      = MAX(((반품대수 - 주문대수×0.25) × 48,300) / 3, 0)
    """
    start = datetime(triple[0][0], triple[0][1], 1)
    _, last = monthrange(triple[-1][0], triple[-1][1])
    end = datetime(triple[-1][0], triple[-1][1], last, 23, 59, 59)

    sm = compute_sales_metrics(perf_df, start, end, owner_cc_id=cc_id)
    total_revenue  = sm["revenue"]
    monthly_avg_rev = total_revenue / 3.0

    lead_m       = compute_lead_metrics(leads_df, start, end, owner_name=cc_name)
    n_leads      = lead_m["lead"]
    marketing_cost    = (n_leads * 31_000) / 3.0
    fitting_fee       = monthly_avg_rev * 0.25
    sales_incentive   = monthly_avg_rev * 0.18 / 3.0
    base_salary       = 2_200_000.0
    supply_total      = compute_supply_cost_total(perf_df, start, end, sp_dict, owner_cc_id=cc_id)
    supply_monthly    = supply_total / 3.0
    n_ordered, n_returned = compute_device_counts(perf_df, start, end, owner_cc_id=cc_id)
    return_fee        = max(((n_returned - n_ordered * 0.25) * 48_300) / 3.0, 0.0)

    total_cost  = marketing_cost + fitting_fee + sales_incentive + base_salary + supply_monthly + return_fee
    margin      = monthly_avg_rev - total_cost
    margin_rate = (margin / monthly_avg_rev * 100.0) if monthly_avg_rev > 0 else 0.0

    return {
        "margin_monthly":  round(margin, 1),
        "revenue_monthly": round(monthly_avg_rev, 1),
        "margin_rate":     round(margin_rate, 1),
        "marketing_cost":  round(marketing_cost, 1),
        "fitting_fee":     round(fitting_fee, 1),
        "sales_incentive": round(sales_incentive, 1),
        "base_salary":     round(base_salary, 1),
        "supply_monthly":  round(supply_monthly, 1),
        "return_fee":      round(return_fee, 1),
        "total_cost":      round(total_cost, 1),
    }


def _series_incent_units_vec(df):
    """인센 인식 대수를 벡터화로 계산. 성공 조건 불만족 행은 0."""
    left_col    = _col(df, "보청기 - 좌")
    right_col   = _col(df, "보청기 - 우")
    status1_col = _col(df, "상태(1)")
    prog_col    = _col(df, "진행 상황")
    stage_col   = _col(df, "단계")
    prog2_col   = _col(df, "시착/구매 프로그램")
    if not left_col or not right_col or not status1_col:
        return pd.Series(0, index=df.index, dtype=int)
    status1  = df[status1_col].astype(str).str.strip()
    progress = df[prog_col].astype(str)  if prog_col   else pd.Series("", index=df.index)
    stage    = df[stage_col].astype(str) if stage_col  else pd.Series("", index=df.index)
    program  = df[prog2_col].fillna("").astype(str) if prog2_col else pd.Series("", index=df.index)
    left     = df[left_col].fillna("").astype(str).str.strip()
    right    = df[right_col].fillna("").astype(str).str.strip()
    excluded = (
        (status1.eq("Denied")        & progress.str.contains("시착-미반납", na=False))
        | (stage.str.contains("진행 중", na=False) & status1.eq("Draft"))
        | ~(stage.str.contains("종료 및 성공", na=False) & status1.eq("Final Quote"))
    )
    left_ok  = left.ne("").astype(int)
    right_ok = right.ne("").astype(int)
    count = left_ok + right_ok
    cros  = (
        (left.str.contains("CROS", na=False)  & left_ok.astype(bool)).astype(int)
        + (right.str.contains("CROS", na=False) & right_ok.astype(bool)).astype(int)
        + program.str.contains("보상판매", na=False).astype(int)
    )
    result = ((count - cros).clip(lower=0) * (~(excluded | (count == 0))).astype(int))
    return result.astype(int)


def _series_fail_units_vec(df):
    """실패 대수를 벡터화로 계산."""
    left_col  = _col(df, "보청기 - 좌")
    right_col = _col(df, "보청기 - 우")
    if not left_col or not right_col:
        return pd.Series(0, index=df.index, dtype=int)
    left  = df[left_col].fillna("").astype(str).str.strip()
    right = df[right_col].fillna("").astype(str).str.strip()
    left_ok  = left.ne("").astype(int)
    right_ok = right.ne("").astype(int)
    count = left_ok + right_ok
    cros  = (
        (left.str.contains("CROS", na=False)  & left_ok.astype(bool)).astype(int)
        + (right.str.contains("CROS", na=False) & right_ok.astype(bool)).astype(int)
    )
    return (count - cros).clip(lower=0).astype(int)
# ── 벡터화 헬퍼 끝 ────────────────────────────────────────────────────────


def incent_recognized_units_sales(row):
    """인센인식대수 (성공). 상태(1) 사용."""
    stage, status1, progress = _stage(row), _status1(row), _progress(row)
    program = _safe_str(row.get("시착/구매 프로그램"))
    left = _safe_str(row.get("보청기 - 좌"))
    right = _safe_str(row.get("보청기 - 우"))
    if status1 == "Denied" and "시착-미반납" in progress:
        return 0
    if "진행 중" in stage and status1 == "Draft":
        return 0
    if "종료 및 성공" not in stage or status1 != "Final Quote":
        return 0
    left_ok = bool(left and left.strip())
    right_ok = bool(right and right.strip())
    count = (1 if left_ok else 0) + (1 if right_ok else 0)
    if count == 0:
        return 0
    cros = 0
    if left_ok and "CROS" in left:
        cros += 1
    if right_ok and "CROS" in right:
        cros += 1
    if "보상판매" in program:
        cros += 1
    return max(0, count - cros)


def row_quantity_fail_sales(row):
    """실패 대수."""
    left = _safe_str(row.get("보청기 - 좌"))
    right = _safe_str(row.get("보청기 - 우"))
    left_ok = bool(left and left.strip())
    right_ok = bool(right and right.strip())
    count = (1 if left_ok else 0) + (1 if right_ok else 0)
    if count == 0:
        return 0
    cros = 0
    if left_ok and "CROS" in left:
        cros += 1
    if right_ok and "CROS" in right:
        cros += 1
    return max(0, count - cros)


def compute_sales_metrics(perf_df, start, end, owner_cc_id=None):
    """인센확정일자(QID) 기준. 총액은 '총액' 사용 (총액 통화 제외)."""
    date_col = _col(perf_df, "QID", "인센확정일자") or _col(perf_df, "종료")
    id_col = _col(perf_df, "기회 ID (18)", "기회 ID")
    amt_col = _col_amount(perf_df)
    user_id_col = _col(perf_df, "User ID (18-digits)")
    if date_col is None:
        return {"sales": 0, "devices": 0, "lost": 0, "lost_devices": 0, "revenue": 0.0}
    dt = _to_date_series(perf_df[date_col])
    mask = in_period(dt, start, end)
    df = perf_df.loc[mask].copy()
    if owner_cc_id and str(owner_cc_id).strip() and user_id_col is not None:
        df = df[df[user_id_col].astype(str).str.strip() == str(owner_cc_id).strip()]
    if df.empty:
        return {"sales": 0, "devices": 0, "lost": 0, "lost_devices": 0, "revenue": 0.0}

    succ_mask = _mask_success_sales_vec(df)
    fail_mask = _mask_fail_sales_vec(df)
    succ_df   = df.loc[succ_mask]
    fail_df   = df.loc[fail_mask]

    if id_col and not succ_df.empty:
        sales, _ = _unique_opp_ids_from_mask(succ_df, id_col, pd.Series(True, index=succ_df.index))
    else:
        sales = int(succ_mask.sum())

    devices  = int(_series_incent_units_vec(succ_df).sum()) if not succ_df.empty else 0
    revenue  = float(succ_df[amt_col].apply(_safe_float).sum()) if (amt_col and not succ_df.empty) else 0.0

    if id_col and not fail_df.empty:
        lost, _ = _unique_opp_ids_from_mask(fail_df, id_col, pd.Series(True, index=fail_df.index))
    else:
        lost = int(fail_mask.sum())

    lost_devices = int(_series_fail_units_vec(fail_df).sum()) if not fail_df.empty else 0

    return {
        "sales": sales,
        "devices": devices,
        "lost": lost,
        "lost_devices": lost_devices,
        "revenue": revenue,
    }


def compute_appts_quote_created(perf_df, start, end, owner_cc_id=None):
    """'작성 일자' 기준으로 기간 내인 행의 고유 기회 ID 수."""
    date_col = _col(
        perf_df,
        "작성 일자",
        "작성일",
        "작성일자",
        "예약 및 견적: 만든 날짜",
        "예약 및 견적 만든 날짜",
        "예약 및 견적:만든 날짜",
    )
    id_col = _col(perf_df, "기회 ID (18)", "기회 ID")
    user_id_col = _col(perf_df, "User ID (18-digits)")
    if date_col is None or id_col is None:
        return 0
    dt = _to_date_series(perf_df[date_col])
    mask = in_period(dt, start, end)
    df = perf_df.loc[mask]
    if owner_cc_id and str(owner_cc_id).strip() and user_id_col is not None:
        df = df[df[user_id_col].astype(str).str.strip() == str(owner_cc_id).strip()]
    if df.empty:
        return 0
    ids = df[id_col].dropna().astype(str).str.strip()
    ids = ids[ids.ne("") & ~ids.str.lower().isin(("nan", "none", "null"))]
    return int(ids.nunique())


def compute_visits_ilsi_unique(perf_df, start, end, owner_cc_id=None):
    """일시 기준 Visited 상태인 고유 기회 ID 수 (compute_visits_vr["visited"] 위임)."""
    return compute_visits_vr(perf_df, start, end, owner_cc_id=owner_cc_id)["visited"]


def compute_visits_vr(perf_df, start, end, owner_cc_id=None):
    """일시 기준 VR 계산용: 견적서 이름에 '최초' 포함(분자) + (분자+No Show+Canceled)(분모) 반환.
    VR = visited / vr_denom × 100"""
    date_col    = _col(perf_df, "일시")
    id_col      = _col(perf_df, "기회 ID (18)", "기회 ID")
    status_col  = _col(perf_df, "상태")
    est_col     = _col(perf_df, "견적서 이름")
    user_id_col = _col(perf_df, "User ID (18-digits)")
    if not date_col or not id_col:
        return {"visited": 0, "vr_denom": 0}
    dt   = _to_date_series(perf_df[date_col])
    mask = in_period(dt, start, end)
    df   = perf_df.loc[mask].copy()
    if owner_cc_id and str(owner_cc_id).strip() and user_id_col is not None:
        df = df[df[user_id_col].astype(str).str.strip() == str(owner_cc_id).strip()]
    if df.empty:
        return {"visited": 0, "vr_denom": 0}
    ids = df[id_col].map(_normalize_opp_id_scalar)
    # Visits 분자: 견적서 이름에 '최초' 포함 AND 상태='Visited'
    est_ok = (
        df[est_col].astype(str).str.contains("최초", na=False)
        if (est_col and est_col in df.columns)
        else pd.Series(False, index=df.index)
    )
    if status_col and status_col in df.columns:
        st = df[status_col].astype(str).str.strip()
        visited_mask = est_ok & st.eq("Visited")
        # VR 분모: Visits(최초+Visited) + No Show + Canceled (일시 기준)
        denom_mask = visited_mask | st.isin(["No Show", "Canceled"])
    else:
        visited_mask = est_ok
        denom_mask   = est_ok
    visited  = int(ids[visited_mask].dropna().nunique())
    vr_denom = int(ids[denom_mask].dropna().nunique())
    return {"visited": visited, "vr_denom": vr_denom}


def _mask_order_qualifying(df, prog_col):
    """Orders qualifying 마스크: 진행상황이 비어있지 않고 '주문전취소'가 아닌 행."""
    if not prog_col or prog_col not in df.columns:
        return pd.Series(False, index=df.index)
    prog_s = df[prog_col].astype(str).str.strip()
    return prog_s.ne("") & prog_s.ne("nan") & prog_s.ne("NaN") & prog_s.ne("None") & ~prog_s.eq("주문전취소")


def compute_orders_on_tad(perf_df, start, end, owner_cc_id=None):
    """동의서 작성일(TAD)이 기간 내이고 진행상황 조건 만족하는 unique ID 수.
    (Orders KPI 기준: 동의서 작성일 기준으로 측정)"""
    tad_col     = _col(perf_df, "TAD", "동의서 작성일")
    id_col      = _col(perf_df, "기회 ID (18)", "기회 ID")
    prog_col    = _col(perf_df, "진행 상황")
    user_id_col = _col(perf_df, "User ID (18-digits)")
    if not tad_col or not id_col or not prog_col:
        return 0
    dt   = _to_date_series(perf_df[tad_col])
    mask = in_period(dt, start, end)
    df   = perf_df.loc[mask].copy()
    if owner_cc_id and str(owner_cc_id).strip() and user_id_col is not None:
        df = df[df[user_id_col].astype(str).str.strip() == str(owner_cc_id).strip()]
    if df.empty:
        return 0
    ord_mask = _mask_order_qualifying(df, prog_col)
    n, _ = _unique_opp_ids_from_mask(df, id_col, ord_mask)
    return n


# 하위 호환 alias
compute_orders_on_sichak = compute_orders_on_tad


def compute_trials_on_sichak(perf_df, start, end, owner_cc_id=None):
    """시착일이 기간 내이고 진행 중+시착일 유효인 행의 고유 기회 ID 수."""
    sich_col = _col(perf_df, "시착일")
    id_col = _col(perf_df, "기회 ID (18)", "기회 ID")
    stage_col = _col(perf_df, "단계")
    user_id_col = _col(perf_df, "User ID (18-digits)")
    if not sich_col or not id_col or not stage_col:
        return 0
    dt = _to_date_series(perf_df[sich_col])
    mask = in_period(dt, start, end)
    df = perf_df.loc[mask].copy()
    if owner_cc_id and str(owner_cc_id).strip() and user_id_col is not None:
        df = df[df[user_id_col].astype(str).str.strip() == str(owner_cc_id).strip()]
    if df.empty:
        return 0
    sich_ok = _mask_sichak_filled(df, sich_col)
    stg = df[stage_col].astype(str).str.strip()
    in_progress = stg.eq("진행 중")
    trial_mask = sich_ok & in_progress
    n, _ = _unique_opp_ids_from_mask(df, id_col, trial_mask)
    return n


# ---------- owner / team 매핑 (cc 시트) ----------
def team_display_name(team_value):
    """팀 표시명: 예) team='김수언' → '팀 수언' (이름 부분만)."""
    t = _safe_str(team_value)
    if not t:
        return "팀"
    if len(t) > 1:
        return "팀 " + t[1:]
    return "팀 " + t


def build_owner_mapping(cc_df):
    """cc 시트: team(팀장=팀명), cc(담당자=팀원), cc_id. 팀 표시명 = 팀 수언 형식."""
    if cc_df is None or cc_df.empty:
        return {}, {}, [], []
    team_col = _col(cc_df, "team", "팀")
    cc_col = _col(cc_df, "cc")
    cc_id_col = _col(cc_df, "cc_id")
    name_to_team = {}
    name_to_cc_id = {}
    team_to_members = {}  # team_value -> [cc names]
    teams_order = []  # unique team values order
    for _, r in cc_df.iterrows():
        name = _safe_str(r.get(cc_col)) if cc_col else ""
        if not name:
            continue
        team_val = _safe_str(r.get(team_col)) if team_col else ""
        name_to_team[name] = team_val
        name_to_cc_id[name] = _safe_str(r.get(cc_id_col)) if cc_id_col else ""
        if team_val not in team_to_members:
            team_to_members[team_val] = []
            teams_order.append(team_val)
        team_to_members[team_val].append(name)
    # 팀 표시명 목록: 팀 수언, 팀 OOO ...
    teams_with_display = [{"value": t, "display": team_display_name(t), "members": team_to_members[t]} for t in teams_order]
    return name_to_team, name_to_cc_id, teams_with_display, list(name_to_team.keys())


# ---------- 담당자별 기간 지표 (한 period 한 명) ----------
def _metrics_one_period(leads_df, perf_df, call_df, start, end, owner_name, owner_cc_id):
    lead_m = compute_lead_metrics(leads_df, start, end, owner_name=owner_name)
    call_m = compute_call_metrics(call_df, start, end, owner_name=owner_name)
    oppts = compute_oppts(perf_df, start, end, owner_cc_id=owner_cc_id)
    vr_m = compute_visits_vr(perf_df, start, end, owner_cc_id=owner_cc_id)
    visits, vr_denom = vr_m["visited"], vr_m["vr_denom"]
    _, trials, order_ids = compute_visits_trials(perf_df, start, end, owner_cc_id=owner_cc_id)
    orders = len(order_ids)
    closed_orders = compute_closed_orders(perf_df, start, end, owner_cc_id=owner_cc_id)
    sales_m = compute_sales_metrics(perf_df, start, end, owner_cc_id=owner_cc_id)
    qualified = lead_m["qualified_count"]
    lead = lead_m["lead"]
    recall_excl = lead_m["recall_excluded_leads"]
    cr2 = (qualified / lead * 100) if lead else 0
    cr2_cc = (qualified / recall_excl * 100) if recall_excl else 0
    vr = (visits / vr_denom * 100) if vr_denom else 0
    or_ = (orders / visits * 100) if visits else 0
    sr = (sales_m["sales"] / closed_orders * 100) if closed_orders else 0
    cr3 = (sales_m["sales"] / oppts * 100) if oppts else 0
    revenue = sales_m["revenue"]
    sales = sales_m["sales"]
    devices = sales_m["devices"]
    abv = (revenue / sales) if sales else 0
    asp = (revenue / devices) if devices else 0
    bin_rate = (devices / sales) if sales else 0
    return {
        "lead": lead, "recall_excluded_leads": recall_excl, "recall_count": lead_m["recall_count"],
        "qualified_count": qualified, "oppts": oppts, "cr2": round(cr2, 2), "cr2_cc": round(cr2_cc, 2),
        "call_count": call_m["call_count"], "call_time_seconds": call_m["call_time_seconds"],
        "call_time_display": format_duration(call_m["call_time_seconds"]),
        "avg_call_time_display": format_duration(call_m["avg_call_time_seconds"]) if call_m["call_count"] else "0초",
        "visits": visits, "vr_denom": vr_denom,
        "trials": trials, "orders": orders, "closed_orders": closed_orders, "sales": sales, "devices": devices,
        "lost": sales_m["lost"], "lost_devices": sales_m["lost_devices"], "revenue": revenue,
        "abv": abv, "asp": asp, "bin_rate": round(bin_rate, 2),
        "vr": round(vr, 2), "or_": round(or_, 2), "sr": round(sr, 2), "cr3": round(cr3, 2),
    }


def _hire_tenure_days(ref_dt, hire_val):
    hp = pd.to_datetime(hire_val, errors="coerce")
    if pd.isna(hp):
        return None
    return int((pd.Timestamp(ref_dt).normalize() - hp.normalize()).days)


def _is_excluded_pip_team(team_val):
    t = _safe_str(team_val).strip()
    return t == "두진경" or "두진경" in t


def _is_excluded_pip(name, team_val):
    if _is_excluded_pip_team(team_val):
        return True
    if _safe_str(name).strip() == "권보선":
        return True
    return False


def three_months_excluding_current(ref_dt):
    """이번 달을 제외한 직전 3개 달력월 (과거→최근 순). 예: 3월 기준 → 12월, 1월, 2월."""
    y, m = ref_dt.year, ref_dt.month
    triple = []
    for k in range(3, 0, -1):
        mm = m - k
        yy = y
        while mm < 1:
            mm += 12
            yy -= 1
        triple.append((yy, mm))
    labels = [f"{mm}월" for _, mm in triple]
    return triple, labels


def cc_three_month_revenue(perf_df, cc_id, triple):
    """triple 각 달의 성공 매출 합 (과거→최근 순). 벡터화."""
    user_id_col = _col(perf_df, "User ID (18-digits)")
    date_col    = _col(perf_df, "종료")
    amt_col     = _col_amount(perf_df)
    revs = [0.0] * len(triple)
    if not cc_id or not user_id_col or not date_col or not amt_col:
        return [round(x, 0) for x in revs]
    uid_s = str(cc_id).strip()
    sub = perf_df.loc[perf_df[user_id_col].astype(str).str.strip() == uid_s]
    if sub.empty:
        return [round(x, 0) for x in revs]
    close_dt  = _to_date_series(sub[date_col])
    succ_mask = _mask_success_sales_vec(sub)
    amounts   = sub[amt_col].apply(_safe_float)
    for idx, (yy, mm) in enumerate(triple):
        start = datetime(yy, mm, 1)
        _, last = monthrange(yy, mm)
        end = datetime(yy, mm, last, 23, 59, 59)
        month_mask = in_period(close_dt, start, end) & succ_mask
        revs[idx] = float(amounts[month_mask].sum())
    return [round(x, 0) for x in revs]


def cc_l3m_three_month_revenue(perf_df, cc_id, ref_dt):
    """직전 3개 달력월(이번 달 제외) 각각 성공 매출 합."""
    triple, labels = three_months_excluding_current(ref_dt)
    revs = cc_three_month_revenue(perf_df, cc_id, triple)
    return revs, labels


def pipeline_trial_rows_for_cc(perf_df, cc_id):
    """현재 Trials(진행 중+시착일) 파이프라인 행 — 상세 페이지 테이블용.
    동의서 작성일(TAD)과 시착일 모두 반환."""
    user_id_col = _col(perf_df, "User ID (18-digits)")
    stage_col   = _col(perf_df, "단계")
    sich_col    = _col(perf_df, "시착일")
    tad_col     = _col(perf_df, "TAD", "동의서 작성일")
    id_col      = _col(perf_df, "기회 ID (18)", "기회 ID")
    partner_col = _col(perf_df, "파트너 청각사", "Partner Audiologist", "파트너", "청각사")
    prog_col    = _col(perf_df, "시착/구매 프로그램", "시착 구매 프로그램")
    left_col    = _col(perf_df, "보청기 - 좌", "보청기-좌")
    right_col   = _col(perf_df, "보청기 - 우", "보청기-우")
    if not cc_id or not user_id_col or not stage_col or not sich_col or not id_col:
        return []
    uid_s = str(cc_id).strip()
    df = perf_df.loc[perf_df[user_id_col].astype(str).str.strip() == uid_s].copy()
    if df.empty:
        return []
    sich_ok = _mask_sichak_filled(df, sich_col)
    stg = df[stage_col].astype(str).str.strip()
    mask = sich_ok & stg.eq("진행 중")
    sub = df.loc[mask]
    if sub.empty:
        return []
    seen = set()
    rows = []
    for _, r in sub.iterrows():
        lv = _safe_str(r.get(left_col)) if left_col else ""
        rv = _safe_str(r.get(right_col)) if right_col else ""
        if not lv.strip() and not rv.strip():
            continue
        oid = _normalize_opp_id_scalar(r.get(id_col))
        if not oid or oid in seen:
            continue
        seen.add(oid)
        dt_sich = pd.to_datetime(r.get(sich_col), errors="coerce")
        sich_str = dt_sich.strftime("%Y.%m.%d") if not pd.isna(dt_sich) else "—"
        # 동의서 작성일(TAD)
        tad_str = "—"
        if tad_col:
            dt_tad = pd.to_datetime(r.get(tad_col), errors="coerce")
            if not pd.isna(dt_tad):
                tad_str = dt_tad.strftime("%Y.%m.%d")
        rows.append(
            {
                "opp_id":  str(oid),
                "tad":     tad_str,
                "sichak":  sich_str,
                "partner": _safe_str(r.get(partner_col)) if partner_col else "—",
                "program": _safe_str(r.get(prog_col))    if prog_col    else "—",
                "left":    lv or "—",
                "right":   rv or "—",
                "notes":   "—",
            }
        )
    return rows


def upcoming_visit_rows_for_cc(perf_df, cc_id, ref_dt=None):
    """오늘 이후 방문 예정 행 — 견적서 이름 '최초' 포함, No Show/Canceled 제외."""
    now = pd.Timestamp(ref_dt) if ref_dt else pd.Timestamp.now()
    # 오늘 00:00:00 이후 (오늘 포함)
    today_start = now.normalize()

    user_id_col = _col(perf_df, "User ID (18-digits)")
    id_col      = _col(perf_df, "기회 ID (18)", "기회 ID")
    partner_col = _col(perf_df, "파트너 청각사", "Partner Audiologist", "파트너", "청각사")
    date_col    = _col(perf_df, "일시")
    status_col  = _col(perf_df, "상태")
    est_col     = _col(perf_df, "견적서 이름")
    if not cc_id or not user_id_col or not id_col or not date_col:
        return []
    uid_s = str(cc_id).strip()
    df = perf_df.loc[perf_df[user_id_col].astype(str).str.strip() == uid_s].copy()
    if df.empty:
        return []
    dt = pd.to_datetime(df[date_col], errors="coerce")
    # 오늘 이후 일정만
    mask = dt >= today_start
    # 견적서 이름에 '최초' 포함
    if est_col and est_col in df.columns:
        mask = mask & df[est_col].astype(str).str.contains("최초", na=False)
    # 상태가 No Show/Canceled가 아닌 것만
    if status_col:
        st = df[status_col].astype(str).str.strip()
        mask = mask & ~st.isin(["No Show", "Canceled"])
    sub = df.loc[mask].copy()
    if sub.empty:
        return []
    dow_kr = ["월", "화", "수", "목", "금", "토", "일"]
    seen = set()
    rows = []
    for _, r in sub.sort_values(date_col).iterrows():
        oid = _normalize_opp_id_scalar(r.get(id_col))
        if not oid or oid in seen:
            continue
        seen.add(oid)
        dv = pd.to_datetime(r.get(date_col), errors="coerce")
        if not pd.isna(dv):
            dstr  = dv.strftime("%m-%d") + f" ({dow_kr[dv.weekday()]})"
            tstr  = dv.strftime("%H:%M")
        else:
            dstr = "—"
            tstr = "—"
        partner = _safe_str(r.get(partner_col)) if partner_col else "—"
        rows.append({"opp_id": str(oid), "partner": partner, "ilsi": dstr, "time": tstr})
    return rows


def _pip_shift_month(y, m, delta):
    """delta 개월 이동한 (연, 월)."""
    mm = m - 1 + delta
    y2 = y + mm // 12
    m2 = mm % 12 + 1
    return y2, m2


def cc_revenue_won_range(perf_df, cc_id, start, end):
    sm = compute_sales_metrics(perf_df, start, end, owner_cc_id=cc_id)
    return float(sm["revenue"])


def cc_month_partial_revenue_m(perf_df, cc_id, ref_dt):
    """이번 달 1일~기준일 성공 매출(백만원)."""
    if not cc_id:
        return 0.0
    y, m = ref_dt.year, ref_dt.month
    start = datetime(y, m, 1)
    end = datetime.combine(ref_dt.date(), time(23, 59, 59))
    if end < start:
        return 0.0
    return round(cc_revenue_won_range(perf_df, cc_id, start, end) / 1e6, 2)


def compute_cc_daily_month(perf_df, cc_id, y, m, ref_dt):
    """캘린더 한 달: 키 'y-m-d'(월 1~12) → 일별 지표.
    Appts(작성 일자)·Sales/Revenue(인센확정일자 QID)는 과거 일자에만 집계.
    Visits(일시)·Orders(동의서 작성일 TAD)는 미래 포함.
    담당자 행만 먼저 필터."""
    if not cc_id:
        return {}
    user_id_col = _col(perf_df, "User ID (18-digits)")
    if not user_id_col:
        return {}
    uid_s = str(cc_id).strip()
    sub = perf_df.loc[perf_df[user_id_col].astype(str).str.strip() == uid_s]
    if sub.empty:
        return {}
    _, last = monthrange(y, m)
    out = {}
    ref_d = ref_dt.date()
    for d in range(1, last + 1):
        day_dt = datetime(y, m, d).date()
        start = datetime(y, m, d)
        end = datetime(y, m, d, 23, 59, 59)
        future = day_dt > ref_d
        visits = compute_visits_ilsi_unique(sub, start, end, owner_cc_id=None)
        orders = compute_orders_on_tad(sub, start, end, owner_cc_id=None)
        if not future:
            appts = compute_appts_quote_created(sub, start, end, owner_cc_id=None)
            sm = compute_sales_metrics(sub, start, end, owner_cc_id=None)
            sales = int(sm["sales"])
            rev = round(float(sm["revenue"]) / 1e6, 2)
            bin_rt = round(float(sm["devices"]) / sales, 2) if sales else 0.0
        else:
            appts = 0
            sales = 0
            rev = 0.0
            bin_rt = 0.0
        key = f"{y}-{m}-{d}"
        out[key] = {
            "appts": int(appts),
            "visits": int(visits),
            "orders": int(orders),
            "sales": sales,
            "rev": rev,
            "bin": bin_rt,
            "future": future,
        }
    return out


def compute_cc_daily_month_fast(sub, y, m, ref_dt):
    """캘린더 한 달 일별 집계 — 담당자 필터된 sub를 받아 벡터화.
    날짜 파싱·마스크 계산을 한 번만 수행하고 일별 루프는 boolean index만 사용."""
    _, last = monthrange(y, m)
    ref_d    = ref_dt.date()
    empty_d  = {"appts": 0, "visits": 0, "orders": 0, "sales": 0, "rev": 0.0, "bin": 0.0}
    if sub is None or sub.empty:
        return {
            f"{y}-{m}-{d}": {**empty_d, "future": datetime(y, m, d).date() > ref_d}
            for d in range(1, last + 1)
        }

    # ── 컬럼 resolve (한 번) ──────────────────────────────────────────────
    ilsi_col   = _col(sub, "일시")
    sich_col   = _col(sub, "시착일")
    tad_col    = _col(sub, "TAD", "동의서 작성일")   # Orders 기준
    qid_col    = _col(sub, "QID", "인센확정일자")    # Sales/Revenue 기준
    appt_col   = _col(sub, "작성 일자", "작성일", "작성일자",
                       "예약 및 견적: 만든 날짜", "예약 및 견적 만든 날짜", "예약 및 견적:만든 날짜")
    close_col  = _col(sub, "종료")   # QID fallback용
    id_col     = _col(sub, "기회 ID (18)", "기회 ID")
    status_col = _col(sub, "상태")
    est_col    = _col(sub, "견적서 이름")   # Visits 기준
    prog_col   = _col(sub, "진행 상황")
    amt_col    = _col_amount(sub)

    # ── 날짜 시리즈 파싱 (한 번) ─────────────────────────────────────────
    ilsi_d  = _to_date_series(sub[ilsi_col]).dt.date  if ilsi_col  else None
    sich_d  = _to_date_series(sub[sich_col]).dt.date  if sich_col  else None
    tad_d   = _to_date_series(sub[tad_col]).dt.date   if tad_col   else None
    appt_d  = _to_date_series(sub[appt_col]).dt.date  if appt_col  else None
    # Sales/Rev: QID 우선, fallback 종료
    _sale_date_col = qid_col or close_col
    sale_d  = _to_date_series(sub[_sale_date_col]).dt.date if _sale_date_col else None

    # ── 재사용 마스크 (한 번) ────────────────────────────────────────────
    _st_ser     = sub[status_col].astype(str).str.strip() if status_col else pd.Series("", index=sub.index)
    # Visits: 일시 기간 내 + 견적서 이름에 '최초' 포함 + 상태='Visited'
    est_ok_sub = (
        sub[est_col].astype(str).str.contains("최초", na=False)
        if (est_col and est_col in sub.columns)
        else pd.Series(False, index=sub.index)
    )
    visited_ok = est_ok_sub & _st_ser.eq("Visited")
    # VR 분모: Visits(최초+Visited) + No Show + Canceled (일시 기준)
    vr_denom_ok = visited_ok | _st_ser.isin(["No Show", "Canceled"])
    # Orders: 동의서 작성일(TAD) 기준 + 진행상황 not empty and != '주문전취소'
    order_qual  = _mask_order_qualifying(sub, prog_col)
    succ_mask   = _mask_success_sales_vec(sub)

    # ── 값 시리즈 (한 번) ────────────────────────────────────────────────
    norm_ids   = sub[id_col].map(_normalize_opp_id_scalar) if id_col else None
    amounts    = sub[amt_col].apply(_safe_float) if amt_col else pd.Series(0.0, index=sub.index)
    incent_ser = _series_incent_units_vec(sub)

    out = {}
    for d in range(1, last + 1):
        day_dt = datetime(y, m, d).date()
        future = day_dt > ref_d

        visits = 0
        vr_denom = 0
        if ilsi_d is not None and norm_ids is not None:
            m_visited = (ilsi_d == day_dt) & visited_ok
            m_vd      = (ilsi_d == day_dt) & vr_denom_ok
            if m_visited.any():
                visits   = int(norm_ids[m_visited].dropna().nunique())
            if m_vd.any():
                vr_denom = int(norm_ids[m_vd].dropna().nunique())

        orders = 0
        # Orders: 동의서 작성일(TAD) 기준, TAD 없으면 시착일 fallback
        _ord_d = tad_d if tad_d is not None else sich_d
        if _ord_d is not None and norm_ids is not None:
            m_o = (_ord_d == day_dt) & order_qual
            if m_o.any():
                orders = int(norm_ids[m_o].dropna().nunique())

        appts = 0
        sales = 0
        rev   = 0.0
        bin_rt = 0.0
        if not future:
            if appt_d is not None and norm_ids is not None:
                m_a = appt_d == day_dt
                if m_a.any():
                    appts = int(norm_ids[m_a].dropna().nunique())
            # Sales/Rev: 인센확정일자(QID) 기준, 없으면 종료 fallback
            if sale_d is not None:
                m_s = (sale_d == day_dt) & succ_mask
                if m_s.any():
                    sales  = (int(norm_ids[m_s].dropna().nunique())
                              if norm_ids is not None else int(m_s.sum()))
                    rev    = round(float(amounts[m_s].sum()) / 1e6, 2)
                    dev_d  = int(incent_ser[m_s].sum())
                    bin_rt = round(dev_d / sales, 2) if sales else 0.0

        out[f"{y}-{m}-{d}"] = {
            "appts": appts, "visits": visits, "vr_denom": vr_denom, "orders": orders,
            "sales": sales, "rev": rev, "bin": bin_rt, "future": future,
        }
    return out


def _pip_monitor_month_span(ref_dt):
    """PIP 캘린더: 이전 분기(선정 기준) + 현재 분기(모니터링) = 6개월.
    이전 분기 성과를 캘린더에서 같이 볼 수 있도록 포함."""
    cur_q, prev_q, _ = pip_quarters_from_ref_dt(ref_dt)
    return prev_q + cur_q


def _pip_default_targets():
    """PIP 상세 타겟 기본: CR2/VR/OR/SR·ABV·Revenue 고정, Sales만 입력·Orders·Visits은 역산."""
    cr2, vr, or_, sr = 45.0, 55.0, 60.0, 50.0
    cr3 = round((vr * or_ * sr) / 10000.0, 2)
    sales = 35.0
    orders = round(sales * 100.0 / sr, 2) if sr > 0 else 0.0
    visits = round(orders * 100.0 / or_, 2) if or_ > 0 else 0.0
    return {
        "cr2": cr2,
        "vr": vr,
        "or_": or_,
        "sr": sr,
        "cr3": cr3,
        "visits": visits,
        "orders": orders,
        "sales": sales,
        "abv_m": 2.5,
        "rev_m": 70.0,
    }


def compute_monthly_pip_metrics(perf_df, cc_id, triple, leads_df=None, cc_name=None, ref_dt=None):
    """L3M + 현재월 각 달별 지표 반환 (퍼널 월별 세부 진단용).
    열 순서: Leads, Appts, CR2, Visits, VR, Orders, OR, Closed_Orders, Sales, SR, ABV, Revenue"""
    if not cc_id:
        return []
    now = ref_dt or datetime.now()
    # triple 에 현재월 추가 (중복 제외)
    cur_ym = (now.year, now.month)
    months_to_compute = list(triple)
    if cur_ym not in months_to_compute:
        months_to_compute.append(cur_ym)

    months = []
    for (yy, mm) in months_to_compute:
        start = datetime(yy, mm, 1)
        _, last = monthrange(yy, mm)
        # 현재월은 오늘까지만
        if (yy, mm) == cur_ym:
            end = datetime(now.year, now.month, now.day, 23, 59, 59)
        else:
            end = datetime(yy, mm, last, 23, 59, 59)
        # Leads
        leads = 0
        if leads_df is not None and cc_name:
            lm = compute_lead_metrics(leads_df, start, end, owner_name=cc_name)
            leads   = lm["lead"]
            qual    = lm["qualified_count"]
        else:
            qual = 0
        oppts  = compute_appts_quote_created(perf_df, start, end, owner_cc_id=cc_id)
        cr2_   = round(qual / leads * 100, 1) if leads else 0.0
        vr_m_pip = compute_visits_vr(perf_df, start, end, owner_cc_id=cc_id)
        visits, vr_denom_pip = vr_m_pip["visited"], vr_m_pip["vr_denom"]
        _, trials, _order_ids = compute_visits_trials(perf_df, start, end, owner_cc_id=cc_id)
        orders = compute_orders_on_tad(perf_df, start, end, owner_cc_id=cc_id)
        closed_orders = compute_closed_orders(perf_df, start, end, owner_cc_id=cc_id)
        sm = compute_sales_metrics(perf_df, start, end, owner_cc_id=cc_id)
        sales   = sm["sales"]
        revenue = sm["revenue"]
        vr_  = round(visits / vr_denom_pip * 100, 1) if vr_denom_pip else 0.0
        or__ = round(orders / visits * 100, 1) if visits else 0.0
        sr_  = round(sales / closed_orders * 100, 1) if closed_orders else 0.0
        abv_ = round(revenue / sales / 1e6, 2) if sales else 0.0
        is_cur = (yy, mm) == cur_ym
        months.append({
            "label": f"{mm}월" + (" (진행중)" if is_cur else ""),
            "year": yy, "month": mm,
            "is_current": is_cur,
            "leads": leads, "oppts": oppts, "cr2": cr2_,
            "visits": visits, "vr": vr_,
            "orders": orders, "or_": or__,
            "closed_orders": closed_orders, "trials": trials,
            "sales": sales, "sr": sr_,
            "abv_m": abv_, "revenue_m": round(revenue / 1e6, 2),
        })
    return months


def compute_bench_monthly_metrics(perf_df, non_pip_cc_ids, triple, leads_df=None, non_pip_names=None, ref_dt=None):
    """non-PIP 인원들의 월별 평균 지표 (퍼널 벤치마크 월별 비교용)."""
    if not non_pip_cc_ids or perf_df is None:
        return []
    user_id_col = _col(perf_df, "User ID (18-digits)")
    if not user_id_col:
        return []
    n = len(non_pip_cc_ids)
    if n == 0:
        return []
    non_pip_ids_str = set(str(cid).strip() for cid in non_pip_cc_ids if cid)
    mask = perf_df[user_id_col].astype(str).str.strip().isin(non_pip_ids_str)
    non_pip_df = perf_df[mask]
    now = ref_dt or datetime.now()
    months = []
    for (yy, mm) in triple:
        start = datetime(yy, mm, 1)
        _, last = monthrange(yy, mm)
        # 현재월은 오늘까지만
        if (yy, mm) == (now.year, now.month):
            end = datetime(now.year, now.month, now.day, 23, 59, 59)
        else:
            end = datetime(yy, mm, last, 23, 59, 59)
        oppts         = compute_appts_quote_created(non_pip_df, start, end)
        vr_m          = compute_visits_vr(non_pip_df, start, end)
        visits, vr_denom = vr_m["visited"], vr_m["vr_denom"]
        orders        = compute_orders_on_tad(non_pip_df, start, end)
        closed_orders = compute_closed_orders(non_pip_df, start, end)
        sm            = compute_sales_metrics(non_pip_df, start, end)
        sales         = sm["sales"]
        revenue       = sm["revenue"]
        # Leads: non-PIP 이름 목록 기반으로 합산 후 평균
        leads_total = 0
        if leads_df is not None and non_pip_names:
            for nm in non_pip_names:
                lm = compute_lead_metrics(leads_df, start, end, owner_name=nm)
                leads_total += lm["lead"]
        vr_   = round(visits / vr_denom * 100, 1) if vr_denom else 0.0
        or__  = round(orders / visits * 100, 1)  if visits  else 0.0
        sr_   = round(sales / closed_orders * 100, 1) if closed_orders else 0.0
        abv_  = round(revenue / sales / 1e6, 2) if sales else 0.0
        is_cur = (yy, mm) == (now.year, now.month)
        months.append({
            "label":         f"{mm}월" + (" (진행중)" if is_cur else ""),
            "is_current":    is_cur,
            "leads":         round(leads_total / n, 1),
            "oppts":         round(oppts / n, 1),
            "visits":        round(visits / n, 1),
            "orders":        round(orders / n, 1),
            "closed_orders": round(closed_orders / n, 1),
            "sales":         round(sales / n, 1),
            "revenue_m":     round(revenue / n / 1e6, 2),
            "vr":  vr_,
            "or_": or__,
            "sr":  sr_,
            "abv_m": abv_,
        })
    return months


def compute_ttfa_mt(perf_df, cc_id, triple):
    """TTFA (작성일자→방문일) 및 MT (시착일→종료일) 평균 소요일 계산."""
    user_id_col = _col(perf_df, "User ID (18-digits)")
    ilsi_col    = _col(perf_df, "일시")
    appt_col    = _col(perf_df, "작성 일자", "작성일", "작성일자",
                       "예약 및 견적: 만든 날짜", "예약 및 견적 만든 날짜", "예약 및 견적:만든 날짜")
    sich_col    = _col(perf_df, "시착일")
    close_col   = _col(perf_df, "종료")
    if not cc_id or not user_id_col:
        return {"ttfa_days": None, "mt_days": None, "appt_to_close_days": None}
    uid_s = str(cc_id).strip()
    sub = perf_df.loc[perf_df[user_id_col].astype(str).str.strip() == uid_s]
    if sub.empty:
        return {"ttfa_days": None, "mt_days": None, "appt_to_close_days": None}
    # 전체 triple 기간
    start_all = datetime(triple[0][0], triple[0][1], 1)
    _, last_all = monthrange(triple[-1][0], triple[-1][1])
    end_all = datetime(triple[-1][0], triple[-1][1], last_all, 23, 59, 59)
    ttfa_days = None
    mt_days   = None
    appt_to_close_days = None
    if ilsi_col and appt_col:
        ilsi_dt = _to_date_series(sub[ilsi_col])
        appt_dt = _to_date_series(sub[appt_col])
        period_mask = in_period(ilsi_dt, start_all, end_all)
        both_ok = period_mask & ilsi_dt.notna() & appt_dt.notna()
        if both_ok.any():
            diff = (ilsi_dt[both_ok] - appt_dt[both_ok]).dt.days
            valid = diff[(diff >= 0) & (diff <= 180)]
            if len(valid) > 0:
                ttfa_days = round(float(valid.sum()) / len(valid), 1)
    if sich_col and close_col:
        close_dt = _to_date_series(sub[close_col])
        sich_dt  = pd.to_datetime(sub[sich_col], errors="coerce")
        period_mask2 = in_period(close_dt, start_all, end_all)
        both_ok2 = period_mask2 & close_dt.notna() & sich_dt.notna()
        if both_ok2.any():
            diff2 = (close_dt[both_ok2] - sich_dt[both_ok2]).dt.days
            valid2 = diff2[(diff2 >= 0) & (diff2 <= 365)]
            if len(valid2) > 0:
                mt_days = round(float(valid2.sum()) / len(valid2), 1)
    # 작성일자 → 종료 평균 소요일 (성공 매출 기준)
    if appt_col and close_col:
        close_dt2 = _to_date_series(sub[close_col])
        appt_dt2  = _to_date_series(sub[appt_col])
        succ_mask = _mask_success_sales_vec(sub)
        period_mask3 = in_period(close_dt2, start_all, end_all)
        both_ok3 = period_mask3 & succ_mask & close_dt2.notna() & appt_dt2.notna()
        if both_ok3.any():
            diff3 = (close_dt2[both_ok3] - appt_dt2[both_ok3]).dt.days
            valid3 = diff3[(diff3 >= 7) & (diff3 <= 365)]
            if len(valid3) > 0:
                appt_to_close_days = round(float(valid3.sum()) / len(valid3), 1)
    return {"ttfa_days": ttfa_days, "mt_days": mt_days, "appt_to_close_days": appt_to_close_days}


def build_pip_calendar_months(perf_df, cc_id, ref_dt, call_df=None, cc_name=None):
    """캘린더용: 현재 분기 3개월 일별 집계.
    담당자 필터를 루프 밖에서 한 번만 수행 후 compute_cc_daily_month_fast 위임.
    call_df가 제공되면 일별 콜타임(call_time_sec, call_time_display)도 포함."""
    if not cc_id:
        return {}
    user_id_col = _col(perf_df, "User ID (18-digits)")
    if not user_id_col:
        return {}
    uid_s = str(cc_id).strip()
    sub = perf_df.loc[perf_df[user_id_col].astype(str).str.strip() == uid_s]

    # 일별 콜타임 사전 구축
    daily_call_sec = {}
    if call_df is not None and not call_df.empty and cc_name:
        date_col  = _col(call_df, "통화 시작 시간t", "통화 시작 시간")
        owner_col = _col(call_df, "담당자")
        talk_col  = _col(call_df, "통화시간(초)", "통화 시간(초)", "통화시간(초)(초)")
        if date_col and talk_col:
            csub = call_df
            if owner_col:
                csub = csub[csub[owner_col].astype(str).str.strip() == cc_name.strip()]
            if not csub.empty:
                call_dates = _to_date_series(csub[date_col]).dt.date
                talk_secs  = pd.to_numeric(csub[talk_col], errors="coerce").fillna(0)
                for cd, cs in zip(call_dates, talk_secs):
                    if pd.notna(cd):
                        daily_call_sec[cd] = daily_call_sec.get(cd, 0.0) + float(cs)

    out = {}
    for y, m in _pip_monitor_month_span(ref_dt):
        mk = f"{y}-{m}"
        month_data = compute_cc_daily_month_fast(sub, y, m, ref_dt)
        for day_key, day_val in month_data.items():
            parts = day_key.split("-")
            if len(parts) == 3:
                try:
                    day_date = datetime(int(parts[0]), int(parts[1]), int(parts[2])).date()
                    sec = daily_call_sec.get(day_date, 0.0)
                    day_val["call_time_sec"] = round(sec, 1)
                    day_val["call_time_display"] = format_duration(sec) if sec > 0 else ""
                except Exception:
                    day_val["call_time_sec"] = 0.0
                    day_val["call_time_display"] = ""
        out[mk] = month_data
    return out


def compute_pip(cc_df, perf_df, leads_df, ref_dt, name_to_team, name_to_cc_id, by_owner, owners, supply_price_df=None, call_df=None):
    """PIP: cc 시트 pip 컬럼 기반 선정.
    O = PIP 대상자(강제 지정), X = 제외, 나머지 hire 6개월+ 인원 중 수익률 하위 10% = 접근 위험.
    두진경 팀은 항상 제외. 벤치마크는 PIP+위험 제외 인원 평균."""
    hire_col  = _col(cc_df, "hire_date", "hire date", "고용일", "입사일", "입사")
    cc_col    = _col(cc_df, "cc")
    pip_col   = _col(cc_df, "pip", "PIP")
    hire_by_name     = {}
    pip_flag_by_name = {}   # 'O', 'X', or ''
    if cc_col:
        for _, r in cc_df.iterrows():
            n = _safe_str(r.get(cc_col))
            if not n:
                continue
            if hire_col:
                hire_by_name[n] = r.get(hire_col)
            if pip_col:
                pip_flag_by_name[n] = _safe_str(r.get(pip_col, "")).strip().upper()

    pip_forced = []  # O 플래그
    best_names = []  # T 플래그 (Best 상담사)
    pool       = []  # 위험후보 선정 대상 (non-O, non-X, non-T, hire 6m+)
    for name in owners:
        if _is_excluded_pip_team(name_to_team.get(name, "")):
            continue
        flag = pip_flag_by_name.get(name, "")
        if flag == "X":
            continue
        days = _hire_tenure_days(ref_dt, hire_by_name.get(name))
        if days is None or days < 183:
            continue
        if flag == "O":
            pip_forced.append(name)
        elif flag == "T":
            best_names.append(name)
        else:
            pool.append(name)
    triple, month_labels_common = _pip_reference_triple_and_labels(ref_dt)
    cur_q, _, pip_start_date_global = pip_quarters_from_ref_dt(ref_dt)
    pip_cfg_static = {
        "reference_label": _pip_reference_range_label(triple),
        "reference_months": [{"y": y, "m": m} for y, m in triple],
        "monitoring_label": f"{cur_q[0][1]}~{cur_q[-1][1]}월 모니터링 ({cur_q[0][0]})",
        "pip_pct": int(PIP_PIP_FRACTION * 100),
        "risk_pct": int(PIP_RISK_FRACTION * 100),
    }
    all_eligible = pip_forced + pool   # 마진 계산 대상 전체
    if not all_eligible:
        empty_chart = {
            "labels": [],
            "bar_pct": [],
            "goal_line": 100,
            "month_labels": month_labels_common,
            "eligible_n": 0,
        }
        return [], empty_chart, [], [], {"benchmarks": {}, "month_labels": month_labels_common, "pip_config": pip_cfg_static}

    sp_dict = _build_supply_price_dict(supply_price_df)

    m3_by_name     = {}
    rev_sum_3m     = {}
    margin_by_name = {}
    for name in all_eligible:
        cid = name_to_cc_id.get(name, "")
        m3_by_name[name] = cc_three_month_revenue(perf_df, cid, triple)
        rev_sum_3m[name] = float(sum(m3_by_name[name]))
        mg = compute_margin(perf_df, leads_df, name, cid, triple, sp_dict)
        margin_by_name[name] = mg

    # 접근위험: pool(non-O, non-X) 중 수익률 하위 PIP_PIP_FRACTION(10%)
    n = len(pool)
    pool_sorted = sorted(pool, key=lambda x: margin_by_name[x]["margin_monthly"])
    risk_n    = max(1, int(np.ceil(n * PIP_PIP_FRACTION))) if n > 0 else 0
    pip_names  = pip_forced                              # O 플래그 = PIP
    risk_names = pool_sorted[:risk_n]                    # 하위 10% = 접근위험
    non_pip_names = pool_sorted[risk_n:]                 # 나머지 = 벤치마크 대상

    def _mean_non_pip(key, is_float=True):
        vals = []
        for nm in non_pip_names:
            m = by_owner.get(nm, {}).get("L3M", {})
            v = m.get(key)
            if v is None:
                continue
            vals.append(float(v))
        if not vals:
            return 0.0
        return sum(vals) / len(vals)

    bench_m = [0.0, 0.0, 0.0]
    for i in range(3):
        vs = [m3_by_name[nm][i] for nm in non_pip_names]
        bench_m[i] = sum(vs) / len(vs) if vs else 0.0
    bench_rev_month_avg = sum(rev_sum_3m[nm] for nm in non_pip_names) / (3.0 * len(non_pip_names)) if non_pip_names else 0.0

    benchmarks = {
        "rev_month_avg": round(bench_rev_month_avg, 1),
        "rev_m1": round(bench_m[0], 1),
        "rev_m2": round(bench_m[1], 1),
        "rev_m3": round(bench_m[2], 1),
        "cr2": round(_mean_non_pip("cr2"), 4),
        "cr3": round(_mean_non_pip("cr3"), 4),
        "vr": round(_mean_non_pip("vr"), 4),
        "or_": round(_mean_non_pip("or_"), 4),
        "sr": round(_mean_non_pip("sr"), 4),
        "asp": round(_mean_non_pip("asp"), 2),
        "abv": round(_mean_non_pip("abv"), 2),
        "trials_avg": round(_mean_non_pip("trials"), 4),
        "call_count_avg": round(_mean_non_pip("call_count"), 4),
        # L3M 합계 → JS에서 /3 하여 월평균으로 사용
        "oppts_l3m": round(_mean_non_pip("oppts"), 1),
        "visits_l3m": round(_mean_non_pip("visits"), 1),
        "orders_l3m": round(_mean_non_pip("orders"), 1),
        "closed_orders_l3m": round(_mean_non_pip("closed_orders"), 1),
        "sales_l3m": round(_mean_non_pip("sales"), 1),
        "trials_l3m": round(_mean_non_pip("trials"), 1),
    }
    # 월별 비교용 벤치마크 (non-PIP 인원 월별 평균) – 현재월 포함
    non_pip_cc_ids = [name_to_cc_id.get(nm) for nm in non_pip_names if name_to_cc_id.get(nm)]
    bench_triple = list(triple)
    cur_ym = (ref_dt.year, ref_dt.month)
    if cur_ym not in bench_triple:
        bench_triple.append(cur_ym)
    benchmarks["monthly_breakdown"] = compute_bench_monthly_metrics(
        perf_df, non_pip_cc_ids, bench_triple,
        leads_df=leads_df, non_pip_names=non_pip_names, ref_dt=ref_dt)

    g = build_fixed_goals("L3M")
    mg = g["revenue"] / 3.0

    def _assign_status(idx, n_total):
        if n_total <= 1:
            return "긴급"
        if idx < max(1, n_total // 3):
            return "긴급"
        if idx < max(2, (2 * n_total) // 3):
            return "위험"
        return "관찰"

    def _build_row(cc, idx, group_n, group_list, kind="pip"):
        team_val = name_to_team.get(cc, "")
        cc_id = name_to_cc_id.get(cc, "")
        m3 = m3_by_name[cc]
        mlabs = month_labels_common
        m_l3m = by_owner.get(cc, {}).get("L3M", {})
        hire_raw = hire_by_name.get(cc)
        hp = pd.to_datetime(hire_raw, errors="coerce")
        hire_yy_mm = ""
        if not pd.isna(hp):
            hire_yy_mm = hp.strftime("%y.%m")
        rev_cc_sum = round(rev_sum_3m[cc], 0)
        rev_month_avg = sum(m3) / 3.0
        status = _assign_status(group_list.index(cc), group_n)
        asp_v = m_l3m.get("asp") or 0
        abv_v = m_l3m.get("abv") or 0
        call_cnt = m_l3m.get("call_count") or 0
        call_t = m_l3m.get("avg_call_time_display") or "—"
        oppts = float(m_l3m.get("oppts") or 0)
        cr3_v = float(m_l3m.get("cr3") or 0)
        sr_v = float(m_l3m.get("sr") or 0)
        or_v = float(m_l3m.get("or_") or 0)
        visits = int(m_l3m.get("visits") or 0)
        orders = int(m_l3m.get("orders") or 0)
        closed_orders = int(m_l3m.get("closed_orders") or 0)
        sales = int(m_l3m.get("sales") or 0)
        lead = int(m_l3m.get("lead") or 0)
        qual = int(m_l3m.get("qualified_count") or 0)
        trials = int(m_l3m.get("trials") or 0)
        pipe_rows = pipeline_trial_rows_for_cc(perf_df, cc_id)
        pl_n = len(pipe_rows)
        monthly_fcst = (oppts / 3.0) * (cr3_v / 100.0) * abv_v if oppts and abv_v else 0.0
        fcst_default = [round(monthly_fcst, 0)] * 3
        pipe_line_rev = pl_n * (sr_v / 100.0) * (or_v / 100.0) * abv_v if pl_n and abv_v else 0.0
        if pipe_line_rev > 0 and monthly_fcst > 0:
            blend = 0.5 * monthly_fcst + 0.5 * (pipe_line_rev / max(pl_n, 1))
            fcst_default = [round(blend, 0)] * 3
        elif pipe_line_rev > 0:
            fcst_default = [round(pipe_line_rev / max(pl_n, 1), 0)] * 3

        def _pct(actual, goal):
            if not goal:
                return 0.0
            return min(150.0, round(float(actual) / float(goal) * 100, 1))

        goal_qual = lead * (g["cr2"] / 100.0) if lead else 0.0
        goal_visits = oppts * (g["vr"] / 100.0) if oppts else 0.0
        goal_orders = visits * (g["or_"] / 100.0) if visits else 0.0
        goal_sales = closed_orders * (g["sr"] / 100.0) if closed_orders else 0.0

        achievement_rows = [
            {"key": "sum3", "label": "최근3개월 매출 합", "actual": float(sum(m3)), "goal": float(g["revenue"]), "pct": _pct(sum(m3), g["revenue"])},
            {"key": "n1m", "label": "N1M 매출(직전달)", "actual": float(m3[2]), "goal": float(mg), "pct": _pct(m3[2], mg)},
            {"key": "n2m", "label": "N2M 매출", "actual": float(m3[1]), "goal": float(mg), "pct": _pct(m3[1], mg)},
            {"key": "n3m", "label": "N3M 매출", "actual": float(m3[0]), "goal": float(mg), "pct": _pct(m3[0], mg)},
            {"key": "qual", "label": "기회 전환수", "actual": float(qual), "goal": float(goal_qual), "pct": _pct(qual, goal_qual)},
            {"key": "oppts", "label": "Appts", "actual": float(oppts), "goal": float(max(oppts, 1)), "pct": _pct(oppts, max(oppts, 1))},
            {"key": "visits", "label": "Visits", "actual": float(visits), "goal": float(goal_visits), "pct": _pct(visits, goal_visits)},
            {"key": "orders", "label": "Orders", "actual": float(orders), "goal": float(goal_orders), "pct": _pct(orders, goal_orders)},
            {"key": "sales", "label": "Sales", "actual": float(sales), "goal": float(max(goal_sales, 1)), "pct": _pct(sales, max(goal_sales, 1))},
        ]

        partial_rev_m = cc_month_partial_revenue_m(perf_df, cc_id, ref_dt) if cc_id else 0.0
        cal_months = build_pip_calendar_months(perf_df, cc_id, ref_dt, call_df=call_df, cc_name=cc) if cc_id else {}
        monthly_goal_won = g["revenue"] / 3.0
        rev_target_m = round(monthly_goal_won / 1e6, 2)
        abv_m_tgt = round(abv_v / 1e6, 2) if abv_v else 13.2
        fcst_m = [round(x / 1e6, 2) for x in fcst_default]
        past_bars_m = [round(x / 1e6, 2) for x in m3]
        chart_labels = list(mlabs) + [f"{ref_dt.month}월(부분)"] + ["+1개월", "+2개월", "+3개월"]
        exp_rev_m = round(pipe_line_rev / 1e6, 1) if pipe_line_rev else 0.0
        gap_75_m = round(max(0.0, 75.0 - exp_rev_m), 1)
        visits_m_avg = round(visits / 3.0, 2) if visits else 0.0
        orders_m_avg = round(orders / 3.0, 2) if orders else 0.0
        sales_m_avg = round(sales / 3.0, 2) if sales else 0.0
        revenue_m_avg = round(sum(m3) / 3.0 / 1e6, 2) if m3 else 0.0
        goal_visits_m = round(goal_visits / 3.0, 2) if goal_visits else 0.0
        goal_orders_m = round(goal_orders / 3.0, 2) if goal_orders else 0.0
        goal_sales_m = round(goal_sales / 3.0, 2) if goal_sales else 0.0
        goal_appts_m = max(1, int(round(oppts / 3.0))) if oppts else 1
        chart_note = (
            "Pipeline 예측(연한 파랑): 참조 3개월 직후 N3M에 대해, "
            "① L3M 월 환산 매출(월 Appts×CR3×ABV)과 ② 현재 Trials 파이프라인 기반 기대 매출을 혼합한 월별 값을 "
            "동일하게 3개월에 반영한 뒤 백만원 단위로 표시합니다. "
            "시뮬레이션 예측(보라): 입력한 시뮬 Trials·SR·VR·OR·ABV로 월 매출(≈Sales×ABV)을 구한 뒤 "
            "가중치(0.95/1.0/1.08)로 월별 막대를 나눠 그립니다."
        )

        monthly_breakdown = compute_monthly_pip_metrics(perf_df, cc_id, triple, leads_df=leads_df, cc_name=cc, ref_dt=ref_dt)
        ttfa_mt = compute_ttfa_mt(perf_df, cc_id, triple)

        pip_t = _pip_default_targets()

        pip_ui = {
            "ref_year": ref_dt.year,
            "ref_month_1": ref_dt.month,
            "ref_day": ref_dt.day,
            "pip_start_year": pip_start_date_global.year,
            "pip_start_month_1": pip_start_date_global.month,
            "pip_start_mdom": 1,
            "l3m_rev_month_avg_m": round(rev_month_avg / 1e6, 1) if rev_month_avg else 0.0,
            "l3m_rev_sum_m": round(sum(m3) / 1e6, 1),
            "n3m_fcst_m": fcst_m,
            "partial_month_rev_m": partial_rev_m,
            "target_1st_m": 75.0,
            "target_norm_m": 100.0,
            "call_goal": int(g.get("call_score") or 80),
            "call_score": int(call_cnt) if call_cnt else None,
            "calendar_months": cal_months,
            "calendar_monitor_months": [f"{y}-{m}" for y, m in _pip_monitor_month_span(ref_dt)],
            "calendar_pip_months": [f"{y}-{m}" for y, m in cur_q],
            "chart_note": chart_note,
            "monthly_goals": {
                "m_appts": goal_appts_m,
                "m_visits": max(1, int(round(pip_t["visits"]))),
                "m_orders": max(1, int(round(pip_t["orders"]))),
                "m_sales": max(1, int(round(pip_t["sales"]))),
                "m_rev_m": round(pip_t["rev_m"], 1),
                "bin": round(g.get("bin_rate") or 1.8, 2),
                "pipeline_trials": pl_n,
            },
            "actual": {
                "cr2": round(m_l3m.get("cr2") or 0, 2),
                "vr": round(m_l3m.get("vr") or 0, 2),
                "or_": round(m_l3m.get("or_") or 0, 2),
                "sr": round(m_l3m.get("sr") or 0, 2),
                "cr3": round(cr3_v, 2),
                "visits": visits_m_avg,
                "orders": orders_m_avg,
                "sales": sales_m_avg,
                "abv_m": round(abv_v / 1e6, 2) if abv_v else 0.0,
                "revenue_m": revenue_m_avg,
                "monthly_breakdown": monthly_breakdown,
                "ttfa_days": ttfa_mt["ttfa_days"],
                "mt_days": ttfa_mt["mt_days"],
                "appt_to_close_days": ttfa_mt["appt_to_close_days"],
            },
            "targets_default": dict(pip_t),
            "chart": {
                "labels": chart_labels,
                "past_m": past_bars_m,
                "partial_m": partial_rev_m,
                "pipeline_fcst_m": fcst_m,
                "goal_line_1_m": 75.0,
                "goal_line_2_m": 100.0,
                "note": chart_note,
            },
            "pipeline_summary": {
                "count": pl_n,
                "weighted_sr": round(sr_v, 1),
                "expected_rev_m": exp_rev_m,
                "gap_to_75m_m": gap_75_m,
                "expected_sales_hint": max(0, int(round(pl_n * (sr_v / 100.0)))) if pl_n else 0,
            },
        }

        detail = {
            "pip_week": 3,
            "remain_weeks": 9,
            "target_1st": 75e6,
            "target_norm": 100e6,
            "month_labels": mlabs,
            "month_labels_fcst": ["+1개월", "+2개월", "+3개월"],
            "month_actual": m3,
            "month_fcst_default": fcst_default,
            "pipeline_trials": pl_n,
            "pip_ui": pip_ui,
            "sim": {
                "pipeline_trials": pl_n,
                "l3m_oppts": round(oppts, 2),
                "cr3": round(cr3_v, 2),
                "abv": round(abv_v, 2),
                "sr": round(sr_v, 2),
                "or_": round(or_v, 2),
                "monthly_formula": round(monthly_fcst, 0),
                "fcst_default": fcst_default,
            },
            "achievement_rows": achievement_rows,
            "pipeline_rows": pipe_rows,
            "upcoming_visit_rows": upcoming_visit_rows_for_cc(perf_df, cc_id, ref_dt),
            "weekly": [
                {
                    "w": i + 1,
                    "target": round(rev_cc_sum / 12 + (i - 1.5) * 2e6, 0),
                    "actual": round(rev_cc_sum / 12 + (i - 2) * 1.5e6, 0),
                }
                for i in range(4)
            ],
        }
        return {
            "cc": cc,
            "team_display": team_display_name(team_val),
            "l3m_revenue": rev_cc_sum,
            "l3m_rev_month_avg": round(rev_month_avg, 1),
            "hire_yy_mm": hire_yy_mm,
            "l3m_m1": m3[0],
            "l3m_m2": m3[1],
            "l3m_m3": m3[2],
            "cr2": round(m_l3m.get("cr2") or 0, 2),
            "cr3": round(m_l3m.get("cr3") or 0, 2),
            "vr": round(m_l3m.get("vr") or 0, 2),
            "or_": round(m_l3m.get("or_") or 0, 2),
            "sr": round(m_l3m.get("sr") or 0, 2),
            "asp": round(asp_v, 0) if asp_v else 0,
            "abv": round(abv_v, 0) if abv_v else 0,
            "call_count": int(call_cnt),
            "call_time": call_t,
            "call_monitor": int(call_cnt),
            "trials": trials,
            "status": status,
            "detail": detail,
            "pip_kind": kind,
            "l3m_margin":      margin_by_name[cc]["margin_monthly"],
            "l3m_margin_rate": margin_by_name[cc]["margin_rate"],
            "l3m_revenue_monthly": margin_by_name[cc]["revenue_monthly"],
        }

    result = [_build_row(cc, i, len(pip_names), pip_names, "pip") for i, cc in enumerate(pip_names)]
    pip_at_risk = [_build_row(cc, i, len(risk_names), risk_names, "risk") for i, cc in enumerate(risk_names)]
    # Best 상담사: T 플래그 (margin 계산 필요 시 all_eligible에 포함 여부 확인)
    # best_names의 margin 데이터가 없을 수 있으므로 별도 계산
    for nm in best_names:
        if nm not in margin_by_name:
            cid = name_to_cc_id.get(nm, "")
            m3_by_name[nm] = cc_three_month_revenue(perf_df, cid, triple)
            rev_sum_3m[nm] = float(sum(m3_by_name[nm]))
            margin_by_name[nm] = compute_margin(perf_df, leads_df, nm, cid, triple, sp_dict)
    pip_best = [_build_row(cc, i, len(best_names), best_names, "best") for i, cc in enumerate(best_names)]

    trend_lines = [{"cc": p["cc"], "data": [p["l3m_m1"], p["l3m_m2"], p["l3m_m3"]]} for p in result]

    if not result:
        empty_chart = {"labels": [], "bar_pct": [], "goal_line": 100, "month_labels": month_labels_common or [], "eligible_n": n}
        return [], empty_chart, [], pip_at_risk, {"benchmarks": benchmarks, "month_labels": month_labels_common, "pip_config": pip_cfg_static}, pip_best

    def pct(actual, goal):
        if not goal:
            return 0.0
        return min(150.0, round(float(actual) / float(goal) * 100, 1))

    nn = len(result)
    sum_cr2 = sum(p["cr2"] for p in result)
    sum_cr3 = sum(p["cr3"] for p in result)
    sum_vr = sum(p["vr"] for p in result)
    sum_or = sum(p["or_"] for p in result)
    sum_sr = sum(p["sr"] for p in result)
    avg_rev_m = sum(p["l3m_rev_month_avg"] for p in result) / nn
    chart = {
        "labels": ["CR2", "CR3", "VR", "OR", "SR", "Revenue"],
        "bar_pct": [
            pct(sum_cr2 / nn, g["cr2"]),
            pct(sum_cr3 / nn, g["cr3"]),
            pct(sum_vr / nn, g["vr"]),
            pct(sum_or / nn, g["or_"]),
            pct(sum_sr / nn, g["sr"]),
            pct(avg_rev_m * 3.0, g["revenue"]),
        ],
        "goal_line": 100,
        "month_labels": month_labels_common or ["1월", "2월", "3월"],
        "eligible_n": len(all_eligible),
    }
    meta = {
        "benchmarks": benchmarks,
        "month_labels": month_labels_common,
        "pip_total_trials": int(sum(p.get("trials") or 0 for p in result)),
        "pip_avg_trials": round(sum(p.get("trials") or 0 for p in result) / nn, 2) if nn else 0.0,
        "pip_config": pip_cfg_static,
    }
    return result, chart, trend_lines, pip_at_risk, meta, pip_best


# ---------- 한 번에 기간별 집계 ----------
def run_metrics(cc_df, leads_df, perf_df, call_df, ref_dt, supply_price_df=None):
    name_to_team, name_to_cc_id, teams_with_display, owners = build_owner_mapping(cc_df)
    periods = ["daily", "weekly", "monthly", "L3M", "L6M"]
    out = {
        "periods": {},
        "owners": owners,
        "name_to_team": name_to_team,
        "name_to_cc_id": name_to_cc_id,
        "teams": teams_with_display,
        "by_owner": {},
        "by_team": {},
        "pip": [],
        "pip_summary": {},
        "pip_trend": [],
        "pip_at_risk": [],
        "pip_benchmarks": {},
        "pip_month_labels": [],
        "pip_kpi": {},
        "pip_config": {},
    }
    for pk in periods:
        start, end = period_range(ref_dt, pk)
        lead_m = compute_lead_metrics(leads_df, start, end)
        call_m = compute_call_metrics(call_df, start, end)
        oppts = compute_oppts(perf_df, start, end)
        vr_m_ov = compute_visits_vr(perf_df, start, end)
        visits, vr_denom_ov = vr_m_ov["visited"], vr_m_ov["vr_denom"]
        _, trials, order_ids = compute_visits_trials(perf_df, start, end)
        orders_placeholder = len(order_ids)
        closed_orders = compute_closed_orders(perf_df, start, end)
        sales_m = compute_sales_metrics(perf_df, start, end)
        qualified = lead_m["qualified_count"]
        lead = lead_m["lead"]
        recall_excl = lead_m["recall_excluded_leads"]
        cr2 = (qualified / lead * 100) if lead else 0
        cr2_cc = (qualified / recall_excl * 100) if recall_excl else 0
        oppts_from_leads = qualified
        vr = (visits / vr_denom_ov * 100) if vr_denom_ov else 0
        or_ = (orders_placeholder / visits * 100) if visits else 0
        sr = (sales_m["sales"] / closed_orders * 100) if closed_orders else 0
        cr3 = (sales_m["sales"] / oppts * 100) if oppts else 0
        revenue = sales_m["revenue"]
        sales = sales_m["sales"]
        devices = sales_m["devices"]
        abv = (revenue / sales) if sales else 0
        asp = (revenue / devices) if devices else 0
        bin_rate = (devices / sales) if sales else 0
        call_time_str = format_duration(call_m["call_time_seconds"])
        avg_str = format_duration(call_m["avg_call_time_seconds"])

        gpk = build_fixed_goals(pk)
        trials_goal = max(0, int(round(visits * gpk["or_"] / 100.0))) if visits else 0
        out["periods"][pk] = {
            "start": start.isoformat() if start else None,
            "end": end.isoformat() if end else None,
            "lead": lead,
            "recall_excluded_leads": recall_excl,
            "recall_count": lead_m["recall_count"],
            "qualified_count": qualified,
            "oppts": oppts,
            "oppts_from_leads": oppts_from_leads,
            "cr2": round(cr2, 2),
            "cr2_cc": round(cr2_cc, 2),
            "call_count": call_m["call_count"],
            "call_time_seconds": call_m["call_time_seconds"],
            "call_time_display": call_time_str,
            "avg_call_time_display": avg_str,
            "visits": visits,
            "trials": trials,
            "trials_goal": trials_goal,
            "closed_orders": closed_orders,
            "orders": orders_placeholder,
            "sales": sales,
            "devices": devices,
            "lost": sales_m["lost"],
            "lost_devices": sales_m["lost_devices"],
            "revenue": revenue,
            "abv": abv,
            "asp": asp,
            "bin_rate": round(bin_rate, 2),
            "vr": round(vr, 2),
            "or_": round(or_, 2),
            "sr": round(sr, 2),
            "cr3": round(cr3, 2),
        }
        ps, pe = prev_period_range(ref_dt, pk)
        pr = _rates_for_range(leads_df, perf_df, call_df, ps, pe)
        out["periods"][pk]["prev_rates"] = pr or {}
    out["rate_compare_labels"] = {
        "daily": "전일 대비",
        "weekly": "전주 대비",
        "monthly": "전월 대비",
        "L3M": "직전 3개월 대비",
        "L6M": "직전 6개월 대비",
    }
    # 담당자별 · 팀별 기간 지표
    for cc in owners:
        out["by_owner"][cc] = {}
        cc_id = name_to_cc_id.get(cc, "")
        for pk in periods:
            start, end = period_range(ref_dt, pk)
            out["by_owner"][cc][pk] = _metrics_one_period(
                leads_df, perf_df, call_df, start, end, cc, cc_id
            )
    for t in teams_with_display:
        team_display = t["display"]
        members = t["members"]
        out["by_team"][team_display] = {}
        for pk in periods:
            agg = {
                "lead": 0, "recall_excluded_leads": 0, "recall_count": 0, "qualified_count": 0,
                "oppts": 0, "cr2": 0, "cr2_cc": 0, "call_count": 0, "call_time_seconds": 0,
                "visits": 0, "vr_denom": 0, "trials": 0, "closed_orders": 0, "orders": 0,
                "sales": 0, "devices": 0,
                "lost": 0, "lost_devices": 0, "revenue": 0.0, "abv": 0, "asp": 0, "bin_rate": 0,
                "vr": 0, "or_": 0, "sr": 0, "cr3": 0,
            }
            for cc in members:
                m = out["by_owner"].get(cc, {}).get(pk, {})
                for k in agg:
                    if k in m:
                        v = m[k]
                        if isinstance(v, (int, float)):
                            agg[k] = agg.get(k, 0) + v
                        else:
                            agg[k] = v
            lead, recall_excl = agg["lead"], agg["recall_excluded_leads"]
            qualified, oppts = agg["qualified_count"], agg["oppts"]
            visits, vr_denom_t = agg["visits"], agg.get("vr_denom", 0)
            trials = agg["trials"]
            orders = agg.get("orders") or 0
            closed_orders = agg.get("closed_orders") or 0
            sales, revenue, devices = agg["sales"], agg["revenue"], agg["devices"]
            agg["cr2"] = round((qualified / lead * 100) if lead else 0, 2)
            agg["cr2_cc"] = round((qualified / recall_excl * 100) if recall_excl else 0, 2)
            agg["vr"] = round((visits / vr_denom_t * 100) if vr_denom_t else 0, 2)
            agg["or_"] = round((orders / visits * 100) if visits else 0, 2)
            agg["sr"] = round((sales / closed_orders * 100) if closed_orders else 0, 2)
            agg["cr3"] = round((sales / oppts * 100) if oppts else 0, 2)
            agg["abv"] = (revenue / sales) if sales else 0
            agg["asp"] = (revenue / devices) if devices else 0
            agg["bin_rate"] = round((devices / sales), 2) if sales else 0
            ctsec = agg.get("call_time_seconds") or 0
            agg["call_time_display"] = format_duration(ctsec)
            ccnt = agg.get("call_count") or 0
            agg["avg_call_time_display"] = format_duration(ctsec / ccnt) if ccnt else "-"
            out["by_team"][team_display][pk] = agg
    # 지표별 1등 상담사 (기간별)
    cc_to_team_disp = {}
    for t in teams_with_display:
        for mem in t["members"]:
            cc_to_team_disp[mem] = t["display"]

    def _best_cc_for(pk_inner, key, need_sales=0, need_lead=0):
        best_cc, best_v = None, None
        for cc in owners:
            m = out["by_owner"].get(cc, {}).get(pk_inner, {})
            if need_sales and (m.get("sales") or 0) < need_sales:
                continue
            if need_lead and (m.get("lead") or 0) < need_lead:
                continue
            v = m.get(key)
            if v is None or (isinstance(v, float) and pd.isna(v)):
                continue
            if not isinstance(v, (int, float)):
                continue
            if best_v is None or v > best_v:
                best_v, best_cc = v, cc
        return best_cc, best_v

    out["top_performers"] = {}
    for pk in periods:
        tops = {}
        for metric_key, col, need_s, need_l in [
            ("cr2", "cr2", 0, 3),
            ("cr2_cc", "cr2_cc", 0, 3),
            ("cr3", "cr3", 0, 1),
            ("revenue", "revenue", 0, 0),
            ("asp", "asp", 1, 0),
            ("abv", "abv", 1, 0),
            ("bin_rate", "bin_rate", 1, 0),
        ]:
            bc, bv = _best_cc_for(pk, col, need_sales=need_s, need_lead=need_l)
            hint = ""
            if bc:
                td = cc_to_team_disp.get(bc, "")
                tm = (out["by_team"].get(td) or {}).get(pk) or {}
                if col in ("cr2", "cr2_cc", "cr3") and tm.get(col) is not None and bv is not None:
                    diff = bv - tm[col]
                    if diff > 0.05:
                        hint = "팀 평균 +{:.1f}%p".format(diff)
                elif col == "revenue" and tm.get("revenue"):
                    hint = "팀 내 1위"
                elif col in ("asp", "abv") and tm.get(col) and bv:
                    diffm = (bv - tm[col]) / 1e6
                    if abs(diffm) >= 0.05:
                        hint = "팀 평균 {:+,.1f}M".format(diffm).replace(",", ".")
                elif col == "bin_rate" and tm.get("bin_rate") is not None:
                    hint = "max 2 근접" if bv >= 1.85 else "팀 내 1위"
            tops[metric_key] = {
                "cc": bc,
                "value": bv,
                "team": cc_to_team_disp.get(bc, "") if bc else "",
                "hint": hint or ("1위" if bc else ""),
            }
        tops["call_score"] = {"cc": None, "value": None, "team": "", "hint": "데이터 없음 (미구현)"}
        out["top_performers"][pk] = tops
    pip_list, pip_summary, pip_trend, pip_at_risk, pip_meta, pip_best = compute_pip(
        cc_df, perf_df, leads_df, ref_dt, name_to_team, name_to_cc_id, out["by_owner"], owners,
        supply_price_df=supply_price_df, call_df=call_df
    )
    out["pip"] = pip_list
    out["pip_summary"] = pip_summary
    out["pip_trend"] = pip_trend
    out["pip_at_risk"] = pip_at_risk
    out["pip_best"] = pip_best
    out["pip_benchmarks"] = (pip_meta or {}).get("benchmarks") or {}
    out["pip_month_labels"] = (pip_meta or {}).get("month_labels") or []
    out["pip_kpi"] = {
        "total_trials": (pip_meta or {}).get("pip_total_trials", 0),
        "avg_trials_per_pip": (pip_meta or {}).get("pip_avg_trials", 0),
    }
    out["pip_config"] = (pip_meta or {}).get("pip_config") or {}
    return out


# ---------- 고정 목표값 ----------
# CR2 50%, CR2 CC 60%, CR3 20%, VR 60%, OR 70%, SR 60%, BIN 1.8
# Revenue: 일 1억, 주 8억, 월 30억
def build_fixed_goals(period_key):
    rev_goals = {"daily": 1e8, "weekly": 8e8, "monthly": 30e8, "L3M": 30e8, "L6M": 30e8}
    return {
        "cr2": 45, "cr2_cc": 55, "cr3": 16,
        "vr": 55, "or_": 60, "sr": 50,
        "bin_rate": 1.8,
        "revenue": rev_goals.get(period_key, 30e8),
        "call_score": 80,
    }


def _format_period_date(dt):
    """datetime -> 'YYYY.MM.DD (요일)' 한글 요일."""
    if dt is None:
        return ""
    wd = ["월", "화", "수", "목", "금", "토", "일"]
    try:
        d = dt if hasattr(dt, "strftime") else pd.to_datetime(dt)
        return d.strftime("%Y.%m.%d") + " (" + wd[d.weekday()] + ")"
    except Exception:
        return str(dt)


def build_period_dates(periods):
    """기간별 표시용 날짜 문자열 (예: 주별 '2026.03.16 (월) ~ 2026.03.22 (일)')."""
    out = {}
    for pk, p in periods.items():
        start_s = p.get("start")
        end_s = p.get("end")
        if not start_s or not end_s:
            out[pk] = ""
            continue
        try:
            start_dt = pd.to_datetime(start_s)
            end_dt = pd.to_datetime(end_s)
            if pk == "daily":
                out[pk] = _format_period_date(start_dt)
            else:
                out[pk] = _format_period_date(start_dt) + " ~ " + _format_period_date(end_dt)
        except Exception:
            out[pk] = start_s + " ~ " + end_s
    return out


# ---------- HTML 빌드 ----------
def format_won(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return "₩0"
    x = float(x)
    if abs(x) >= 1e8:
        return f"₩{x/1e8:.2f}B"
    if abs(x) >= 1e4:
        return f"₩{x/1e6:.1f}M"
    return f"₩{x:,.0f}"


def build_html(payload):
    """제공된 HTML 템플릿 구조에 맞춰 데이터 주입."""
    ref_dt = datetime.now()
    periods = payload.get("periods", {})
    period_labels = {
        "daily": "일별 (오늘)",
        "weekly": "주별 (이번 주)",
        "monthly": "월별 (이번 달)",
        "L3M": "최근 3개월 (L3M)",
        "L6M": "최근 6개월 (L6M)",
    }
    payload["period_labels"] = period_labels
    payload["goals_by_period"] = {pk: build_fixed_goals(pk) for pk in ["daily", "weekly", "monthly", "L3M", "L6M"]}
    payload["goals"] = build_fixed_goals("weekly")  # 기본 주별
    payload["period_dates"] = build_period_dates(periods)
    payload["ref_date"] = ref_dt.strftime("%Y-%m-%d")
    # JSON 직렬화
    def default(o):
        if isinstance(o, (datetime, pd.Timestamp)):
            return o.isoformat()
        if isinstance(o, np.integer):
            return int(o)
        if isinstance(o, np.floating):
            return float(o)
        if isinstance(o, np.bool_):
            return bool(o)
        raise TypeError(type(o))
    data_json = json.dumps(payload, ensure_ascii=False, default=default)
    # HTML (사용자 제공 구조 유지, 데이터만 치환)
    html_template = r"""
<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Sales Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:15px;background:#F2F1ED;color:#1a1a18;min-width:900px}
.shell{display:flex;min-height:100vh;position:relative;transition:all .2s}
.shell.sidebar-collapsed .sidebar{width:0!important;min-width:0!important;padding:0!important;overflow:hidden;border:none;opacity:0;pointer-events:none}
.sb-open{display:none;position:fixed;left:10px;top:12px;z-index:200;width:42px;height:42px;border-radius:10px;border:1px solid #E0DED8;background:#fff;align-items:center;justify-content:center;cursor:pointer;font-size:23px;color:#185FA5;box-shadow:0 2px 8px rgba(0,0,0,.08)}
.shell.sidebar-collapsed .sb-open{display:flex!important}
.sidebar{width:200px;background:#fff;border-right:1px solid #E0DED8;flex-shrink:0;display:flex;flex-direction:column;transition:opacity .2s,min-width .2s;position:sticky;top:0;height:100vh;overflow-y:auto}
.sidebar-head{display:flex;align-items:center;justify-content:space-between;padding:12px 14px 10px;border-bottom:1px solid #E0DED8;gap:6px}
.logo{font-size:16px;font-weight:700;color:#185FA5;letter-spacing:-.3px;line-height:1.25;flex:1}
.sb-btn{background:#F4F3EF;border:1px solid #E0DED8;border-radius:6px;cursor:pointer;color:#666;font-size:14px;padding:4px 8px;flex-shrink:0}
.sb-btn:hover{background:#EBF3FC;color:#185FA5}
.nav-sec{font-size:13px;font-weight:600;color:#bbb;letter-spacing:.07em;text-transform:uppercase;padding:14px 18px 4px}
.nav-item{padding:7px 18px;font-size:15px;color:#666;cursor:pointer;border-left:3px solid transparent;transition:all .15s}
.nav-item:hover{background:#F4F3EF;color:#1a1a18}
.nav-item.active{background:#EBF3FC;color:#185FA5;border-left-color:#185FA5;font-weight:600}
.main{flex:1;display:flex;flex-direction:column;min-width:0;overflow-x:auto}
.page{display:none;padding:18px 22px;flex-direction:column;gap:13px}
.page.active{display:flex}
.topbar{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;flex-shrink:0}
.page-title{font-size:18px;font-weight:700}
.page-sub{font-size:14px;color:#999;margin-top:1px}
.controls{display:flex;gap:6px;align-items:center}
.tab-btn{font-size:14px;padding:4px 12px;border-radius:20px;border:1px solid #D3D1C7;background:#fff;color:#666;cursor:pointer;transition:all .15s;white-space:nowrap}
.tab-btn.active{background:#185FA5;color:#fff;border-color:#185FA5}
.dropdown{font-size:15px;padding:5px 12px;border:1px solid #D3D1C7;border-radius:8px;background:#fff;outline:none;color:#333;cursor:pointer;font-weight:500}
.alert-bar{background:#FAEEDA;border:1px solid #FAC775;border-radius:7px;padding:7px 12px;font-size:14px;color:#854F0B;display:flex;align-items:center;gap:6px;flex-shrink:0}
.alert-r{background:#FCEBEB;border-color:#F09595;color:#A32D2D}
.sec{font-size:13px;font-weight:700;color:#aaa;text-transform:uppercase;letter-spacing:.08em;margin-bottom:7px}
.kpi-row{display:grid;gap:9px;flex-shrink:0}
.kpi-8{grid-template-columns:repeat(8,1fr)}
.kpi-4{grid-template-columns:repeat(4,1fr)}
.kpi-5{grid-template-columns:repeat(5,1fr)}
.kpi{background:#fff;border:1px solid #E0DED8;border-radius:10px;padding:12px 14px;position:relative}
.kpi-compact{padding:10px 8px!important}
.kpi-compact .kpi-num{font-size:20px!important}
.kpi-compact .kpi-num-lg{font-size:22px!important}
.kpi-compact .kpi-def{font-size:11px!important}
.kpi-compact .kpi-name{font-size:12px!important}
.kpi-name{font-size:13px;color:#999;font-weight:600;text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px}
.kpi-def{font-size:12px;color:#ccc;margin-bottom:4px}
.kpi-num{font-size:25px;font-weight:700;line-height:1;letter-spacing:-.5px}
.kpi-num-lg{font-size:29px}
.kpi-meta{display:flex;justify-content:space-between;margin-top:4px}
.kpi-target{font-size:13px;color:#aaa}
.kpi-delta{font-size:13px;font-weight:600}
.up{color:#3B6D11}.dn{color:#A32D2D}
.prog-wrap{margin-top:7px;height:4px;background:#EEE;border-radius:2px;overflow:hidden}
.prog-fill{height:100%;border-radius:2px}
.achv{position:absolute;top:10px;right:10px;font-size:13px;font-weight:700;padding:2px 6px;border-radius:6px}
.achv-g{background:#EAF3DE;color:#3B6D11}.achv-y{background:#FAEEDA;color:#854F0B}.achv-r{background:#FCEBEB;color:#A32D2D}
.card{background:#fff;border:1px solid #E0DED8;border-radius:10px;padding:13px 15px}
.row-split{display:grid;grid-template-columns:1.5fr 1fr 1fr;gap:10px;flex-shrink:0}
.row2{display:grid;grid-template-columns:1fr 1fr;gap:10px;flex-shrink:0}
.smc{background:#F8F7F3;border:1px solid #E8E7E1;border-radius:7px;padding:9px 11px}
.smc-label{font-size:13px;color:#aaa;margin-bottom:2px}
.smc-val{font-size:18px;font-weight:700}
.smc-sub{font-size:12px;color:#bbb;margin-top:1px}
.smc-bar{margin-top:4px;height:3px;background:#E0DED8;border-radius:2px;overflow:hidden}
.smc-fill{height:100%;border-radius:2px;background:#378ADD}
.f-row{display:flex;align-items:center;gap:7px;margin-bottom:5px}
.f-lbl{font-size:12px;color:#aaa;width:58px;text-align:right;flex-shrink:0;line-height:1.15;word-break:keep-all}
.f-track{flex:1;height:22px;background:#F4F3EF;border-radius:4px;overflow:hidden}
.f-fill{height:100%;display:flex;align-items:center;padding:0 8px;border-radius:4px}
.f-num{font-size:13px;font-weight:700;color:#fff}
.f-meta{font-size:12px;width:80px;flex-shrink:0}
.badge{font-size:12px;padding:2px 6px;border-radius:6px;font-weight:700;display:inline-block}
.b-g{background:#EAF3DE;color:#3B6D11}.b-y{background:#FAEEDA;color:#854F0B}.b-r{background:#FCEBEB;color:#A32D2D}.b-b{background:#EBF3FC;color:#185FA5}.b-gray{background:#F1EFE8;color:#666}
.rt{width:100%;border-collapse:collapse;font-size:14px}
.rt th{font-size:13px;font-weight:500;color:#aaa;padding:3px 6px;border-bottom:1px solid #EEE;text-align:right}
.rt th:first-child{text-align:left}
.rt td{padding:5px 6px;border-bottom:1px solid #F4F3EF;text-align:right}
.rt td:first-child{text-align:left;color:#555}
.rt tr:last-child td{border-bottom:none}
.tt{width:100%;border-collapse:collapse;font-size:14px}
.tt th{font-size:13px;font-weight:600;color:#aaa;padding:5px 8px;border-bottom:1px solid #E0DED8;text-align:center;background:#FAFAF8;white-space:nowrap}
.tt th:first-child{text-align:left}
.tt.tt-sortable th{cursor:pointer;user-select:none}
.tt.tt-sortable th:hover{background:#F0EFE8}
.cc-detail-top{background:#d4edda !important;color:#155724}
.cc-detail-bottom{background:#f8d7da !important;color:#721c24}
.tt td{padding:5px 8px;border-bottom:1px solid #F4F3EF;text-align:center;font-size:14px;white-space:nowrap}
.tt td:first-child{text-align:left}
.tt tr:hover td{background:#F8F7F4;cursor:pointer}
.rank1{color:#185FA5;font-weight:700}
.total-row td{background:#F4F3EF!important;font-weight:700;border-top:2px solid #D3D1C7;color:#333}
.goal-card{background:#fff;border:1px solid #E0DED8;border-radius:10px;padding:14px 16px}
.goal-period{font-size:13px;font-weight:700;color:#aaa;text-transform:uppercase;letter-spacing:.07em;margin-bottom:10px}
.g-row{display:flex;align-items:center;justify-content:space-between;padding:5px 0;border-bottom:1px solid #F4F3EF}
.g-row:last-child{border-bottom:none}
.g-label{font-size:14px;color:#555;font-weight:500}
.g-vals{display:flex;gap:8px;align-items:center}
.g-actual{font-size:15px;font-weight:700}
.g-target{font-size:13px;color:#aaa}
.g-pct{font-size:13px;font-weight:700;padding:1px 6px;border-radius:5px}
.g-bar-wrap{height:3px;background:#EEE;border-radius:2px;overflow:hidden;margin-top:3px}
.g-bar-fill{height:100%;border-radius:2px}
.top-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}
.top-p-card{background:#F8F7F3;border:1px solid #E8E7E1;border-radius:8px;padding:11px 12px;min-height:92px}
.top-p-metric{font-size:13px;color:#aaa;font-weight:700;letter-spacing:.04em}
.top-p-name{font-size:16px;font-weight:700;margin:5px 0 2px;color:#333}
.top-p-val{font-size:18px;font-weight:700;color:#185FA5}
.top-p-hint{font-size:12px;color:#888;margin-top:5px;line-height:1.3}
.underperf-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px}
.up-card{background:#fff;border:1.5px solid #F7C1C1;border-radius:8px;padding:10px 12px}
.up-card-name{font-size:15px;font-weight:700;color:#A32D2D}
.up-items{margin-top:7px;display:flex;flex-direction:column;gap:4px}
.up-item{display:flex;justify-content:space-between;font-size:14px;padding:3px 0;border-bottom:1px solid #F4F3EF}
.up-item:last-child{border-bottom:none}
.pip-goal-row{display:flex;align-items:center;gap:8px;padding:5px 0;border-bottom:1px solid #F4F3EF}
.pip-goal-row:last-child{border-bottom:none}
.pip-goal-label{font-size:14px;color:#555;width:88px;flex-shrink:0}
.pip-goal-bar{flex:1;height:5px;background:#EEE;border-radius:3px;overflow:hidden}
.pip-goal-fill{height:100%;border-radius:3px}
.pip-goal-vals{display:flex;gap:5px;align-items:center;width:135px;justify-content:flex-end;flex-shrink:0;font-size:14px}
.pip-detail-stack{display:flex;flex-direction:column;gap:13px;width:100%}
.pip-sim-box-inner{display:flex;flex-direction:column;gap:10px;width:100%;min-width:0}
.pip-sim-row{display:flex;align-items:center;justify-content:space-between;gap:14px;min-width:0}
.pip-sim-row label{flex:0 0 auto;min-width:96px;font-size:14px;color:#444;font-weight:600}
.pip-sim-inp{flex:1;min-width:120px;max-width:100%;box-sizing:border-box;padding:7px 12px;font-size:15px;border:1px solid #C8C4BA;border-radius:6px;text-align:right;background:#fff}
.pip-l3m-kpi{display:grid;grid-template-columns:repeat(auto-fit,minmax(104px,1fr));gap:10px 20px;align-content:start;min-width:0}
.pip-l3m-kpi .kv{display:flex;flex-direction:column;gap:3px}
.pip-l3m-kpi .kv span{color:#888;font-size:13px}
.pip-l3m-kpi .kv strong{font-size:16px;font-weight:700;color:#222}
.sim-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.sim-input-row{display:flex;align-items:center;justify-content:space-between;padding:6px 0;border-bottom:1px solid #F4F3EF}
.sim-input-row:last-child{border-bottom:none}
.sim-label{font-size:14px;color:#555;font-weight:500}
.sim-input{width:90px;font-size:15px;padding:4px 8px;border:1px solid #D3D1C7;border-radius:5px;text-align:right;background:#fff}
.sim-result{background:#EBF3FC;border:1px solid #B5D4F4;border-radius:8px;padding:10px 14px;margin-top:10px}
.sim-res-row{display:flex;justify-content:space-between;padding:4px 0;font-size:14px;border-bottom:1px solid #D4E8F8}
.sim-res-row:last-child{border-bottom:none}
.sim-res-label{color:#185FA5;font-weight:500}
.sim-res-val{font-weight:700;color:#0C447C}
#pip-detail-content .pipd-sec{font-size:13px;font-weight:700;color:#aaa;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px}
#pip-detail-content .pipd-tag{font-size:14px;padding:4px 12px;border-radius:20px;border:1px solid #D3D1C7;color:#666;background:#fff;cursor:pointer;white-space:nowrap;transition:all .15s}
#pip-detail-content .pipd-tag:hover{background:#F4F3EF}
#pip-detail-content .pipd-tag.pipd-on{background:#185FA5;color:#fff;border-color:#185FA5}
#pip-detail-content .pipd-gn{color:#3B6D11;font-weight:700}
#pip-detail-content .pipd-gd{color:#A32D2D;font-weight:700}
#pip-detail-content .pipd-hdr{display:grid;grid-template-columns:auto 1fr repeat(5,auto);gap:0 16px;align-items:center}
#pip-detail-content .pipd-avatar{width:50px;height:50px;border-radius:50%;background:#FCEBEB;display:flex;align-items:center;justify-content:center;font-size:20px;font-weight:700;color:#A32D2D;flex-shrink:0}
#pip-detail-content .pipd-hi .pipd-name{font-size:19px;font-weight:700}
#pip-detail-content .pipd-hi .pipd-sub{font-size:14px;color:#aaa;margin-top:2px}
#pip-detail-content .pipd-hkpi{text-align:center;padding:0 6px;border-left:1px solid #EEE}
#pip-detail-content .pipd-hkpi-l{font-size:13px;color:#aaa;margin-bottom:2px;white-space:nowrap}
#pip-detail-content .pipd-hkpi-v{font-size:23px;font-weight:700;line-height:1}
#pip-detail-content .pipd-hkpi-s{font-size:13px;color:#aaa;margin-top:2px}
#pip-detail-content .pipd-pt{width:100%;border-collapse:collapse;font-size:15px}
#pip-detail-content .pipd-pt th{font-size:13px;font-weight:600;color:#aaa;padding:6px 10px;border-bottom:1px solid #E0DED8;text-align:right;white-space:nowrap;background:#FAFAF8}
#pip-detail-content .pipd-pt th:first-child{text-align:left}
#pip-detail-content .pipd-pt td{padding:7px 10px;border-bottom:1px solid #F4F3EF;text-align:right;white-space:nowrap;vertical-align:middle}
#pip-detail-content .pipd-pt td:first-child{text-align:left;font-weight:600;color:#333}
#pip-detail-content .pipd-pt tr:hover td{background:#FAFAF8}
#pip-detail-content .pipd-pt .pipd-gr td{background:#F8F7F3;font-size:13px;font-weight:700;color:#aaa;text-transform:uppercase;letter-spacing:.06em;padding:4px 10px;border-bottom:1px solid #E8E7E1}
#pip-detail-content .pipd-sic input{width:70px;font-size:15px;padding:3px 6px;border:1px solid #D3D1C7;border-radius:5px;text-align:right;background:#fff;color:#185FA5;font-weight:600}
#pip-detail-content .pipd-sic input.pipd-ro{background:#EEE;color:#555;cursor:not-allowed}
#pip-detail-content .pipd-chart-wrap{position:relative;height:280px}
#pip-detail-content .pipd-leg{display:flex;gap:14px;flex-wrap:wrap;margin-top:8px}
#pip-detail-content .pipd-li{display:flex;align-items:center;gap:5px;font-size:13px;color:#666}
#pip-detail-content .pipd-lb{width:11px;height:11px;border-radius:2px;flex-shrink:0}
#pip-detail-content .pipd-ll{width:16px;height:2px;flex-shrink:0}
#pip-detail-content .pipd-cal-hdr{display:flex;align-items:center;gap:8px;margin-bottom:10px;flex-wrap:wrap;width:100%}
#pip-detail-content .pipd-cal-nav{background:none;border:1px solid #E0DED8;border-radius:6px;width:26px;height:26px;cursor:pointer;font-size:16px;color:#555;display:flex;align-items:center;justify-content:center;flex-shrink:0}
#pip-detail-content .pipd-cal-nav:hover{background:#F4F3EF}
#pip-detail-content .pipd-cal-title{font-size:16px;font-weight:600;min-width:88px;text-align:center}
#pipd-cal-panel-main{overflow-x:hidden!important}
#pip-detail-content .pipd-mg-wrap{display:grid;grid-template-columns:repeat(7,minmax(0,1fr));gap:3px}
#pip-detail-content .pipd-mg-wrap-8{display:grid;grid-template-columns:repeat(7,minmax(0,1fr)) 180px;gap:3px}
#pip-detail-content .pipd-wk-sum{background:#E0F1FC!important;border:1px solid #A8D4EE!important;border-radius:8px!important;padding:7px 9px!important;min-height:0!important;cursor:default!important;box-sizing:border-box}
#pip-detail-content .pipd-wk-sum:hover{border-color:#72B6DE!important}
#pip-detail-content .pipd-wk-sum table{width:100%;border-collapse:collapse}
#pip-detail-content .pipd-wk-sum table th{color:#4A7EA0;font-weight:600;font-size:10px;padding:0 2px 3px;border-bottom:1px solid #B8D8EE;text-align:right;white-space:nowrap}
#pip-detail-content .pipd-wk-sum table th:first-child{text-align:left}
#pip-detail-content .pipd-wk-sum table td{padding:2px 2px;text-align:right;font-size:10.5px;white-space:nowrap}
#pip-detail-content .pipd-wk-sum table td:first-child{text-align:left;color:#3A6A8A;font-weight:600;font-size:10px}
#pip-detail-content .pipd-wk-sum .wks-gp{color:#1D9E75;font-weight:700}
#pip-detail-content .pipd-wk-sum .wks-gn{color:#C0392B;font-weight:700}
#pip-detail-content .pipd-mg-dow{font-size:13px;font-weight:600;color:#aaa;text-align:center;padding:3px 0;border-bottom:1px solid #EEE;margin-bottom:2px}
#pip-detail-content .pipd-mg-dow.we{color:#aaa}
#pip-detail-content .pipd-dow-header{font-size:13px;font-weight:600;color:#aaa;text-align:center;padding:3px 0;margin-bottom:2px}
#pip-detail-content .pipd-cal-sticky-header{position:sticky;top:0;z-index:10;background:#fff;padding-bottom:3px;border-bottom:2px solid #E5E4DF;margin-bottom:4px}
#pip-detail-content .pipd-mc{border:1px solid #E8E7E1;border-radius:6px;padding:4px 5px;min-height:150px;background:#F8F7F3;font-size:12px;cursor:default;transition:border-color .15s;box-sizing:border-box;min-width:0;overflow:hidden}
#pip-detail-content #pipd-cal-panel-main{transition:box-shadow .2s,outline .2s;position:relative}
#pip-detail-content #pipd-cal-panel-main.cal-active{outline:2px solid #5B9BD5;border-radius:6px}
#pip-detail-content #pipd-cal-panel-main:not(.cal-active)::before{content:'🖱 캘린더를 클릭하면 내부 스크롤이 활성화됩니다';display:block;position:sticky;top:0;z-index:20;background:#EBF3FC;color:#185FA5;font-size:10.5px;font-weight:600;text-align:center;padding:4px 0;pointer-events:none;border-bottom:1px solid #BFDBFE;letter-spacing:-.2px}
#pip-detail-content .pipd-mc.st-ok{border-left:3px solid #1D9E75}
#pip-detail-content .pipd-mc.st-warn{border-left:3px solid #EF9F27}
#pip-detail-content .pipd-mc.st-bad{border-left:3px solid #E24B4A}
#pip-detail-content .pipd-mc.st-hint{border-left:3px solid #AEB5C4;opacity:.82}
#pip-detail-content .pipd-mc-tg{color:#bbb;margin:0 2px;font-weight:400}
#pip-detail-content .pipd-mc-g{color:#aaa;font-size:12px;font-weight:600}
#pip-detail-content .pipd-mc-cm{margin-top:3px;padding:2px 4px;border-radius:4px;font-size:8px;line-height:1.3;font-weight:600;overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}
#pip-detail-content .pipd-mc-cm.st-ok{background:#E8F5E9;color:#2E7D32}
#pip-detail-content .pipd-mc-cm.st-warn{background:#FFF8E6;color:#B8860B}
#pip-detail-content .pipd-mc-cm.st-bad{background:#FCEBEB;color:#A32D2D}
#pip-detail-content .pipd-cal-toggle{font-size:14px;padding:5px 12px;border-radius:20px;border:1px solid #185FA5;background:#fff;color:#185FA5;cursor:pointer;font-weight:600;white-space:nowrap}
#pip-detail-content .pipd-cal-toggle:hover{background:#EBF3FC}
#pip-detail-content .pipd-detail-sel{font-size:15px;padding:5px 10px;border-radius:6px;border:1px solid #D3D1C7;min-width:160px}
#pip-detail-content .pipd-detail-month{font-size:15px;padding:5px 10px;border-radius:6px;border:1px solid #D3D1C7;min-width:132px;font-family:inherit}
#pip-detail-content table.pipd-dt{width:100%;border-collapse:collapse;font-size:14px}
#pip-detail-content table.pipd-dt th,#pip-detail-content table.pipd-dt td{padding:6px 8px;border-bottom:1px solid #EEE;text-align:center;white-space:nowrap}
#pip-detail-content table.pipd-dt th{background:#FAFAF8;font-size:13px;color:#888;font-weight:600}
#pip-detail-content table.pipd-dt td.pipd-dtc{font-variant-numeric:tabular-nums}
#pip-detail-content table.pipd-dt .h{color:#1D9E75;font-weight:700}
#pip-detail-content table.pipd-dt .m{color:#A32D2D;font-weight:700}
#pip-detail-content .pipd-mc:hover{border-color:#B5D4F4}
#pip-detail-content .pipd-mc.empty{background:transparent;border-color:transparent;cursor:default;min-height:0}
#pip-detail-content .pipd-mc.today{border:1.5px solid #185FA5}
#pip-detail-content .pipd-mc.month-first{border-top:3px solid #185FA5;background:#F0F6FF}
#pip-detail-content .pipd-mc.pip-start{border:1.5px solid #A32D2D}
#pip-detail-content .pipd-mc.hit{border-left:3px solid #1D9E75}
#pip-detail-content .pipd-mc.miss{border-left:3px solid #E24B4A}
#pip-detail-content .pipd-mc.future{opacity:1}
#pip-detail-content .pipd-mc.weekend{background:#F8F7F3;opacity:1;cursor:pointer}
#pip-detail-content .pipd-mc-date{font-weight:700;color:#555;margin-bottom:3px;display:flex;align-items:center;gap:3px}
#pip-detail-content .pipd-tdot{width:5px;height:5px;border-radius:50%;background:#185FA5;display:inline-block}
#pip-detail-content .pipd-pdot{width:5px;height:5px;border-radius:50%;background:#A32D2D;display:inline-block}
#pip-detail-content .pipd-mc-row{display:flex;justify-content:space-between;padding:1px 0;border-bottom:1px solid #F0EEE8;gap:2px}
#pip-detail-content .pipd-mc-row:last-child{border-bottom:none}
#pip-detail-content .pipd-mc-lbl{color:#bbb;font-size:11px;flex-shrink:0}
#pip-detail-content .pipd-mc-val{font-weight:600;font-size:11px;text-align:right;min-width:0}
#pip-detail-content .pipd-mc-val.h{color:#3B6D11}
#pip-detail-content .pipd-mc-val.m{color:#A32D2D}
#pip-detail-content .pipd-mc-val.p{color:#185FA5}
#pip-detail-content .pipd-mc-val.n{color:#aaa}
#pip-detail-content .pipd-wg-wrap{display:grid;grid-template-columns:repeat(7,1fr);gap:6px}
#pip-detail-content .pipd-wd{background:#F8F7F3;border:1px solid #E8E7E1;border-radius:8px;padding:9px 10px;min-height:120px}
#pip-detail-content .pipd-wd.today{border:1.5px solid #185FA5}
#pip-detail-content .pipd-wd.pip{border:1.5px solid #A32D2D}
#pip-detail-content .pipd-wd.weekend{opacity:1}
#pip-detail-content .pipd-wd-date{font-size:15px;font-weight:700;color:#333;margin-bottom:6px;display:flex;align-items:center;gap:4px}
#pip-detail-content .pipd-wm{display:flex;justify-content:space-between;font-size:13px;padding:2.5px 0;border-bottom:1px solid #F0EEE8}
#pip-detail-content .pipd-wm:last-child{border-bottom:none}
#pip-detail-content .pipd-wm-l{color:#aaa}
#pip-detail-content .pipd-wm-v{font-weight:600}
#pip-detail-content .pipd-dv-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:8px}
#pip-detail-content .pipd-dv-card{background:#F8F7F3;border:1px solid #E8E7E1;border-radius:8px;padding:10px 12px}
#pip-detail-content .pipd-dv-l{font-size:13px;color:#aaa;margin-bottom:3px}
#pip-detail-content .pipd-dv-v{font-size:21px;font-weight:700}
#pip-detail-content .pipd-dv-s{font-size:13px;color:#aaa;margin-top:2px}
#pip-detail-content .pipd-dv-prog{height:4px;background:#EEE;border-radius:2px;overflow:hidden;margin-top:5px}
#pip-detail-content .pipd-dv-fill{height:100%;border-radius:2px}
#pip-detail-content .pipd-wsum{display:flex;border:1px solid #E0DED8;border-radius:7px;overflow:hidden;margin-top:8px}
#pip-detail-content .pipd-ws-item{flex:1;padding:8px 12px;border-right:1px solid #E0DED8}
#pip-detail-content .pipd-ws-item:last-child{border-right:none}
#pip-detail-content .pipd-ws-l{font-size:13px;color:#aaa;margin-bottom:3px}
#pip-detail-content .pipd-ws-v{font-size:16px;font-weight:700}
#pip-detail-content .pipd-prog{height:4px;background:#EEE;border-radius:2px;overflow:hidden;margin-top:5px}
#pip-detail-content .pipd-prog-fill{height:100%;border-radius:2px}
#pip-detail-content .pipd-pl{width:100%;border-collapse:collapse;font-size:14px}
#pip-detail-content .pipd-pl th{font-size:13px;font-weight:600;color:#aaa;padding:5px 10px;border-bottom:1px solid #E0DED8;text-align:center;background:#FAFAF8;white-space:nowrap}
#pip-detail-content .pipd-pl th:first-child{text-align:left}
#pip-detail-content .pipd-pl td{padding:6px 10px;border-bottom:1px solid #F4F3EF;text-align:center;white-space:nowrap}
#pip-detail-content .pipd-pl td:first-child{font-family:monospace;font-size:13px;color:#185FA5;text-align:left}
#pip-detail-content .pipd-pl tr:hover td{background:#F8F7F4}
/* ---- PIP 퍼널 진단 ---- */
#pip-detail-content .pipd-fn-wrap{display:flex;align-items:stretch;gap:0}
#pip-detail-content .pipd-fn-step{flex:1;position:relative}
#pip-detail-content .pipd-fn-box{border:1.5px solid #E0DED8;border-radius:8px;padding:10px 12px;background:#fff;height:100%}
#pip-detail-content .pipd-fn-box.prob{border:2px solid #E24B4A;background:#FFF5F5}
#pip-detail-content .pipd-fn-box.warn{border:2px solid #EF9F27;background:#FFFBF0}
#pip-detail-content .pipd-fn-box.ok{border:2px solid #1D9E75;background:#F5FBF7}
#pip-detail-content .pipd-fn-arr{display:flex;align-items:center;padding:0 5px;color:#D3D1C7;font-size:21px;flex-shrink:0;margin-top:8px}
#pip-detail-content .pipd-fn-stage{font-size:12px;font-weight:700;color:#aaa;text-transform:uppercase;letter-spacing:.05em;margin-bottom:3px}
#pip-detail-content .pipd-fn-val{font-size:25px;font-weight:700;line-height:1}
#pip-detail-content .pipd-fn-vs{font-size:13px;color:#aaa;margin-top:2px}
#pip-detail-content .pipd-fn-bar{height:5px;background:#EEE;border-radius:3px;overflow:hidden;margin-top:5px}
#pip-detail-content .pipd-fn-fill{height:100%;border-radius:3px}
#pip-detail-content .pipd-fn-issue{font-size:13px;padding:4px 7px;border-radius:5px;margin-top:5px;font-weight:600}
#pip-detail-content .pipd-fn-issue.p{background:#FCEBEB;color:#A32D2D}
#pip-detail-content .pipd-fn-issue.w{background:#FAEEDA;color:#854F0B}
#pip-detail-content .pipd-fn-issue.g{background:#EAF3DE;color:#3B6D11}
/* ---- PIP 월별 목표 ---- */
#pip-detail-content .pipd-gm-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}
#pip-detail-content .pipd-gm-card{border:1.5px solid #E0DED8;border-radius:9px;padding:12px 14px;background:#F8F7F3;position:relative}
#pip-detail-content .pipd-gm-card.cur{border:2px solid #185FA5;background:#F0F7FF}
#pip-detail-content .pipd-gm-card.done{border:2px solid #1D9E75;background:#F0FBF5;opacity:.85}
#pip-detail-content .pipd-gm-title{font-size:13px;font-weight:700;color:#aaa;text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px;display:flex;align-items:center;gap:6px}
#pip-detail-content .pipd-gm-row{display:flex;justify-content:space-between;align-items:center;padding:3px 0;border-bottom:1px solid #F0EEE8;font-size:14px}
#pip-detail-content .pipd-gm-row:last-child{border-bottom:none}
#pip-detail-content .pipd-gm-lbl{color:#888}
#pip-detail-content .pipd-gm-val{font-weight:700}
#pip-detail-content .pipd-gm-derived{font-size:13px;color:#aaa;margin-top:6px;padding-top:6px;border-top:1px dashed #E0DED8}
/* ---- PIP 월별 목표 카드 내 진척 테이블 ---- */
#pip-detail-content .pipd-gm-tbl{width:100%;border-collapse:collapse;margin-top:7px}
#pip-detail-content .pipd-gm-tbl th{font-size:13px;color:#aaa;font-weight:600;padding:3px 5px;border-bottom:1px solid #E0DED8;text-align:right;white-space:nowrap;background:#F4F3EF}
#pip-detail-content .pipd-gm-tbl th:first-child{text-align:left}
#pip-detail-content .pipd-gm-tbl td{font-size:14px;padding:3px 5px;border-bottom:1px solid #F0EEE8;text-align:right;white-space:nowrap}
#pip-detail-content .pipd-gm-tbl td:first-child{text-align:left;color:#666;font-weight:500;font-size:13px}
#pip-detail-content .pipd-gm-tbl .gp{color:#1D9E75;font-weight:700}
#pip-detail-content .pipd-gm-tbl .gn{color:#E24B4A;font-weight:700}
#pip-detail-content .pipd-gm-tbl .na{color:#bbb}
/* ---- PIP 시나리오 테이블 ---- */
#pip-detail-content .pipd-sc-tbl{border-collapse:collapse;font-size:13px;white-space:nowrap}
#pip-detail-content .pipd-sc-tbl th{padding:5px 9px;border-bottom:2px solid #E0DED8;text-align:center;white-space:nowrap;font-size:12px;font-weight:600}
#pip-detail-content .pipd-sc-tbl th:first-child{text-align:left;background:#FAFAF8;color:#aaa}
#pip-detail-content .pipd-sc-tbl td{padding:5px 9px;border-bottom:1px solid #F4F3EF;text-align:center;white-space:nowrap}
#pip-detail-content .pipd-sc-tbl td:first-child{text-align:left;color:#555;font-weight:600;background:#FAFAF8;font-size:12px}
#pip-detail-content .pipd-sc-tbl .sect td{background:#F4F3EF;font-size:12px;font-weight:700;color:#aaa;letter-spacing:.05em;text-transform:uppercase;padding:3px 9px}
#pip-detail-content .pipd-sc-tbl .rev-row td{font-size:14px;font-weight:700}
#pip-detail-content .pipd-sc-tbl .rev-row td:first-child{font-size:12px}
#pip-detail-content .pipd-sc-inp{font-size:15px;padding:4px 8px;border:1px solid #D3D1C7;border-radius:5px;text-align:right;background:#fff;font-weight:600;color:#185FA5}
#pip-detail-content .pipd-sc-inp:focus{outline:none;border-color:#185FA5}
</style>
</head>
<body>
<button type="button" class="sb-open" id="sb-open" onclick="toggleSidebar()" title="메뉴 열기">☰</button>
<div class="shell" id="app-shell">
<div class="sidebar" id="sidebar">
  <div class="sidebar-head">
    <div class="logo">Sales Dashboard</div>
    <button type="button" class="sb-btn" onclick="toggleSidebar()" title="사이드바 접기">접기</button>
  </div>
  <div class="nav-sec">Overview</div>
  <div class="nav-item active" onclick="nav('overview',this)">전체</div>
  <div class="nav-sec">분석</div>
  <div class="nav-item" onclick="nav('teams',this)">팀별 현황</div>
  <div class="nav-item" onclick="nav('pip',this)">PIP</div>
</div>
<div class="main">
<div class="page active" id="page-overview">
  <div class="topbar">
    <div><div class="page-title">Sales Overview</div><div class="page-sub" id="ov-period-label">주별 · 이번 주</div><div class="page-sub" id="ov-period-dates" style="font-size:14px;color:#666;margin-top:2px"></div></div>
    <div class="controls">
      <span style="font-size:14px;color:#aaa">기간</span>
      <button class="tab-btn" onclick="setOvPeriod('daily',this)">일별</button>
      <button class="tab-btn active" onclick="setOvPeriod('weekly',this)">주별</button>
      <button class="tab-btn" onclick="setOvPeriod('monthly',this)">월별</button>
      <button class="tab-btn" onclick="setOvPeriod('q3',this)">최근 3개월</button>
      <button class="tab-btn" onclick="setOvPeriod('q6',this)">최근 6개월</button>
    </div>
  </div>
  <div class="alert-bar" id="ov-alert">데이터 기준일: <span id="ov-ref-date"></span>. 목표: CR2 50%, CR2 CC 60%, CR3 20%, VR 60%, OR 70%, SR 60%, BIN 1.8.</div>
  <div>
    <div class="sec">핵심 KPI</div>
    <div class="kpi-row kpi-5" id="ov-kpi-row"></div>
  </div>
  <div class="row-split">
  <div class="card">
    <div class="sec">영업 퍼널</div>
      <div id="ov-funnel"></div>
      <div id="ov-funnel-footer"></div>
    </div>
    <div class="card">
      <div class="sec">세부 지표</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:7px" id="ov-detail-metrics"></div>
    </div>
    <div class="card">
      <div class="sec">전환율 목표 달성 현황</div>
      <table class="rt"><thead><tr><th>지표</th><th>실적</th><th>목표</th><th>달성률</th><th id="ov-rate-compare-th">전기간 대비</th></tr></thead><tbody id="ov-rate-table"></tbody></table>
    </div>
  </div>
  <div class="card">
    <div class="sec">팀별 지표 현황</div>
    <div style="overflow-x:auto"><table class="tt tt-sortable" style="min-width:1280px"><thead><tr><th style="text-align:left" data-col="0">팀</th><th data-col="1">Leads</th><th data-col="2">기회전환수</th><th data-col="3">Appts</th><th data-col="4">CR2</th><th data-col="5">CR2 CC</th><th data-col="6">Visits</th><th data-col="7">VR</th><th data-col="8">Trials</th><th data-col="9">Orders</th><th data-col="10">OR</th><th data-col="11">Closed_Orders</th><th data-col="12">Sales</th><th data-col="13">SR</th><th data-col="14">CR3</th><th data-col="15">Revenue</th><th data-col="16">ASP</th><th data-col="17">ABV</th><th data-col="18">BIN</th><th data-col="19">Call Score</th><th data-col="20">Call Time</th></tr></thead><tbody id="ov-team-table"></tbody></table></div>
  </div>
  <div class="card">
    <div class="sec">지표별 1등 상담사</div>
    <div class="top-grid" id="ov-top-performers"></div>
  </div>
  <div class="card">
    <div class="sec">참고</div>
    <p style="font-size:14px;color:#666;margin:0">Trials=시착중(일시). Orders=Visited+시착일. OR=Orders/Visits. Closed_Orders=종료일 기간 내 Visited+시착일+종료(유실/성공) 동일 행. SR=Sales/Closed_Orders. Call Scoring 미구현.</p>
  </div>
</div>
<div class="page" id="page-teams">
  <div class="topbar">
    <div><div class="page-title">팀별 현황</div><div class="page-sub" id="team-page-sub">팀·기간 선택 시 해당 팀 담당자(cc) 지표 표시. 담당자 클릭 시 상세.</div></div>
    <div class="controls">
      <span style="font-size:14px;color:#aaa">기간</span>
      <button class="tab-btn" id="team-period-daily" onclick="setTeamPeriod('daily',this)">일별</button>
      <button class="tab-btn active" id="team-period-weekly" onclick="setTeamPeriod('weekly',this)">주별</button>
      <button class="tab-btn" id="team-period-monthly" onclick="setTeamPeriod('monthly',this)">월별</button>
      <select class="dropdown" id="team-select" onchange="renderTeamPage()">
        <option value="">팀 선택</option>
      </select>
      <input id="team-cc-search" placeholder="담당자 검색..." style="font-size:14px;padding:5px 12px;border:1px solid #D3D1C7;border-radius:8px;outline:none;width:150px" oninput="renderTeamPage()">
    </div>
  </div>
  <div><div class="sec">팀 파이프라인 현황</div><div class="kpi-row kpi-8" id="team-period-kpis"></div></div>
  <div class="row2">
    <div class="card"><div class="sec">주의 필요 팀원</div><div class="underperf-grid" id="team-underperf"></div></div>
    <div class="card"><div class="sec">팀 핵심 전환율</div><table class="rt" id="team-rate-table"><thead><tr><th style="text-align:left">지표</th><th>실적</th><th>목표</th><th>달성률</th></tr></thead><tbody></tbody></table></div>
  </div>
  <div class="card" style="flex:1;overflow:auto">
    <div class="sec" id="team-table-title">팀 선택 시 담당자별 지표</div>
    <div style="overflow-x:auto"><table class="tt tt-sortable" id="team-cc-table" style="min-width:1200px"><thead><tr><th style="text-align:left" data-col="0">담당자</th><th data-col="1">Leads</th><th data-col="2">기회전환수</th><th data-col="3">Appts</th><th data-col="4">CR2</th><th data-col="5">CR2 CC</th><th data-col="6">Visits</th><th data-col="7">VR</th><th data-col="8">Trials</th><th data-col="9">Orders</th><th data-col="10">OR</th><th data-col="11">Closed_Orders</th><th data-col="12">Sales</th><th data-col="13">SR</th><th data-col="14">CR3</th><th data-col="15">Revenue</th><th data-col="16">ASP</th><th data-col="17">BIN</th><th data-col="18">Call Time</th></tr></thead><tbody id="team-cc-body"></tbody></table></div>
  </div>
</div>
<div id="cc-detail-overlay" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.4);z-index:100;align-items:center;justify-content:center" onclick="closeCcDetail()"></div>
<div id="cc-detail-modal" style="display:none;position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);background:#fff;border-radius:12px;box-shadow:0 8px 32px rgba(0,0,0,.2);z-index:101;max-width:95%;max-height:90vh;overflow:hidden;flex-direction:column" onclick="event.stopPropagation()">
  <div style="padding:14px 18px;border-bottom:1px solid #E0DED8;display:flex;justify-content:space-between;align-items:center">
    <div><span id="cc-detail-title" style="font-size:18px;font-weight:700">담당자 상세</span><span style="font-size:14px;color:#999;margin-left:8px">기간별 지표 (상위 초록, 하위 빨강)</span></div>
    <button type="button" onclick="closeCcDetail()" style="background:none;border:none;font-size:21px;cursor:pointer;color:#666">&times;</button>
  </div>
  <div style="padding:10px 18px">
    <span style="font-size:14px;color:#999">기간</span>
    <button class="tab-btn active" id="cc-detail-period-all" onclick="setCcDetailPeriod('all',this)">전체</button>
    <button class="tab-btn" id="cc-detail-period-weekly" onclick="setCcDetailPeriod('weekly',this)">주별</button>
    <button class="tab-btn" id="cc-detail-period-monthly" onclick="setCcDetailPeriod('monthly',this)">월별</button>
  </div>
  <div style="flex:1;overflow:auto;padding:0 18px 18px">
    <table class="tt" id="cc-detail-table" style="min-width:900px"><thead><tr><th>기간</th><th>Leads</th><th>기회전환수</th><th>Appts</th><th>CR2</th><th>CR2 CC</th><th>Visits</th><th>VR</th><th>Trials</th><th>OR</th><th>Sales</th><th>SR</th><th>CR3</th><th>Revenue</th><th>ASP</th><th>BIN</th><th>콜 건수</th></tr></thead><tbody id="cc-detail-body"></tbody></table>
  </div>
</div>
<div class="page" id="page-pip">
  <div class="topbar">
    <div><div class="page-title">PIP · 성과향상 프로그램</div><div class="page-sub" id="pip-page-sub"></div></div>
    <div class="controls">
      <span class="badge b-r" style="font-size:14px;padding:4px 10px" id="pip-criteria-badge">선정 기준 (로드 후 갱신)</span>
      <button class="tab-btn active" id="pip-list-btn" onclick="showPIPView('list')">목록</button>
      <button class="tab-btn" id="pip-detail-btn" onclick="showPIPView('detail')">담당자 상세</button>
    </div>
  </div>
  <div id="pip-list-view" style="display:flex;flex-direction:column;gap:13px">
    <div class="alert-bar alert-r" id="pip-alert"></div>
    <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:9px;flex-shrink:0">
      <div class="kpi"><div class="kpi-name">PIP 대상</div><div class="kpi-num" style="color:#A32D2D;font-size:29px" id="pip-s1">0명</div><div class="kpi-target" id="pip-s1-sub">선정 풀 대비</div></div>
      <div class="kpi"><div class="kpi-name">평균 L3M 매출</div><div class="kpi-num" id="pip-s2" style="font-size:21px">—</div><div class="kpi-target" id="pip-s2-sub">PIP 제외 평균 대비</div></div>
      <div class="kpi"><div class="kpi-name">평균 CR2</div><div class="kpi-num" id="pip-s3">—</div><div class="kpi-target" id="pip-s3-sub">PIP 제외 평균 대비</div></div>
      <div class="kpi"><div class="kpi-name">평균 CR3</div><div class="kpi-num" id="pip-s4">—</div><div class="kpi-target" id="pip-s4-sub">PIP 제외 평균 대비</div></div>
      <div class="kpi"><div class="kpi-name">총 Trials (PIPELINE)</div><div class="kpi-num" id="pip-s5" style="font-size:23px">—</div><div class="kpi-target" id="pip-s5-sub">PIP 제외 평균 대비</div></div>
    </div>
    <div class="card" style="flex-shrink:0">
      <div class="sec">PIP 대상자</div>
      <p id="pip-note-pip-list" style="font-size:14px;color:#666;margin:0 0 10px 0">참조기간 월별 매출 외 열은 <strong>L3M</strong> 기준 지표입니다. (로드 후 갱신)</p>
      <div style="overflow-x:auto"><table class="tt tt-sortable" id="pip-main-table" style="min-width:1400px"><thead><tr><th style="text-align:left" data-pip-sort="cc">담당자</th><th data-pip-sort="team">팀</th><th id="pip-h-m1" data-pip-sort="m1">—월</th><th id="pip-h-m2" data-pip-sort="m2">—월</th><th id="pip-h-m3" data-pip-sort="m3">—월</th><th data-pip-sort="avgm">L3M 평균 매출</th><th data-pip-sort="cr2">CR2</th><th data-pip-sort="cr3">CR3</th><th data-pip-sort="vr">VR</th><th data-pip-sort="or_">OR</th><th data-pip-sort="sr">SR</th><th data-pip-sort="asp">ASP</th><th data-pip-sort="abv">ABV</th><th data-pip-sort="margin">L3M 수익</th><th data-pip-sort="mrate">수익률</th></tr></thead><tbody id="pip-table-body"></tbody></table></div>
    </div>
    <div class="card" style="flex-shrink:0">
      <div class="sec" id="pip-risk-sec-title">PIP 접근 위험 (로드 후 갱신)</div>
      <div style="overflow-x:auto"><table class="tt tt-sortable" id="pip-risk-table" style="min-width:1400px"><thead><tr><th style="text-align:left" data-pip-sort="cc">담당자</th><th data-pip-sort="team">팀</th><th class="pip-risk-mh" data-pip-sort="m1">—월</th><th class="pip-risk-mh" data-pip-sort="m2">—월</th><th class="pip-risk-mh" data-pip-sort="m3">—월</th><th data-pip-sort="avgm">L3M 평균 매출</th><th data-pip-sort="cr2">CR2</th><th data-pip-sort="cr3">CR3</th><th data-pip-sort="vr">VR</th><th data-pip-sort="or_">OR</th><th data-pip-sort="sr">SR</th><th data-pip-sort="asp">ASP</th><th data-pip-sort="abv">ABV</th><th data-pip-sort="margin">L3M 수익</th><th data-pip-sort="mrate">수익률</th></tr></thead><tbody id="pip-risk-body"></tbody></table></div>
    </div>
  </div>
  <div id="pip-detail-view" style="display:none;flex-direction:column;gap:13px">
    <div style="display:flex;align-items:center;gap:10px;flex-shrink:0;flex-wrap:wrap">
      <select class="dropdown" id="pip-person-select" onchange="renderPIPDetailSelect(this.value)"></select>
      <button class="tab-btn" onclick="showPIPView('list')">← 목록으로</button>
      <button class="tab-btn" id="pip-report-btn" onclick="pipdGenerateWeeklyReport()" style="background:#1B2A4A;color:#fff;border-color:#1B2A4A;font-weight:600">📄 주간 리포트 추출하기</button>
      <span style="font-size:14px;color:#aaa">담당자를 선택하면 세부 성과가 표시됩니다.</span>
    </div>
    <div id="pip-detail-content" style="width:100%;min-width:0"></div>
  </div>
</div>
</div>
</div>
<script>
var __PAYLOAD__ = null;
var _pipC1 = null, _pipC2 = null, _pipDChart = null;
function getPayload() { return __PAYLOAD__ || {}; }
function toggleSidebar() {
  var sh = document.getElementById('app-shell');
  var op = document.getElementById('sb-open');
  if (!sh) return;
  sh.classList.toggle('sidebar-collapsed');
  var c = sh.classList.contains('sidebar-collapsed');
  try { localStorage.setItem('salesDashSb', c ? '1' : '0'); } catch(e) {}
  if (op) op.style.display = c ? 'flex' : 'none';
}
function getPeriod(key) { var p = (getPayload().periods || {})[key]; return p || {}; }
function getGoals(periodKey) { var by = getPayload().goals_by_period || {}; return by[periodKey] || getPayload().goals || {}; }
function pctFmt(v) { return (v != null && !isNaN(v)) ? (Number(v).toFixed(2) + '%') : '-'; }
function wonFmt(v) {
  if (v == null || isNaN(v)) return '—';
  var x = Number(v);
  var W = (typeof String.fromCharCode === 'function') ? String.fromCharCode(8361) : '';
  if (!W) W = 'won ';
  if (Math.abs(x) >= 1e8) return W + (x/1e8).toFixed(2) + 'B';
  if (Math.abs(x) >= 1e4) return W + (x/1e6).toFixed(2) + 'M';
  return W + Math.round(x).toLocaleString();
}
function renderOverview(periodKey) {
  var key = periodKey || 'weekly';
  var p = getPeriod(key);
  var g = getGoals(key);
  var labels = getPayload().period_labels || {};
  var periodDates = getPayload().period_dates || {};
  document.getElementById('ov-period-label').textContent = labels[key] || key;
  var datesEl = document.getElementById('ov-period-dates');
  if (datesEl) datesEl.textContent = periodDates[key] || '';
  document.getElementById('ov-ref-date').textContent = getPayload().ref_date || '';

  var lead = p.lead || 0, oppts = p.oppts || 0, opptsLead = p.oppts_from_leads || 0;
  var visits = p.visits || 0, trials = p.trials || 0, ord = p.orders || 0, sales = p.sales || 0;
  var cr2 = p.cr2 != null ? p.cr2 : 0, cr2cc = p.cr2_cc != null ? p.cr2_cc : 0, cr3 = p.cr3 != null ? p.cr3 : 0;
  var revenue = p.revenue != null ? p.revenue : 0, asp = p.asp != null ? p.asp : 0, abv = p.abv != null ? p.abv : 0;
  var vr = p.vr != null ? p.vr : 0, or_ = p.or_ != null ? p.or_ : 0, sr = p.sr != null ? p.sr : 0;
  var bin = p.bin_rate != null ? p.bin_rate : 0, callCnt = p.call_count || 0, callTime = p.avg_call_time_display || '-';

  var achv = function(val, goal) {
    if (goal == null || goal === 0) return 'achv-y';
    var r = (val / goal) * 100;
    if (r >= 100) return 'achv-g';
    if (r >= 90) return 'achv-y';
    return 'achv-r';
  };
  var tg = p.trials_goal != null ? p.trials_goal : 0;
  var trialsK = p.trials || 0;
  var trialsAchvCls = (tg > 0) ? achv(trialsK, tg) : 'achv-y';
  var trialsAchvPct = (tg > 0) ? Math.round(trialsK / tg * 100) + '%' : (trialsK ? '—' : '0%');
  var trialsProg = (tg > 0) ? Math.min(100, trialsK / tg * 100) : 0;
  var kpiHtml = [
    '<div class="kpi kpi-compact"><span class="achv ' + achv(cr2, g.cr2) + '">' + (g.cr2 ? Math.round(cr2/g.cr2*100) + '%' : '-') + '</span><div class="kpi-name">CR2</div><div class="kpi-def">Lead→Opp</div><div class="kpi-num kpi-num-lg">' + pctFmt(cr2) + '</div><div class="kpi-meta"><div class="kpi-target">' + pctFmt(g.cr2) + '</div></div><div class="prog-wrap"><div class="prog-fill" style="width:' + Math.min(100, g.cr2 ? (cr2/g.cr2*100) : 0) + '%;background:#EF9F27"></div></div></div>',
    '<div class="kpi kpi-compact"><span class="achv ' + achv(cr2cc, g.cr2_cc) + '">' + (g.cr2_cc ? Math.round(cr2cc/g.cr2_cc*100) + '%' : '-') + '</span><div class="kpi-name">CR2 CC</div><div class="kpi-def">Recall 제외</div><div class="kpi-num kpi-num-lg">' + pctFmt(cr2cc) + '</div><div class="kpi-meta"><div class="kpi-target">' + pctFmt(g.cr2_cc) + '</div></div><div class="prog-wrap"><div class="prog-fill" style="width:' + Math.min(100, g.cr2_cc ? (cr2cc/g.cr2_cc*100) : 0) + '%;background:#1D9E75"></div></div></div>',
    '<div class="kpi kpi-compact"><span class="achv ' + achv(cr3, g.cr3) + '">' + (g.cr3 ? Math.round(cr3/g.cr3*100) + '%' : '-') + '</span><div class="kpi-name">CR3</div><div class="kpi-def">Opp→Sales</div><div class="kpi-num kpi-num-lg">' + pctFmt(cr3) + '</div><div class="kpi-meta"><div class="kpi-target">' + pctFmt(g.cr3) + '</div></div><div class="prog-wrap"><div class="prog-fill" style="width:' + Math.min(100, g.cr3 ? (cr3/g.cr3*100) : 0) + '%;background:#EF9F27"></div></div></div>',
    '<div class="kpi kpi-compact"><span class="achv ' + trialsAchvCls + '">' + trialsAchvPct + '</span><div class="kpi-name">Trials</div><div class="kpi-def">시착중(진행 중+시착일)</div><div class="kpi-num kpi-num-lg">' + trialsK + '</div><div class="kpi-meta"><div class="kpi-target">Orders목표환산 ' + tg + '건</div></div><div class="prog-wrap"><div class="prog-fill" style="width:' + trialsProg + '%;background:#85B7EB"></div></div></div>',
    '<div class="kpi kpi-compact"><span class="achv ' + achv(revenue, g.revenue) + '">' + (g.revenue ? Math.round(revenue/g.revenue*100) + '%' : '-') + '</span><div class="kpi-name">Revenue</div><div class="kpi-def">총 매출</div><div class="kpi-num" style="font-size:20px">' + wonFmt(revenue) + '</div><div class="kpi-meta"><div class="kpi-target">' + wonFmt(g.revenue) + '</div></div><div class="prog-wrap"><div class="prog-fill" style="width:' + Math.min(100, g.revenue ? (revenue/g.revenue*100) : 0) + '%;background:#EF9F27"></div></div></div>'
  ].join('');
  document.getElementById('ov-kpi-row').innerHTML = kpiHtml;

  var qual = p.qualified_count != null ? p.qualified_count : (opptsLead || 0);
  var closedT = p.closed_orders != null ? p.closed_orders : 0;
  var maxLead = Math.max(lead, 1);
  var maxOppts = Math.max(oppts, 1);
  var funnelHtml = [
    '<div class="f-row"><div class="f-lbl">Leads</div><div class="f-track"><div class="f-fill" style="width:100%;background:#185FA5"><span class="f-num">' + lead.toLocaleString() + '</span></div></div><div class="f-meta"></div></div>',
    '<div class="f-row"><div class="f-lbl">기회전환수</div><div class="f-track"><div class="f-fill" style="width:' + (qual/maxLead*100) + '%;background:#2A6CAD"><span class="f-num">' + qual.toLocaleString() + '</span></div></div><div class="f-meta"><span class="badge b-y">CR2 CC ' + pctFmt(cr2cc) + '</span></div></div>',
    '<div style="height:14px;margin:8px 0"></div>',
    '<div class="f-row"><div class="f-lbl">Appts</div><div class="f-track"><div class="f-fill" style="width:100%;background:#378ADD"><span class="f-num">' + oppts + '</span></div></div><div class="f-meta"></div></div>',
    '<div class="f-row"><div class="f-lbl">Visits</div><div class="f-track"><div class="f-fill" style="width:' + (oppts ? (visits/oppts*100) : 0) + '%;background:#65B5F2"><span class="f-num">' + visits + '</span></div></div><div class="f-meta"><span class="badge b-y">VR ' + pctFmt(vr) + '</span></div></div>',
    '<div class="f-row"><div class="f-lbl">Orders</div><div class="f-track"><div class="f-fill" style="width:' + (visits ? (ord/visits*100) : 0) + '%;background:#A8D5F4"><span class="f-num">' + ord + '</span></div></div><div class="f-meta"><span class="badge b-g">OR ' + pctFmt(or_) + '</span></div></div>',
    '<div class="f-row"><div class="f-lbl">Trials</div><div class="f-track"><div class="f-fill" style="width:' + (oppts ? (trials/oppts*100) : 0) + '%;background:#85B7EB"><span class="f-num" style="color:#185FA5">' + trials + '</span></div></div><div class="f-meta"><span class="badge b-b">시착중</span></div></div>',
    '<div class="f-row"><div class="f-lbl">Sales</div><div class="f-track"><div class="f-fill" style="width:' + (oppts ? (sales/oppts*100) : 0) + '%;background:#0F6E56"><span class="f-num">' + sales + '</span></div></div><div class="f-meta"><span class="badge b-g">SR ' + pctFmt(sr) + '</span></div></div>'
  ].join('');
  document.getElementById('ov-funnel').innerHTML = funnelHtml;
  document.getElementById('ov-funnel-footer').innerHTML = '<div style="display:flex;gap:12px;margin-top:6px"><span style="font-size:13px;color:#666">CR2 CC <strong>' + pctFmt(cr2cc) + '</strong></span><span style="font-size:13px;color:#666">CR3 <strong>' + pctFmt(cr3) + '</strong></span><span style="font-size:13px;color:#666">BIN <strong>' + (bin ? bin.toFixed(2) : '-') + '</strong></span></div>';

  var smcHtml = [
    '<div class="smc"><div class="smc-label">ASP</div><div class="smc-val">' + wonFmt(asp) + '</div><div class="smc-sub">목표 ' + wonFmt(g.asp) + '</div><div class="smc-bar"><div class="smc-fill" style="width:' + (g.asp ? Math.min(100, asp/g.asp*100) : 0) + '%;background:#1D9E75"></div></div></div>',
    '<div class="smc"><div class="smc-label">ABV</div><div class="smc-val">' + wonFmt(abv) + '</div><div class="smc-sub">목표 ' + wonFmt(g.abv) + '</div><div class="smc-bar"><div class="smc-fill" style="width:100%;background:#1D9E75"></div></div></div>',
    '<div class="smc"><div class="smc-label">BIN Rate</div><div class="smc-val">' + (bin ? bin.toFixed(2) : '-') + '</div><div class="smc-sub">기기수/판매건</div><div class="smc-bar"><div class="smc-fill" style="width:' + (bin ? Math.min(100, bin/2*100) : 0) + '%"></div></div></div>',
    '<div class="smc"><div class="smc-label">Call Scoring</div><div class="smc-val">—</div><div class="smc-sub">데이터 없음 (미구현)</div></div>',
    '<div class="smc"><div class="smc-label">Call Time (avg)</div><div class="smc-val">' + callTime + '</div><div class="smc-sub">평균 통화 시간</div></div>',
    '<div class="smc"><div class="smc-label">콜 건수</div><div class="smc-val">' + callCnt.toLocaleString() + '</div><div class="smc-sub">목표 ' + (g.call_count || 0) + '</div><div class="smc-bar"><div class="smc-fill" style="width:' + (g.call_count ? Math.min(100, callCnt/g.call_count*100) : 0) + '%"></div></div></div>'
  ].join('');
  document.getElementById('ov-detail-metrics').innerHTML = smcHtml;

  var prevR = p.prev_rates || {};
  var cmpTh = document.getElementById('ov-rate-compare-th');
  if (cmpTh) cmpTh.textContent = (getPayload().rate_compare_labels || {})[key] || '전기간 대비';
  function rateDelta(cur, prevV, isBin) {
    if (prevV == null || cur == null || isNaN(prevV) || isNaN(cur)) return '—';
    var d = Number(cur) - Number(prevV);
    var ar = d >= 0 ? '▲' : '▼';
    if (isBin) return ar + Math.abs(d).toFixed(2);
    return ar + Math.abs(d).toFixed(1) + '%p';
  }
  var rateRows = [
    ['CR2', pctFmt(cr2), pctFmt(g.cr2), g.cr2 ? Math.round(cr2/g.cr2*100) : 0, rateDelta(cr2, prevR.cr2, false)],
    ['CR2 CC', pctFmt(cr2cc), pctFmt(g.cr2_cc), g.cr2_cc ? Math.round(cr2cc/g.cr2_cc*100) : 0, rateDelta(cr2cc, prevR.cr2_cc, false)],
    ['CR3', pctFmt(cr3), pctFmt(g.cr3), g.cr3 ? Math.round(cr3/g.cr3*100) : 0, rateDelta(cr3, prevR.cr3, false)],
    ['VR', pctFmt(vr), pctFmt(g.vr), g.vr ? Math.round(vr/g.vr*100) : 0, rateDelta(vr, prevR.vr, false)],
    ['OR', pctFmt(or_), pctFmt(g.or_), g.or_ ? Math.round(or_/g.or_*100) : 0, rateDelta(or_, prevR.or_, false)],
    ['SR', pctFmt(sr), pctFmt(g.sr), g.sr ? Math.round(sr/g.sr*100) : 0, rateDelta(sr, prevR.sr, false)],
    ['BIN Rate', bin ? bin.toFixed(2) : '-', g.bin_rate != null ? g.bin_rate.toFixed(2) : '-', g.bin_rate ? Math.round(bin/g.bin_rate*100) : 0, rateDelta(bin, prevR.bin_rate, true)]
  ];
  document.getElementById('ov-rate-table').innerHTML = rateRows.map(function(r) {
    var badge = r[3] >= 100 ? 'b-g' : (r[3] >= 90 ? 'b-y' : 'b-r');
    return '<tr><td>' + r[0] + '</td><td>' + r[1] + '</td><td>' + r[2] + '</td><td><span class="badge ' + badge + '">' + r[3] + '%</span></td><td>' + r[4] + '</td></tr>';
  }).join('');

  function ovTeamRow(name, m, isTotal) {
    var cls = isTotal ? ' total-row' : '';
    var abv = (m.sales >= 1 && m.abv != null) ? wonFmt(m.abv) : '-';
    var asp = (m.devices >= 1 && m.asp != null) ? wonFmt(m.asp) : '-';
    var qualM = m.qualified_count != null ? m.qualified_count : 0;
    var ct = m.closed_orders != null ? m.closed_orders : 0;
    var callTimeD = m.call_time_display != null ? m.call_time_display : (m.avg_call_time_display || '-');
    var ordM = m.orders != null ? m.orders : 0;
    return '<tr class="ov-team-row' + cls + '" data-name="' + name.replace(/"/g, '&quot;') + '"><td style="text-align:left;font-weight:600">' + name + '</td><td>' + (m.lead||0) + '</td><td>' + qualM + '</td><td>' + (m.oppts||0) + '</td><td>' + pctFmt(m.cr2) + '</td><td>' + pctFmt(m.cr2_cc) + '</td><td>' + (m.visits||0) + '</td><td>' + pctFmt(m.vr) + '</td><td>' + (m.trials||0) + '</td><td>' + ordM + '</td><td>' + pctFmt(m.or_) + '</td><td>' + ct + '</td><td>' + (m.sales||0) + '</td><td>' + pctFmt(m.sr) + '</td><td>' + pctFmt(m.cr3) + '</td><td>' + wonFmt(m.revenue) + '</td><td>' + asp + '</td><td>' + abv + '</td><td>' + (m.bin_rate!=null ? m.bin_rate.toFixed(2) : '-') + '</td><td>—</td><td>' + callTimeD + '</td></tr>';
  }
  var teamHtml = [];
  (getPayload().teams || []).forEach(function(t) {
    var m = ((getPayload().by_team || {})[t.display] || {})[key] || {};
    teamHtml.push(ovTeamRow(t.display, m, false));
  });
  teamHtml.push(ovTeamRow('전체', p, true));
  var ovt = document.getElementById('ov-team-table');
  if (ovt) { ovt.innerHTML = teamHtml.join(''); }
  var ovTeamTable = document.querySelector('#page-overview .tt-sortable');
  if (ovTeamTable && !ovTeamTable._sortBound) {
    ovTeamTable._sortBound = true;
    ovTeamTable.querySelectorAll('thead th').forEach(function(th) {
      th.addEventListener('click', function() {
        var col = parseInt(th.getAttribute('data-col'), 10);
        if (isNaN(col)) return;
        var tbody = ovTeamTable.querySelector('tbody');
        var rows = Array.from(tbody.querySelectorAll('tr.ov-team-row, tr.total-row'));
        var totalRow = rows.filter(function(r) { return r.classList.contains('total-row'); })[0];
        var dataRows = rows.filter(function(r) { return !r.classList.contains('total-row'); });
        var dir = (th.getAttribute('data-sort') === 'asc') ? -1 : 1;
        th.setAttribute('data-sort', dir === 1 ? 'asc' : 'desc');
        dataRows.sort(function(a, b) {
          var ac = a.children[col];
          var bc = b.children[col];
          if (!ac || !bc) return 0;
          var at = ac.textContent.trim();
          var bt = bc.textContent.trim();
          var an = parseFloat(at.replace(/[,%]/g, ''), 10);
          var bn = parseFloat(bt.replace(/[,%]/g, ''), 10);
          if (!isNaN(an) && !isNaN(bn)) return (an - bn) * dir;
          return (at < bt ? -1 : at > bt ? 1 : 0) * dir;
        });
        dataRows.forEach(function(r) { tbody.appendChild(r); });
        if (totalRow) tbody.appendChild(totalRow);
      });
    });
  }

  var tp = (getPayload().top_performers || {})[key] || {};
  function topPc(title, e, fmt) {
    if (!e || !e.cc) {
      var h = (e && e.hint) ? e.hint : '';
      return '<div class="top-p-card"><div class="top-p-metric">' + title + '</div><div class="top-p-name">—</div><div class="top-p-val">—</div><div class="top-p-hint">' + h + '</div></div>';
    }
    return '<div class="top-p-card"><div class="top-p-metric">' + title + '</div><div class="top-p-name">' + e.cc + ' <span style="color:#999;font-weight:500;font-size:14px">' + (e.team||'') + '</span></div><div class="top-p-val">' + fmt(e.value) + '</div><div class="top-p-hint">' + (e.hint||'') + '</div></div>';
  }
  var topH = [
    topPc('CR2', tp.cr2, function(v){ return pctFmt(v); }),
    topPc('CR2 CC', tp.cr2_cc, function(v){ return pctFmt(v); }),
    topPc('CR3', tp.cr3, function(v){ return pctFmt(v); }),
    topPc('REVENUE', tp.revenue, function(v){ return wonFmt(v); }),
    topPc('ASP', tp.asp, function(v){ return wonFmt(v); }),
    topPc('ABV', tp.abv, function(v){ return wonFmt(v); }),
    topPc('BIN RATE', tp.bin_rate, function(v){ return (v!=null) ? Number(v).toFixed(2) : '—'; }),
    topPc('CALL SCORE', tp.call_score, function(v){ return String(v); })
  ].join('');
  var ovp = document.getElementById('ov-top-performers');
  if (ovp) ovp.innerHTML = topH;
}
function setOvPeriod(key, el) {
  var k = key; if (key === 'q3') k = 'L3M'; if (key === 'q6') k = 'L6M';
  document.querySelectorAll('#page-overview .tab-btn').forEach(function(b){ b.classList.remove('active'); });
  if (el) el.classList.add('active');
  renderOverview(k);
}
function nav(id, el) {
  document.querySelectorAll('.nav-item').forEach(function(n){ n.classList.remove('active'); });
  if (el) el.classList.add('active');
  document.querySelectorAll('.page').forEach(function(p){ p.classList.remove('active'); });
  var page = document.getElementById('page-' + id);
  if (page) page.classList.add('active');
  if (id === 'settings' && typeof refreshSimCur === 'function') refreshSimCur();
  if (id === 'pip' && typeof renderPip === 'function') renderPip();
}
var _teamPeriodKey = 'weekly';
function setTeamPeriod(key, el) {
  _teamPeriodKey = key;
  document.querySelectorAll('#page-teams .tab-btn').forEach(function(b) { b.classList.remove('active'); });
  if (el) el.classList.add('active');
  renderTeamPage();
}
function renderTeamPage() {
  var payload = getPayload();
  var teams = payload.teams || [];
  var sel = document.getElementById('team-select');
  var teamDisplay = sel && sel.value ? sel.value : (teams[0] && teams[0].display ? teams[0].display : '');
  if (!sel) return;
  if (sel.options.length <= 1) {
    teams.forEach(function(t) {
      var opt = document.createElement('option');
      opt.value = t.display;
      opt.textContent = t.display;
      sel.appendChild(opt);
    });
    if (teams[0]) sel.value = teams[0].display;
  }
  teamDisplay = sel.value || teamDisplay;
  var periodKey = _teamPeriodKey || 'weekly';
  var search = (document.getElementById('team-cc-search') || {}).value || '';
  search = search.toLowerCase().trim();
  var byTeam = payload.by_team || {};
  var byOwner = payload.by_owner || {};
  var teamData = byTeam[teamDisplay];
  var members = [];
  teams.forEach(function(t) {
    if (t.display === teamDisplay) members = t.members || [];
  });
  if (search) members = members.filter(function(m) { return m.toLowerCase().indexOf(search) >= 0; });
  var kpi = teamData && teamData[periodKey] ? teamData[periodKey] : {};
  var g = getGoals(periodKey);
  var achv = function(v, goal) {
    if (goal == null || goal === 0) return '';
    var p = (v / goal) * 100;
    return p >= 100 ? 'achv-g' : (p >= 90 ? 'achv-y' : 'achv-r');
  };
  var kpiHtml = '<div class="kpi"><div class="kpi-name">Leads</div><div class="kpi-num">' + (kpi.lead || 0) + '</div><div class="kpi-target">목표 ' + (g.lead != null ? g.lead : '—') + '</div></div>' +
    '<div class="kpi"><div class="kpi-name">Appts</div><div class="kpi-num">' + (kpi.oppts || 0) + '</div><div class="kpi-target">목표 ' + (g.oppts != null ? g.oppts : '—') + '</div></div>' +
    '<div class="kpi"><div class="kpi-name">Visits</div><div class="kpi-num">' + (kpi.visits || 0) + '</div></div>' +
    '<div class="kpi"><div class="kpi-name">Trials</div><div class="kpi-num">' + (kpi.trials || 0) + '</div></div>' +
    '<div class="kpi"><div class="kpi-name">Orders</div><div class="kpi-num">' + (kpi.orders || 0) + '</div></div>' +
    '<div class="kpi"><div class="kpi-name">Sales</div><div class="kpi-num">' + (kpi.sales || 0) + '</div></div>' +
    '<div class="kpi"><div class="kpi-name">Revenue</div><div class="kpi-num" style="font-size:21px">' + wonFmt(kpi.revenue) + '</div><div class="kpi-target">목표 ' + wonFmt(g.revenue) + '</div></div>' +
    '<div class="kpi"><span class="achv ' + achv(kpi.cr2, g.cr2) + '">' + (g.cr2 ? Math.round((kpi.cr2 || 0) / g.cr2 * 100) + '%' : '') + '</span><div class="kpi-name">CR2</div><div class="kpi-num">' + pctFmt(kpi.cr2) + '</div><div class="kpi-target">목표 ' + pctFmt(g.cr2) + '</div><div class="prog-wrap"><div class="prog-fill" style="width:' + (g.cr2 ? Math.min(100, (kpi.cr2 || 0) / g.cr2 * 100) : 0) + '%;background:#EF9F27"></div></div></div>' +
    '<div class="kpi"><span class="achv ' + achv(kpi.cr3, g.cr3) + '">' + (g.cr3 ? Math.round((kpi.cr3 || 0) / g.cr3 * 100) + '%' : '') + '</span><div class="kpi-name">CR3</div><div class="kpi-num">' + pctFmt(kpi.cr3) + '</div><div class="kpi-target">목표 ' + pctFmt(g.cr3) + '</div><div class="prog-wrap"><div class="prog-fill" style="width:' + (g.cr3 ? Math.min(100, (kpi.cr3 || 0) / g.cr3 * 100) : 0) + '%;background:#EF9F27"></div></div></div>';
  var kpiEl = document.getElementById('team-period-kpis');
  if (kpiEl) kpiEl.innerHTML = kpiHtml;
  var titleEl = document.getElementById('team-table-title');
  if (titleEl) titleEl.textContent = (teamDisplay || '팀') + ' · 담당자별 지표';
  var rateRows = [
    ['CR2', kpi.cr2, g.cr2], ['CR2 CC', kpi.cr2_cc, g.cr2_cc], ['CR3', kpi.cr3, g.cr3],
    ['VR', kpi.vr, g.vr], ['OR', kpi.or_, g.or_], ['SR', kpi.sr, g.sr]
  ];
  var rateHtml = rateRows.map(function(r) {
    var p = (r[2] && r[1] != null) ? Math.round(r[1] / r[2] * 100) : 0;
    var badge = p >= 100 ? 'b-g' : (p >= 90 ? 'b-y' : 'b-r');
    return '<tr><td>' + r[0] + '</td><td style="text-align:right">' + pctFmt(r[1]) + '</td><td style="text-align:right">' + pctFmt(r[2]) + '</td><td style="text-align:right"><span class="badge ' + badge + '">' + p + '%</span></td></tr>';
  }).join('');
  var rateTbody = document.querySelector('#team-rate-table tbody');
  if (rateTbody) rateTbody.innerHTML = rateHtml;
  var thr = { cr2: 40, cr2_cc: 48, cr3: 15, vr: 50, sr: 50 };
  var flagged = members.filter(function(cc) {
    var m = (byOwner[cc] || {})[periodKey] || {};
    return (m.cr2 != null && m.cr2 < thr.cr2) || (m.cr2_cc != null && m.cr2_cc < thr.cr2_cc) || (m.cr3 != null && m.cr3 < thr.cr3) || (m.vr != null && m.vr < thr.vr) || (m.sr != null && m.sr < thr.sr);
  });
  var underperfEl = document.getElementById('team-underperf');
  if (underperfEl) {
    if (flagged.length === 0) underperfEl.innerHTML = '<div style="color:#3B6D11;font-size:15px;padding:10px 0">✓ 현재 주의 필요 팀원 없음</div>';
    else underperfEl.innerHTML = flagged.slice(0, 6).map(function(cc) {
      var m = (byOwner[cc] || {})[periodKey] || {};
      var iss = [];
      if (m.cr2 != null && m.cr2 < thr.cr2) iss.push({ l: 'CR2', v: pctFmt(m.cr2), g: thr.cr2 + '%' });
      if (m.cr2_cc != null && m.cr2_cc < thr.cr2_cc) iss.push({ l: 'CR2 CC', v: pctFmt(m.cr2_cc), g: thr.cr2_cc + '%' });
      if (m.cr3 != null && m.cr3 < thr.cr3) iss.push({ l: 'CR3', v: pctFmt(m.cr3), g: thr.cr3 + '%' });
      if (m.vr != null && m.vr < thr.vr) iss.push({ l: 'VR', v: pctFmt(m.vr), g: thr.vr + '%' });
      if (m.sr != null && m.sr < thr.sr) iss.push({ l: 'SR', v: pctFmt(m.sr), g: thr.sr + '%' });
      return '<div class="up-card"><div class="up-card-name">' + cc + '</div><div style="font-size:13px;color:#aaa;margin-top:1px">' + iss.map(function(i){ return i.l; }).join(' · ') + ' 주의</div><div class="up-items">' + iss.map(function(i){ return '<div class="up-item"><span style="color:#666">' + i.l + '</span><span style="color:#A32D2D;font-weight:700">' + i.v + ' <span style="color:#aaa;font-weight:400">/ ' + i.g + '</span></span></div>'; }).join('') + '</div></div>';
    }).join('');
  }
  function teamRow(cc, m) {
    var qual = m.qualified_count != null ? m.qualified_count : 0;
    var ct = m.closed_orders != null ? m.closed_orders : 0;
    var callT = m.call_time_display != null ? m.call_time_display : (m.avg_call_time_display || '-');
    var ordM = m.orders != null ? m.orders : 0;
    return '<tr class="team-cc-row" data-cc="' + (cc || '').replace(/"/g, '&quot;') + '"><td style="text-align:left;font-weight:600;cursor:pointer" title="클릭 시 상세">' + (cc || '') + '</td><td>' + (m.lead || 0) + '</td><td>' + qual + '</td><td>' + (m.oppts || 0) + '</td><td>' + pctFmt(m.cr2) + '</td><td>' + pctFmt(m.cr2_cc) + '</td><td>' + (m.visits || 0) + '</td><td>' + pctFmt(m.vr) + '</td><td>' + (m.trials || 0) + '</td><td>' + ordM + '</td><td>' + pctFmt(m.or_) + '</td><td>' + ct + '</td><td>' + (m.sales || 0) + '</td><td>' + pctFmt(m.sr) + '</td><td>' + pctFmt(m.cr3) + '</td><td>' + wonFmt(m.revenue) + '</td><td>' + wonFmt(m.asp) + '</td><td>' + (m.bin_rate != null ? m.bin_rate.toFixed(2) : '-') + '</td><td>' + callT + '</td></tr>';
  }
  var rows = [];
  members.forEach(function(cc) {
    var m = (byOwner[cc] || {})[periodKey] || {};
    rows.push(teamRow(cc, m));
  });
  var tbody = document.getElementById('team-cc-body');
  if (tbody) tbody.innerHTML = rows.join('');
  tbody.querySelectorAll('tr.team-cc-row td:first-child').forEach(function(td) {
    td.onclick = function() {
      var tr = td.closest('tr');
      var cc = tr && tr.getAttribute ? tr.getAttribute('data-cc') : '';
      if (cc) openCcDetail(cc);
    };
  });
  var teamTable = document.getElementById('team-cc-table');
  if (teamTable && !teamTable._sortBound) {
    teamTable._sortBound = true;
    teamTable.querySelectorAll('thead th').forEach(function(th) {
      th.addEventListener('click', function() {
        var col = parseInt(th.getAttribute('data-col'), 10);
        if (isNaN(col)) return;
        var tbody = teamTable.querySelector('tbody');
        var rows = Array.from(tbody.querySelectorAll('tr.team-cc-row'));
        var dir = (th.getAttribute('data-sort') === 'asc') ? -1 : 1;
        th.setAttribute('data-sort', dir === 1 ? 'asc' : 'desc');
        rows.sort(function(a, b) {
          var ac = a.children[col], bc = b.children[col];
          if (!ac || !bc) return 0;
          var at = ac.textContent.trim(), bt = bc.textContent.trim();
          var an = parseFloat(at.replace(/[,%]/g, ''), 10), bn = parseFloat(bt.replace(/[,%]/g, ''), 10);
          if (!isNaN(an) && !isNaN(bn)) return (an - bn) * dir;
          return (at < bt ? -1 : at > bt ? 1 : 0) * dir;
        });
        rows.forEach(function(r) { tbody.appendChild(r); });
      });
    });
  }
}
var _ccDetailPeriod = 'all';
var _ccDetailName = '';
function setCcDetailPeriod(key, el) {
  _ccDetailPeriod = key;
  document.querySelectorAll('#cc-detail-modal .tab-btn').forEach(function(b) { b.classList.remove('active'); });
  if (el) el.classList.add('active');
  if (_ccDetailName) renderCcDetailTable(_ccDetailName);
}
function openCcDetail(ccName) {
  _ccDetailName = ccName;
  document.getElementById('cc-detail-title').textContent = ccName + ' · 기간별 지표';
  _ccDetailPeriod = 'all';
  document.querySelectorAll('#cc-detail-modal .tab-btn').forEach(function(b) { b.classList.remove('active'); });
  var allBtn = document.getElementById('cc-detail-period-all');
  if (allBtn) allBtn.classList.add('active');
  renderCcDetailTable(ccName);
  document.getElementById('cc-detail-overlay').style.display = 'flex';
  document.getElementById('cc-detail-modal').style.display = 'flex';
}
function closeCcDetail() {
  document.getElementById('cc-detail-overlay').style.display = 'none';
  document.getElementById('cc-detail-modal').style.display = 'none';
}
function renderCcDetailTable(ccName) {
  var byOwner = getPayload().by_owner || {};
  var periods = getPayload().periods || {};
  var labels = getPayload().period_labels || {};
  var data = byOwner[ccName] || {};
  var order = _ccDetailPeriod === 'weekly' ? ['weekly'] : (_ccDetailPeriod === 'monthly' ? ['monthly'] : ['daily','weekly','monthly','L3M']);
  var rows = [];
  order.forEach(function(pk) {
    if (!data[pk]) return;
    var m = data[pk];
    rows.push({
      key: pk,
      label: labels[pk] || pk,
      lead: m.lead || 0, qual: m.qualified_count != null ? m.qualified_count : 0, oppts: m.oppts || 0,
      cr2: m.cr2, cr2_cc: m.cr2_cc, visits: m.visits || 0, vr: m.vr, trials: m.trials || 0, or_: m.or_, sales: m.sales || 0, sr: m.sr, cr3: m.cr3,
      revenue: m.revenue != null ? m.revenue : 0, asp: m.asp, bin: m.bin_rate, call_count: m.call_count || 0
    });
  });
  var numCols = ['lead','qual','oppts','cr2','cr2_cc','visits','vr','trials','or_','sales','sr','cr3','revenue','asp','bin','call_count'];
  var colKeys = numCols;
  var vals = {};
  colKeys.forEach(function(k) {
    vals[k] = rows.map(function(r) { var v = r[k]; return (v != null && !isNaN(v)) ? Number(v) : null; });
  });
  var isHigherBetter = { lead:1, qual:1, oppts:1, cr2:1, cr2_cc:1, visits:1, vr:1, trials:1, or_:1, sales:1, sr:1, cr3:1, revenue:1, asp:1, bin:1, call_count:1 };
  var getCellClass = function(col, val) {
    var arr = (vals[col] || []).filter(function(x) { return x != null && !isNaN(x); });
    if (arr.length < 2) return '';
    var sorted = arr.slice().sort(function(a,b) { return a - b; });
    var min = sorted[0], max = sorted[sorted.length - 1];
    var v = val != null && !isNaN(val) ? Number(val) : null;
    if (v == null) return '';
    if (v === max && isHigherBetter[col]) return 'cc-detail-top';
    if (v === min && isHigherBetter[col]) return 'cc-detail-bottom';
    if (v === max && !isHigherBetter[col]) return 'cc-detail-bottom';
    if (v === min && !isHigherBetter[col]) return 'cc-detail-top';
    return '';
  };
  var html = rows.map(function(r) {
    var cells = ['<td style="text-align:left;font-weight:600">' + r.label + '</td>'];
    cells.push('<td class="' + getCellClass('lead', r.lead) + '">' + r.lead + '</td>');
    cells.push('<td class="' + getCellClass('qual', r.qual) + '">' + r.qual + '</td>');
    cells.push('<td class="' + getCellClass('oppts', r.oppts) + '">' + r.oppts + '</td>');
    cells.push('<td class="' + getCellClass('cr2', r.cr2) + '">' + pctFmt(r.cr2) + '</td>');
    cells.push('<td class="' + getCellClass('cr2_cc', r.cr2_cc) + '">' + pctFmt(r.cr2_cc) + '</td>');
    cells.push('<td class="' + getCellClass('visits', r.visits) + '">' + r.visits + '</td>');
    cells.push('<td class="' + getCellClass('vr', r.vr) + '">' + pctFmt(r.vr) + '</td>');
    cells.push('<td class="' + getCellClass('trials', r.trials) + '">' + r.trials + '</td>');
    cells.push('<td class="' + getCellClass('or_', r.or_) + '">' + pctFmt(r.or_) + '</td>');
    cells.push('<td class="' + getCellClass('sales', r.sales) + '">' + r.sales + '</td>');
    cells.push('<td class="' + getCellClass('sr', r.sr) + '">' + pctFmt(r.sr) + '</td>');
    cells.push('<td class="' + getCellClass('cr3', r.cr3) + '">' + pctFmt(r.cr3) + '</td>');
    cells.push('<td class="' + getCellClass('revenue', r.revenue) + '">' + wonFmt(r.revenue) + '</td>');
    cells.push('<td class="' + getCellClass('asp', r.asp) + '">' + wonFmt(r.asp) + '</td>');
    cells.push('<td class="' + getCellClass('bin', r.bin) + '">' + (r.bin != null ? r.bin.toFixed(2) : '-') + '</td>');
    cells.push('<td class="' + getCellClass('call_count', r.call_count) + '">' + r.call_count + '</td>');
    return '<tr>' + cells.join('') + '</tr>';
  }).join('');
  var tbody = document.getElementById('cc-detail-body');
  if (tbody) tbody.innerHTML = html || '<tr><td colspan="17">데이터 없음</td></tr>';
}
function showPIPView(view) {
  var listEl = document.getElementById('pip-list-view');
  var detailEl = document.getElementById('pip-detail-view');
  var listBtn = document.getElementById('pip-list-btn');
  var detailBtn = document.getElementById('pip-detail-btn');
  if (view === 'list') {
    if (listEl) listEl.style.display = 'flex';
    if (detailEl) detailEl.style.display = 'none';
    if (listBtn) listBtn.classList.add('active');
    if (detailBtn) detailBtn.classList.remove('active');
  } else {
    if (listEl) listEl.style.display = 'none';
    if (detailEl) detailEl.style.display = 'flex';
    if (listBtn) listBtn.classList.remove('active');
    if (detailBtn) detailBtn.classList.add('active');
    var sel = document.getElementById('pip-person-select');
    var dc = document.getElementById('pip-detail-content');
    if (sel && sel.value) renderPIPDetailContent(sel.value);
    else if (dc) dc.innerHTML = '<div class="card"><p style="color:#999;padding:14px;font-size:15px">목록에서 행을 클릭하거나, 위에서 담당자를 선택하세요.</p></div>';
  }
}
function renderPIPDetailSelect(name) {
  if (name) renderPIPDetailContent(name);
}
function findPipRow(name) {
  var pip = getPayload().pip || [];
  var r = pip.find(function(p) { return p.cc === name; });
  if (r) return r;
  var risk = getPayload().pip_at_risk || [];
  return risk.find(function(p) { return p.cc === name; }) || null;
}
function pipBench() { return getPayload().pip_benchmarks || {}; }
function pipCellStyle(pVal, benchVal) {
  if (pVal == null || benchVal == null || isNaN(Number(pVal)) || isNaN(Number(benchVal))) return '';
  var pv = Number(pVal), bv = Number(benchVal);
  if (bv === 0) return '';
  if (pv >= bv * 1.001) return 'color:#1D9E75;font-weight:600';
  if (pv <= bv * 0.999) return 'color:#E24B4A;font-weight:600';
  return '';
}
var _pipSortState = {};
function pipSortValue(p, key) {
  switch (key) {
    case 'cc': return p.cc || '';
    case 'team': return p.team_display || '';
    case 'm1': return p.l3m_m1 != null ? p.l3m_m1 : 0;
    case 'm2': return p.l3m_m2 != null ? p.l3m_m2 : 0;
    case 'm3': return p.l3m_m3 != null ? p.l3m_m3 : 0;
    case 'avgm': return p.l3m_rev_month_avg != null ? p.l3m_rev_month_avg : (((p.l3m_m1 || 0) + (p.l3m_m2 || 0) + (p.l3m_m3 || 0)) / 3);
    case 'cr2': return p.cr2 || 0;
    case 'cr3': return p.cr3 || 0;
    case 'vr': return p.vr || 0;
    case 'or_': return p.or_ || 0;
    case 'sr': return p.sr || 0;
    case 'asp': return p.asp || 0;
    case 'abv': return p.abv || 0;
    case 'call': return p.call_monitor != null ? p.call_monitor : p.call_count;
    case 'status': return p.status === '긴급' ? 0 : (p.status === '위험' ? 1 : 2);
    case 'margin': return p.l3m_margin != null ? p.l3m_margin : -Infinity;
    case 'mrate':  return p.l3m_margin_rate != null ? p.l3m_margin_rate : -Infinity;
    default: return 0;
  }
}
function pipCompareRows(a, b, key, dir) {
  var va = pipSortValue(a, key), vb = pipSortValue(b, key);
  if (typeof va === 'string') return dir * va.localeCompare(vb, 'ko');
  return dir * (Number(va) - Number(vb));
}
/* ═══════════════════════════════════════════════════════
   주간 성과 리포트 PDF 생성
   ═══════════════════════════════════════════════════════ */
function pipdGenerateWeeklyReport() {
  var C = window.__pipDetailCtx;
  if (!C) { alert('담당자를 먼저 선택하세요.'); return; }
  var ccName = C.ccName || C.cc_name || '';
  if (!ccName) { alert('담당자 이름을 확인할 수 없습니다. 담당자 상세 페이지를 먼저 열어주세요.'); return; }
  var pip = findPipRow(ccName);
  var d   = (pip && pip.detail) || {};
  var ui  = d.pip_ui || C.ui || {};
  var act = ui.actual || C.act || {};
  var td  = ui.targets_default || C.td || {};

  /* ── 기간 계산 ── */
  var today = new Date();
  /* C.refY/refM1/refD 가 있으면 기준 날짜로 사용 */
  if (C.refY && C.refM1 && C.refD) {
    today = new Date(C.refY, C.refM1 - 1, C.refD);
  }
  var todayDow = today.getDay(); // 0=Sun
  var mon = new Date(today); mon.setDate(today.getDate() - ((todayDow + 6) % 7));
  /* 지난주: 캘린더와 동일하게 월~일(7일) 기준 */
  var lastMon = new Date(mon); lastMon.setDate(mon.getDate() - 7);
  var lastSun = new Date(mon); lastSun.setDate(mon.getDate() - 1);  /* 이번주 월요일-1 = 지난주 일요일 */
  var nextMon = new Date(mon);
  var nextFri = new Date(mon); nextFri.setDate(mon.getDate() + 4);
  function fmtD(dt) {
    return (dt.getMonth()+1).toString().padStart(2,'0') + '/' + dt.getDate().toString().padStart(2,'0');
  }
  function fmtFull(dt) {
    return dt.getFullYear() + '-' + (dt.getMonth()+1).toString().padStart(2,'0') + '-' + dt.getDate().toString().padStart(2,'0');
  }
  var lastWeekLabel = lastMon.getFullYear() + '년 ' + (lastMon.getMonth()+1) + '월 ' + fmtD(lastMon) + '~' + fmtD(lastSun);
  var issueDate = fmtFull(new Date());
  var thisWeekLabel = fmtD(nextMon) + '~' + fmtD(nextFri) + ' (영업일 5일)';

  /* ── 지난주 실적 — pipdSumRange() 활용 (캘린더와 동일하게 월~일 7일 집계) ── */
  var sLW = pipdSumRange(lastMon, lastSun);
  /* 이번달 누적 (1일 ~ 오늘, 월배너·목표 재계산 공용, PIP 상세와 동일 기준) */
  var curMonStart = new Date(today.getFullYear(), today.getMonth(), 1);
  var sThisMon    = pipdSumRange(curMonStart, today);

  /* 월별 고정 목표 */
  var MO_APPTS  = 169, MO_VISITS = 93, MO_ORDERS = 56, MO_SALES = 28, MO_REV = 70;
  var W_APPTS   = Math.round(MO_APPTS  / 4);
  var W_VISITS  = Math.round(MO_VISITS / 4);
  var W_ORDERS  = Math.round(MO_ORDERS / 4);
  var W_SALES   = Math.round(MO_SALES  / 4);
  var W_REV     = Math.round(MO_REV    / 4 * 10) / 10;  /* 숫자 M 단위 */
  var W_LEADS   = Math.round(W_APPTS   / 0.45);

  /* 지난주 실적 (캘린더 데이터) */
  var aAppts  = sLW.appts  || 0;
  var aVisits = sLW.visits || 0;
  var aOrders = sLW.orders || 0;
  var aSales  = sLW.sales  || 0;
  var aRev    = Math.round((sLW.rev || 0) * 10) / 10;  /* M 단위 숫자 */
  /* Leads: monthly_breakdown L3M 평균 / 4 */
  var _l3mMbLeads = (act.monthly_breakdown || []).filter(function(m){ return !m.is_current; });
  var _avgLeadsM  = _l3mMbLeads.length
    ? _l3mMbLeads.reduce(function(s,m){ return s + (m.leads||0); }, 0) / _l3mMbLeads.length
    : 0;
  var aLeads = Math.round(_avgLeadsM / 4);
  /* CR 실적 — 지난주 집계 우선, fallback L3M */
  var cr2Act = (aLeads > 0 && aAppts > 0) ? Math.round(aAppts / aLeads * 1000) / 10 : (act.cr2 || 0);
  /* VR/OR: 지난주 실제 볼륨으로 계산
     SR: 주간 내 Orders(TAD기준) ÷ Sales(QID기준)는 날짜기준이 달라 100% 초과 오류 발생
         → L3M 평균(Python에서 Sales÷Closed_Orders로 계산된 act.sr) 사용 */
  var vrAct  = sLW.vr_denom > 0 ? Math.round(sLW.visits / sLW.vr_denom * 1000) / 10 : (act.vr  || 0);
  var orAct  = aVisits > 0      ? Math.round(aOrders / aVisits * 1000) / 10           : (act.or_ || 0);
  var srAct  = act.sr || 0;  /* L3M 평균 SR (Sales÷Closed_Orders) */

  function pct(a, t) { return (t != null && t !== 0) ? Math.round(a / t * 100) : 0; }
  function gap(a, t) { return Math.round((a - t) * 10) / 10; }
  function cls(p) { return p >= 110 ? 'good' : p >= 90 ? 'warn' : 'bad'; }

  var pcts = {
    leads:  pct(aLeads,  W_LEADS),
    appts:  pct(aAppts,  W_APPTS),
    visits: pct(aVisits, W_VISITS),
    orders: pct(aOrders, W_ORDERS),
    sales:  pct(aSales,  W_SALES),
    rev:    pct(aRev,    W_REV),
  };
  var overallPct = Math.round(Object.values(pcts).reduce(function(s,v){return s+v;},0) / 6);
  var overallCls = overallPct >= 110 ? 'good' : overallPct >= 90 ? 'warn' : 'bad';

  var abvAct  = act.abv_m  || td.abv_m || 2.5;
  var cr2Tgt  = td.cr2  || 45;
  var vrTgt   = td.vr   || 55;
  var orTgt   = td.or_  || 60;
  var srTgt   = td.sr   || 50;

  function crChip(label, actual, tgt) {
    var ok = actual >= tgt - 0.05;
    return '<span class="cr-chip ' + (ok ? 'good' : 'bad') + '">'
      + label + ' ' + actual.toFixed(1) + '% '
      + (ok ? '목표 '+tgt+'% 초과 ✓' : '목표 '+tgt+'% 미달') + '</span>';
  }

  /* ── 6개월 차트 데이터 (L3M 실적 + 이번달 이중표시 + 미래 2개월 예측) ── */
  var mb     = (act.monthly_breakdown || []);
  var l3mMb  = mb.filter(function(m){ return !m.is_current; });
  var curMb  = mb.filter(function(m){ return m.is_current; });

  /* 이번달 경과 비율 기반 스케일 */
  var elapsedWeeks = Math.max(1, Math.ceil(today.getDate() / 7));
  var scale = 4 / elapsedWeeks;

  function lbl(y, m) {
    return String(y).slice(2) + '년 ' + m + '월';
  }
  function nextYM(y, m, delta) {
    var mm = m - 1 + delta; return { y: y + Math.floor(mm / 12), m: (mm % 12) + 1 };
  }

  /* ── 월별 성과 추이 차트: pipdSumRange 기반 실제값 (monthly_breakdown fallback) ── */
  var chartLabels = [], actAppts = [], actVisits = [], actOrders = [], actSales = [], actRevM = [];
  var prjAppts    = [], prjVisits = [], prjOrders = [], prjSales = [], prjRevM  = [];
  var _l3mSumApp=0, _l3mSumVis=0, _l3mSumOrd=0, _l3mSumSal=0, _l3mSumRev=0;
  var _l3mCnt = l3mMb.length || 1;
  l3mMb.forEach(function(m) {
    chartLabels.push(lbl(m.year, m.month));
    /* L3M 월 실적: pipdSumRange 캘린더 대신 monthly_breakdown 서버 계산값 직접 사용
       (캘린더 일별 집계는 작성일자 매칭 누락이 발생해 실제값보다 훨씬 작게 잡힘) */
    var ma  = (m.oppts     || 0);
    var mv  = (m.visits    || 0);
    var mo  = (m.orders    || 0);
    var ms2 = (m.sales     || 0);
    var mr  = Math.round((m.revenue_m || 0) * 10) / 10;
    actAppts.push(ma); actVisits.push(mv); actOrders.push(mo); actSales.push(ms2); actRevM.push(mr);
    prjAppts.push(null); prjVisits.push(null); prjOrders.push(null); prjSales.push(null); prjRevM.push(null);
    _l3mSumApp += ma; _l3mSumVis += mv; _l3mSumOrd += mo; _l3mSumSal += ms2; _l3mSumRev += mr;
  });
  /* L3M 평균값 (미래 예측 기준) */
  var l3mAvgAppts  = _l3mSumApp / _l3mCnt;
  var l3mAvgVisits = _l3mSumVis / _l3mCnt;
  var l3mAvgOrders = _l3mSumOrd / _l3mCnt;
  var l3mAvgSales  = _l3mSumSal / _l3mCnt;
  var l3mAvgRevM   = _l3mSumRev / _l3mCnt;

  /* 이번달 (오늘까지 실적 + 예측) — sThisMon 재사용 */
  var curY = today.getFullYear(), curM = today.getMonth() + 1;
  chartLabels.push(lbl(curY, curM));
  actAppts.push(sThisMon.appts);  actVisits.push(sThisMon.visits);
  actOrders.push(sThisMon.orders); actSales.push(sThisMon.sales);
  actRevM.push(Math.round(sThisMon.rev * 10) / 10);
  prjAppts.push(Math.round((sThisMon.appts  > 0 ? sThisMon.appts  : l3mAvgAppts)  * scale));
  prjVisits.push(Math.round((sThisMon.visits > 0 ? sThisMon.visits : l3mAvgVisits) * scale));
  prjOrders.push(Math.round((sThisMon.orders > 0 ? sThisMon.orders : l3mAvgOrders) * scale));
  prjSales.push(Math.round((sThisMon.sales   > 0 ? sThisMon.sales  : l3mAvgSales)  * scale));
  prjRevM.push(Math.round((sThisMon.rev      > 0 ? sThisMon.rev    : l3mAvgRevM)   * scale * 10) / 10);
  /* 미래 2개월 예측 */
  for (var fi = 1; fi <= 2; fi++) {
    var nxt = nextYM(curY, curM, fi);
    chartLabels.push(lbl(nxt.y, nxt.m));
    actAppts.push(null); actVisits.push(null); actOrders.push(null); actSales.push(null); actRevM.push(null);
    prjAppts.push(Math.round(l3mAvgAppts)); prjVisits.push(Math.round(l3mAvgVisits));
    prjOrders.push(Math.round(l3mAvgOrders)); prjSales.push(Math.round(l3mAvgSales));
    prjRevM.push(Math.round(l3mAvgRevM * 10) / 10);
  }

  /* 목표선 (월별 고정) */
  var tgtAppts  = MO_APPTS,  tgtVisits = MO_VISITS,  tgtOrders = MO_ORDERS;
  var tgtSales  = MO_SALES,  tgtRevM   = MO_REV;

  /* ── 진단 — 달성률 편차 기준 우선순위 정렬, 상위 3개만 ── */
  var allDiag = [
    {label:'Leads',   a:aLeads,  t:W_LEADS,  p:pcts.leads,  isRev:false},
    {label:'Appts',   a:aAppts,  t:W_APPTS,  p:pcts.appts,  isRev:false},
    {label:'Visits',  a:aVisits, t:W_VISITS, p:pcts.visits, isRev:false},
    {label:'Orders',  a:aOrders, t:W_ORDERS, p:pcts.orders, isRev:false},
    {label:'Sales',   a:aSales,  t:W_SALES,  p:pcts.sales,  isRev:false},
    {label:'Revenue', a:aRev,    t:W_REV,    p:pcts.rev,    isRev:true},
    {label:'CR2',     a:cr2Act,  t:cr2Tgt,   p:Math.round(cr2Act/Math.max(cr2Tgt,1)*100), isRate:true},
    {label:'VR',      a:vrAct,   t:vrTgt,    p:Math.round(vrAct/Math.max(vrTgt,1)*100),   isRate:true},
    {label:'OR',      a:orAct,   t:orTgt,    p:Math.round(orAct/Math.max(orTgt,1)*100),   isRate:true},
    {label:'SR',      a:srAct,   t:srTgt,    p:Math.round(srAct/Math.max(srTgt,1)*100),   isRate:true},
  ];
  /* 달성률 오름차순 → 하위 = 집중 필요, 내림차순 → 상위 = 강점 */
  var sortedAsc  = allDiag.slice().sort(function(a,b){ return a.p - b.p; });
  var sortedDesc = allDiag.slice().sort(function(a,b){ return b.p - a.p; });
  /* 집중 포인트: 달성률 90% 미만 상위 3개 */
  var focuses = sortedAsc.filter(function(it){ return it.p < 90; }).slice(0,3);
  /* 강점: 달성률 110% 초과 상위 3개 */
  var strengths = sortedDesc.filter(function(it){ return it.p > 110; }).slice(0,3);
  /* 달성률 표시 */
  function pctStr(it) {
    return it.isRate ? it.a.toFixed(1) + '% (목표 ' + it.t + '%)' : it.p + '%';
  }

  /* ── 이번주 목표: (월목표 - 이번달 누적) / 남은 주차 ── */
  var lastDayOfMon = new Date(today.getFullYear(), today.getMonth() + 1, 0);
  var daysLeft = Math.floor((lastDayOfMon - mon) / 86400000) + 1;
  /* 소수 주차 사용 (Math.ceil 금지 — 분모 과대 → 목표 과소 방지) */
  var remainWeeks = Math.max(1, daysLeft / 7);
  function calcTWGoal(monthly, actSoFar) {
    var remaining = Math.max(0, monthly - actSoFar);
    /* Math.max(monthly/4) 하한선 제거 — 순수하게 (잔여) / (남은 주차) */
    return Math.max(1, Math.round(remaining / remainWeeks));
  }
  var twAppts  = calcTWGoal(MO_APPTS,  sThisMon.appts);
  var twVisits = calcTWGoal(MO_VISITS, sThisMon.visits);
  var twOrders = calcTWGoal(MO_ORDERS, sThisMon.orders);
  var twSales  = calcTWGoal(MO_SALES,  sThisMon.sales);
  var twRev    = Math.max(0.1, Math.round((MO_REV - Math.round(sThisMon.rev * 10) / 10) / remainWeeks * 10) / 10);
  var BDAYS    = 5;

  /* ── 파이프라인 데이터 ── */
  var upcomingRows = d.upcoming_visit_rows || [];
  var trialRows    = d.pipeline_rows       || [];

  function shortId(id) {
    /* 기회 ID 전체 18자리 표시 */
    return String(id || '');
  }

  /* ── 이번주 방문예정: 날짜별 그룹핑, 시간 첫 번째 열 ── */
  var upcomingHtml = (function() {
    if (!upcomingRows.length) return '<tr><td colspan="4" style="color:#94A3B8;text-align:center;font-size:9px">방문 예정 없음</td></tr>';
    var groups = [], groupMap = {};
    upcomingRows.forEach(function(r) {
      var dateKey = (r.ilsi || '—');
      if (!groupMap[dateKey]) { groupMap[dateKey] = []; groups.push(dateKey); }
      groupMap[dateKey].push(r);
    });
    var html = '';
    groups.forEach(function(dk) {
      var rows = groupMap[dk];
      html += '<tr class="date-group"><td colspan="4" style="background:#EFF6FF;color:#1D4ED8;font-weight:700;font-size:8.5px;padding:3px 8px;border-top:1px solid #BFDBFE;">' + dk + '</td></tr>';
      rows.forEach(function(r) {
        html += '<tr>'
          + '<td style="white-space:nowrap;padding-left:14px;font-family:\'DM Mono\',monospace;">' + (r.time||'—') + '</td>'
          + '<td style="white-space:nowrap">' + shortId(r.opp_id) + '</td>'
          + '<td>' + (r.partner||'—') + '</td>'
          + '</tr>';
      });
    });
    return html;
  })();

  /* ── 시험착용 진행중: 시착일 기준 오름차순 정렬 + 주차 그룹핑 ── */
  var sortedTrialRows = trialRows.slice().sort(function(a, b) {
    var sa = (a.sichak || '').replace(/\./g, '-');
    var sb = (b.sichak || '').replace(/\./g, '-');
    return sa < sb ? -1 : sa > sb ? 1 : 0;
  });
  function _sichakWeekGroup(sichakStr) {
    /* sichakStr: "YYYY.MM.DD" or "YYYY-MM-DD" */
    if (!sichakStr || sichakStr === '—') return '날짜 미확인';
    var s = sichakStr.replace(/\./g, '-');
    var dt = new Date(s);
    if (isNaN(dt)) return '날짜 미확인';
    var diffMs = today - dt;
    var diffDays = Math.floor(diffMs / 86400000) + 1; /* 당일=1일차 */
    if (diffDays <= 7)  return '1주차';
    if (diffDays <= 14) return '2주차';
    if (diffDays <= 21) return '3주차';
    if (diffDays <= 28) return '4주차';
    if (diffDays <= 35) return '5주차';
    return '시험착용기간 초과';
  }
  var trialHtml = (function() {
    if (!sortedTrialRows.length) return '<tr><td colspan="7" style="color:#94A3B8;text-align:center;font-size:9px">현재 시험착용 중인 고객 없음</td></tr>';
    /* 주차별 그룹 (정렬 순서 유지) */
    var weekOrder = ['1주차','2주차','3주차','4주차','5주차','시험착용기간 초과','날짜 미확인'];
    var weekGroups = {}, weekGroupOrder = [];
    sortedTrialRows.forEach(function(r) {
      var wg = _sichakWeekGroup(r.sichak);
      if (!weekGroups[wg]) { weekGroups[wg] = []; weekGroupOrder.push(wg); }
      weekGroups[wg].push(r);
    });
    /* weekOrder 기준으로 정렬 */
    weekGroupOrder.sort(function(a,b){ return weekOrder.indexOf(a) - weekOrder.indexOf(b); });
    var groupColors = {
      '1주차':           {bg:'#F0FDF4',border:'#86EFAC',txt:'#15803D'},
      '2주차':           {bg:'#FFF7ED',border:'#FED7AA',txt:'#C2410C'},
      '3주차':           {bg:'#FFFBEB',border:'#FDE68A',txt:'#B45309'},
      '4주차':           {bg:'#FEF2F2',border:'#FECACA',txt:'#B91C1C'},
      '5주차':           {bg:'#FDF4FF',border:'#E9D5FF',txt:'#7E22CE'},
      '시험착용기간 초과': {bg:'#1F2937',border:'#374151',txt:'#F9FAFB'},
      '날짜 미확인':      {bg:'#F1F5F9',border:'#CBD5E1',txt:'#64748B'},
    };
    var html = '';
    weekGroupOrder.forEach(function(wg) {
      var gc = groupColors[wg] || {bg:'#F1F5F9',border:'#CBD5E1',txt:'#475569'};
      html += '<tr><td colspan="7" style="background:'+gc.bg+';color:'+gc.txt+';font-weight:700;font-size:8.5px;padding:3px 8px;border-top:1px solid '+gc.border+';border-bottom:1px solid '+gc.border+';">' + wg + '</td></tr>';
      weekGroups[wg].forEach(function(r) {
        html += '<tr>'
          + '<td style="white-space:nowrap;padding-left:14px">' + (r.tad||'—') + '</td>'
          + '<td style="white-space:nowrap">' + (r.sichak||'—') + '</td>'
          + '<td style="white-space:nowrap">' + shortId(r.opp_id) + '</td>'
          + '<td>' + (r.partner||'—') + '</td>'
          + '<td>' + (r.program||'—') + '</td>'
          + '<td>' + (r.left||'—') + '</td>'
          + '<td>' + (r.right||'—') + '</td>'
          + '</tr>';
      });
    });
    return html;
  })();

  /* ── 월 목표 역산 배너 + 통합 퍼널 테이블 헬퍼 ── */
  var MO_LEADS_TGT = Math.round(MO_APPTS / 0.45);
  var MO_ABV_M = MO_SALES > 0 ? Math.round(MO_REV / MO_SALES * 10) / 10 : 2.5;
  var twLeads  = Math.round(twAppts / 0.45);
  var _cumLeads  = (curMb.length ? (curMb[0].leads  || 0) : 0);  /* Leads: monthly_breakdown */
  var _cumAppts  = sThisMon.appts;
  var _cumVisits = sThisMon.visits;
  var _cumOrders = sThisMon.orders;
  var _cumSales  = sThisMon.sales;
  var _cumRevM   = Math.round(sThisMon.rev * 10) / 10;
  function _mpct(a,t){return t>0?Math.min(Math.round(a/t*100),150):0;}
  var _cRevP =_mpct(_cumRevM,  MO_REV);
  var _cSalP =_mpct(_cumSales, MO_SALES);
  var _cOrdP =_mpct(_cumOrders,MO_ORDERS);
  var _cVisP =_mpct(_cumVisits,MO_VISITS);
  var _cAppP =_mpct(_cumAppts, MO_APPTS);
  var _cLdP  =_mpct(_cumLeads, MO_LEADS_TGT);
  var _monLbl=(today.getMonth()+1)+'월';
  function _pCol(p){return p>=100?'#16A34A':p>=85?'#D97706':'#DC2626';}
  function _mbN(lbl,cv,tV,tU,cV,cP){
    var fp=Math.min(cP,100);
    var tS=tU==='M'?Number(tV).toFixed(1)+tU:tV+tU;
    var cS=tU==='M'?Number(cV).toFixed(1)+tU:cV+tU;
    return '<div class="mb-node"><div class="mb-node-label" style="color:'+cv+';">'+lbl+'</div>'
      +'<div class="mb-node-val">'+(tU==='M'?Number(tV).toFixed(1):tV)+'<small>'+tU+'</small></div>'
      +'<div class="mb-prog"><div class="mb-prog-bar"><div class="mb-prog-fill" style="width:'+fp+'%;background:'+cv+';"></div></div>'
      +'<span class="mb-prog-pct" style="color:'+_pCol(cP)+';">'+cP+'%</span></div>'
      +'<div class="mb-sub">누적 '+cS+' / '+tS+'</div></div>';
  }
  function _mbA(aLbl,aVal,cv){
    return '<div class="mb-arrow"><div class="mb-cr-label">'+aLbl+'</div>'
      +'<div class="mb-arrow-line"><div class="mb-arrow-shaft"></div><div class="mb-arrow-head"></div></div>'
      +'<div class="mb-cr-val" style="color:'+cv+';">'+aVal+'</div></div>';
  }
  function _uR(nm,cv,wT,aA,twG,crH,isM){
    var g=isM?Math.round((aA-wT)*10)/10:(aA-wT);
    var p=wT?Math.round(aA/wT*100):0;
    var pc=p>=110?'good':p>=90?'warn':'bad';
    var gA=Math.abs(isM?Math.round(g*10)/10:g);
    var gS=(g>=0?'▲ ':'▼ ')+gA+(isM?'M':'');
    var gC=g>=0?'neg':'pos';
    var dv=Math.round(twG/5*10)/10;
    var u=isM?'M':'건';
    return '<tr class="dr"><td class="bg-last"><div class="stage-name" style="color:'+cv+';">'+nm+'</div></td>'
      +'<td class="nc muted bg-last">'+(isM?wT+'M':wT)+'</td>'
      +'<td class="nc bg-last">'+(isM?aA+'M':aA)+'</td>'
      +'<td class="nc '+gC+' bg-last">'+gS+'</td>'
      +'<td class="bg-last" style="text-align:center;"><span class="fpct '+pc+'">'+p+'%</span></td>'
      +'<td class="sep-col bg-sep"><div class="sep-line"></div></td>'
      +'<td class="nc wk bg-this" style="color:'+cv+';">'+twG+'<span style="font-size:9px;font-weight:400;color:var(--dgray);">'+u+'</span></td>'
      +'<td class="sep-col bg-sep"><div class="sep-line"></div></td>'
      +'<td class="nc daily bg-daily">'+dv+'<span style="font-size:8px;font-weight:400;color:var(--dgray);">'+u+'</span></td>'
      +'</tr>';
  }
  function _ccR(lbl,a,t){
    var ok=a>=t-0.05;
    return '<span class="cr-chip">'+lbl+' '+a.toFixed(1)+'%</span>'
      +'<span class="cr-chip '+(ok?'good':'bad')+'">'+(ok?'목표 '+t+'% 초과 ✓':'목표 '+t+'% 미달')+'</span>';
  }

  var lbl_js      = JSON.stringify(chartLabels);
  var actAppts_js = JSON.stringify(actAppts);
  var actVisits_js= JSON.stringify(actVisits);
  var actOrders_js= JSON.stringify(actOrders);
  var actSales_js = JSON.stringify(actSales);
  var actRevM_js  = JSON.stringify(actRevM);
  var prjAppts_js = JSON.stringify(prjAppts);
  var prjVisits_js= JSON.stringify(prjVisits);
  var prjOrders_js= JSON.stringify(prjOrders);
  var prjSales_js = JSON.stringify(prjSales);
  var prjRevM_js  = JSON.stringify(prjRevM);

  /* ── L3M 기준 인덱스 (isProj 분리) ── */
  var L3M_CNT = l3mMb.length;  /* 과거 solid 개수 (보통 3) */

  var html = '<!DOCTYPE html><html lang="ko"><head>'
    + '<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
    + '<title>주간 성과 리포트 – ' + ccName + '</title>'
    + '<link href="https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@400;500;600;700&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">'
    + '<style>'
    + ':root{--navy:#1B2A4A;--blue:#2563EB;--amber:#D97706;--red:#DC2626;--green:#16A34A;--purple:#7C3AED;--teal:#0D9488;--lgray:#F1F5F9;--mgray:#CBD5E1;--dgray:#64748B;--c-leads:#64748B;--c-appts:#2563EB;--c-visits:#0D9488;--c-orders:#D97706;--c-sales:#7C3AED;--c-rev:#475569;}'
    + '*{box-sizing:border-box;margin:0;padding:0;}'
    + 'body{background:#DDE3EC;font-family:"Noto Sans KR",sans-serif;display:flex;justify-content:center;padding:28px 16px;-webkit-print-color-adjust:exact;print-color-adjust:exact;}'
    + '.page{width:794px;background:#fff;box-shadow:0 4px 32px rgba(0,0,0,.13);display:flex;flex-direction:column;}'
    + '.header{background:var(--navy);padding:12px 18px 10px;display:flex;align-items:center;justify-content:space-between;}'
    + '.header h1{font-size:14px;font-weight:700;color:#fff;letter-spacing:-.3px;}'
    + '.header p{font-size:10px;color:#94A3B8;margin-top:2px;}'
    + '.hbadge{font-size:10.5px;font-weight:600;padding:4px 12px;border-radius:20px;color:#fff;white-space:nowrap;}'
    + '.hbadge.good{background:var(--green);}.hbadge.warn{background:var(--amber);}.hbadge.bad{background:var(--red);}'
    + '.slabel{font-size:9px;font-weight:700;color:var(--navy);letter-spacing:.6px;text-transform:uppercase;border-left:3px solid var(--blue);padding-left:6px;margin-bottom:5px;flex-shrink:0;}'
    /* monthly banner */
    + '.monthly-banner{background:#F8FAFC;border-bottom:1px solid var(--mgray);padding:9px 18px 8px;}'
    + '.mb-title{font-size:9px;font-weight:700;color:var(--navy);letter-spacing:.5px;text-transform:uppercase;border-left:3px solid var(--blue);padding-left:6px;margin-bottom:7px;}'
    + '.mb-flow{display:flex;align-items:stretch;gap:0;}'
    + '.mb-node{display:flex;flex-direction:column;align-items:center;flex:1;min-width:0;background:#fff;border:1px solid var(--mgray);border-radius:6px;padding:6px 4px 5px;}'
    + '.mb-node-label{font-size:8px;font-weight:700;margin-bottom:2px;}'
    + '.mb-node-val{font-size:15px;font-weight:700;font-family:"DM Mono",monospace;line-height:1;color:var(--navy);}'
    + '.mb-node-val small{font-size:8.5px;font-weight:400;color:var(--dgray);}'
    + '.mb-prog{display:flex;align-items:center;gap:3px;margin-top:4px;width:100%;}'
    + '.mb-prog-bar{flex:1;height:3px;background:#E2E8F0;border-radius:2px;overflow:hidden;}'
    + '.mb-prog-fill{height:100%;border-radius:2px;max-width:100%;}'
    + '.mb-prog-pct{font-size:7.5px;font-weight:700;white-space:nowrap;}'
    + '.mb-sub{font-size:7px;color:var(--dgray);margin-top:2px;}'
    + '.mb-arrow{display:flex;flex-direction:column;align-items:center;justify-content:center;width:44px;flex-shrink:0;gap:2px;padding:0 2px;}'
    + '.mb-arrow-line{display:flex;align-items:center;width:100%;}'
    + '.mb-arrow-shaft{flex:1;height:1.5px;background:var(--mgray);}'
    + '.mb-arrow-head{width:0;height:0;border-top:4px solid transparent;border-bottom:4px solid transparent;border-left:6px solid var(--mgray);flex-shrink:0;}'
    + '.mb-cr-label{font-size:7px;font-weight:600;color:var(--dgray);white-space:nowrap;text-align:center;line-height:1.2;}'
    + '.mb-cr-val{font-size:8px;font-weight:700;white-space:nowrap;text-align:center;}'
    /* unified table */
    + '.row2{padding:10px 18px 0;}'
    + '.unified-wrap{background:var(--lgray);border-radius:6px;padding:8px 10px 8px;}'
    + '.utbl{width:100%;border-collapse:collapse;}'
    + '.utbl .grp-hd td{padding:3px 6px;font-size:7.5px;font-weight:700;color:#fff;text-align:center;}'
    + '.grp-last{background:var(--navy)!important;}.grp-this{background:var(--blue)!important;}.grp-daily{background:#0D9488!important;}.grp-blank{background:transparent!important;}'
    + '.utbl .col-hd td{font-size:7.5px;font-weight:600;color:var(--dgray);text-align:center;padding:3px 5px 4px;border-bottom:1.5px solid var(--mgray);white-space:nowrap;}'
    + '.utbl .col-hd td.left{text-align:left;}'
    + '.utbl .dr td{padding:4.5px 5px;vertical-align:middle;border-bottom:.5px solid #DDE5EF;}'
    + '.utbl .dr:last-child td{border-bottom:none;}'
    + '.sep-col{width:8px!important;padding:0!important;}.sep-line{width:1px;background:var(--mgray);margin:0 auto;height:100%;min-height:18px;}'
    + '.stage-name{font-size:10px;font-weight:700;line-height:1.2;}'
    + '.nc{text-align:center;font-family:"DM Mono",monospace;font-size:10.5px;color:var(--navy);}'
    + '.nc.muted{color:var(--dgray);font-size:10px;}.nc.pos{color:var(--red);font-size:10px;}.nc.neg{color:var(--green);font-size:10px;}'
    + '.nc.wk{font-size:13px;font-weight:700;text-align:center;}.nc.daily{font-size:11px;font-weight:700;color:#0D9488;text-align:center;}'
    + '.fpct{display:inline-block;font-size:8.5px;font-weight:700;padding:2px 5px;border-radius:4px;white-space:nowrap;}'
    + '.fpct.good{background:#DCFCE7;color:#15803D;}.fpct.warn{background:#FEF3C7;color:#B45309;}.fpct.bad{background:#FEE2E2;color:#B91C1C;}'
    + '.cr-chip{display:inline-block;font-size:7px;color:var(--dgray);background:#E0E8F4;border-radius:3px;padding:1px 5px;white-space:nowrap;margin-right:3px;}'
    + '.cr-chip.good{background:#DCFCE7;color:#15803D;}.cr-chip.bad{background:#FEE2E2;color:#991B1B;}'
    + '.bg-last{background:#F8FAFC!important;}.bg-this{background:#EFF6FF!important;}.bg-daily{background:#F0FDFA!important;}.bg-sep{background:#fff!important;}'
    /* diag */
    + '.row3{display:grid;grid-template-columns:1fr 1fr;gap:8px;padding:8px 18px 0;}'
    + '.diag-wrap{display:flex;flex-direction:column;}'
    + '.diag-card{border-radius:6px;padding:8px 10px;flex:1;}'
    + '.diag-card.warn{background:#FFFBEB;border:1px solid #FCD34D;}.diag-card.good{background:#F0FDF4;border:1px solid #86EFAC;}'
    + '.diag-ttl{font-size:10px;font-weight:700;margin-bottom:5px;}'
    + '.diag-ttl.warn{color:#B45309;}.diag-ttl.good{color:#15803D;}'
    + '.focus-table,.good-table{width:100%;border-collapse:collapse;}'
    + '.focus-table th{font-size:7.5px;font-weight:600;color:#B45309;text-align:left;padding:0 4px 3px;border-bottom:1px solid #FCD34D;}'
    + '.good-table th{font-size:7.5px;font-weight:600;color:#15803D;text-align:left;padding:0 4px 3px;border-bottom:1px solid #86EFAC;}'
    + '.focus-table td{font-size:9px;color:var(--navy);padding:3px 4px;border-bottom:.5px solid #FEF3C7;vertical-align:middle;}'
    + '.good-table td{font-size:9px;color:var(--navy);padding:3.5px 4px;border-bottom:.5px solid #D1FAE5;vertical-align:middle;}'
    + '.focus-table tr:last-child td,.good-table tr:last-child td{border-bottom:none;}'
    + '.rank-badge{display:inline-flex;align-items:center;justify-content:center;width:14px;height:14px;border-radius:50%;font-size:8px;font-weight:700;color:#fff;background:var(--amber);flex-shrink:0;}'
    + '.rank-badge.r1{background:#DC2626;}.rank-badge.r2{background:var(--amber);}.rank-badge.r3{background:#64748B;}'
    + '.star-badge{display:inline-flex;align-items:center;justify-content:center;width:14px;height:14px;border-radius:50%;font-size:7.5px;font-weight:700;color:#fff;background:#16A34A;flex-shrink:0;}'
    + '.ach-pill{display:inline-block;padding:1px 5px;border-radius:4px;font-size:8px;font-weight:700;background:#DCFCE7;color:#15803D;white-space:nowrap;}'
    /* chart */
    + '.row4{padding:8px 18px 0;}'
    + '.chart-card{background:var(--lgray);border-radius:6px;padding:8px 10px 8px;}'
    + '.chart-legend{display:flex;flex-wrap:wrap;gap:5px 12px;margin-bottom:5px;}'
    + '.leg-item{display:flex;align-items:center;gap:3px;font-size:7.5px;}'
    + '.leg-line{width:14px;height:2px;display:inline-block;}.leg-bar{width:10px;height:9px;display:inline-block;border-radius:1px;}'
    /* pipeline */
    + '.row5{padding:8px 18px 0;flex:1;}'
    + '.pipe-sub-title{font-size:8.5px;font-weight:700;margin-bottom:4px;display:flex;align-items:center;gap:5px;}'
    + '.pipe-count{font-size:8px;padding:1px 6px;border-radius:8px;font-weight:600;}'
    + '.pipe-table{width:100%;border-collapse:collapse;}'
    + '.pipe-table th{font-size:8px;font-weight:600;color:var(--navy);text-align:left;padding:3px 5px;background:#EEF3FA;border-bottom:1px solid var(--mgray);white-space:nowrap;}'
    + '.pipe-table td{font-size:9px;padding:3.5px 5px;color:var(--navy);border-bottom:.5px solid #F1F5F9;vertical-align:middle;}'
    + '.pipe-table tr:last-child td{border-bottom:none;}'
    + '.pipe-table tr:nth-child(even) td{background:#FAFCFE;}'
    + '.pipe-table tr.date-group td{background:#EFF6FF!important;}'
    /* footer */
    + '.footer{margin-top:8px;background:var(--lgray);padding:5px 18px;display:flex;justify-content:space-between;}'
    + '.footer p{font-size:8px;color:var(--dgray);}'
    + '.print-btn{position:fixed;bottom:20px;right:20px;background:var(--navy);color:#fff;border:none;padding:10px 18px;border-radius:8px;font-size:13px;font-weight:600;cursor:pointer;z-index:999;box-shadow:0 4px 14px rgba(0,0,0,.25);}'
    + '@media print{body{background:none;padding:0;}.page{box-shadow:none;width:100%;}.print-btn{display:none;}@page{size:A4 portrait;margin:8mm;}}'
    + '</style></head><body>'
    + '<button class="print-btn" onclick="__printReport()">🖨️ PDF 저장</button>'
    + '<script>function __printReport(){'
    +   'var y=String(' + lastMon.getFullYear() + ').slice(2);'
    +   'var m=String(' + (lastMon.getMonth()+1) + ').padStart(2,"0");'
    +   'var wn=Math.ceil(' + lastMon.getDate() + '/7);'
    +   'var orig=document.title;'
    +   'document.title=y+m+" "+wn+"주차_주간리포트_' + ccName + '";'
    +   'window.print();'
    +   'setTimeout(function(){document.title=orig;},1000);'
    + '}<\/script>'
    + '<div class="page">'
    /* ── HEADER ── */
    + '<div class="header">'
    + '<div><h1>주간 성과 리포트</h1>'
    + '<p>상담사: <strong style="color:#fff">' + ccName + '</strong>'
    + ' <span style="color:#CBD5E1">(' + (pip ? (pip.team_display||pip.team||'—') : '—') + ')</span>'
    + ' &nbsp;|&nbsp; 지난주: ' + lastWeekLabel
    + ' &nbsp;|&nbsp; 발행일: ' + issueDate + '</p></div>'
    + '<div class="hbadge ' + overallCls + '">지난주 종합 ' + overallPct + '% 달성</div>'
    + '</div>'
    /* ── SECTION 1: 이번달 월 목표 역산 배너 ── */
    + '<div class="monthly-banner">'
    + '<div class="mb-title">' + _monLbl + ' 월 목표</div>'
    + '<div class="mb-flow">'
    + _mbN('Revenue','var(--c-rev)',MO_REV,'M',_cumRevM,_cRevP)
    + _mbA('ABV',MO_ABV_M.toFixed(1)+'M','var(--c-rev)')
    + _mbN('Sales','var(--c-sales)',MO_SALES,'건',_cumSales,_cSalP)
    + _mbA('SR',srTgt+'%','var(--c-sales)')
    + _mbN('Orders','var(--c-orders)',MO_ORDERS,'건',_cumOrders,_cOrdP)
    + _mbA('OR',orTgt+'%','var(--c-orders)')
    + _mbN('Visits','var(--c-visits)',MO_VISITS,'건',_cumVisits,_cVisP)
    + _mbA('VR',vrTgt+'%','var(--c-visits)')
    + _mbN('Appts','var(--c-appts)',MO_APPTS,'건',_cumAppts,_cAppP)
    + _mbA('CR2',cr2Tgt+'%','var(--c-appts)')
    + _mbN('Leads','var(--c-leads)',MO_LEADS_TGT,'건',_cumLeads,_cLdP)
    + '</div></div>'
    /* ── SECTION 2: 지난주 퍼널 달성률 · 이번주 목표 · 일 평균 통합 테이블 ── */
    + '<div class="row2"><div class="slabel">지난주 퍼널 달성률 &nbsp;·&nbsp; 이번주 목표 &nbsp;·&nbsp; 일 평균</div>'
    + '<div class="unified-wrap"><table class="utbl">'
    + '<colgroup>'
    + '<col style="width:58px"><col style="width:42px"><col style="width:42px"><col style="width:44px"><col style="width:40px">'
    + '<col style="width:8px"><col style="width:52px"><col style="width:8px"><col style="width:52px">'
    + '</colgroup>'
    + '<tr class="grp-hd">'
    + '<td class="grp-blank" colspan="1"></td>'
    + '<td class="grp-last"  colspan="4">지난주 실적</td>'
    + '<td class="grp-blank bg-sep"></td>'
    + '<td class="grp-this"  colspan="1">이번주 목표</td>'
    + '<td class="grp-blank bg-sep"></td>'
    + '<td class="grp-daily" colspan="1">일 평균</td>'
    + '</tr>'
    + '<tr class="col-hd">'
    + '<td class="left">단계</td><td>목표</td><td>실적</td><td>갭</td><td>달성률</td>'
    + '<td class="sep-col bg-sep"></td><td>주간 목표</td><td class="sep-col bg-sep"></td><td>일 평균</td>'
    + '</tr>'
    + _uR('Leads','var(--c-leads)',W_LEADS,aLeads,twLeads,_ccR('CR2',cr2Act,cr2Tgt),false)
    + _uR('Appts','var(--c-appts)',W_APPTS,aAppts,twAppts,_ccR('VR',vrAct,vrTgt),false)
    + _uR('Visits','var(--c-visits)',W_VISITS,aVisits,twVisits,_ccR('OR',orAct,orTgt),false)
    + _uR('Orders','var(--c-orders)',W_ORDERS,aOrders,twOrders,_ccR('SR',srAct,srTgt),false)
    + _uR('Sales','var(--c-sales)',W_SALES,aSales,twSales,'<span class="cr-chip">ABV ₩'+abvAct.toFixed(1)+'M</span>',false)
    + _uR('Revenue','var(--c-rev)',W_REV,aRev,twRev,'<span style="font-size:7.5px;color:var(--dgray);">월 잔여 '+Math.max(0,Math.round((MO_REV-_cumRevM)*10)/10)+'M · '+remainWeeks+'주 남음</span>',true)
    + '</table></div></div>'
    /* ── SECTION 3: 집중 포인트 + 잘하고 있는 점 ── */
    + '<div class="row3">'
    + '<div class="diag-wrap"><div class="slabel">이번주 집중 포인트</div><div class="diag-card warn">'
    + '<div class="diag-ttl warn">▲ 개선 필요 KPI (우선순위 순)</div>'
    + (focuses.length
        ? '<table class="focus-table"><thead><tr><th style="width:22px;">순위</th><th style="width:50px;">KPI</th><th>지난주 달성률</th><th>개선 포인트</th></tr></thead><tbody>'
          + focuses.map(function(it,i){
              var badge=['r1','r2','r3'][i]||'r3';
              var fc=it.label==='Orders'?'시착 어필':it.label==='ABV'?'피칭 - 성능 강조':it.label==='Visits'?'노쇼를 방문으로':it.label==='CR2'?'리드 퀄리티 점검':it.label==='VR'?'방문 확정 리마인드':it.label==='OR'?'시착 전환 강화':it.label==='SR'?'제안 설득력 강화':it.label==='Revenue'?'고단가 제품 제안':it.label==='Sales'?'클로징 집중':'—';
              return '<tr><td><span class="rank-badge '+badge+'">'+(i+1)+'</span></td><td style="font-weight:700;">'+it.label+'</td><td>'+pctStr(it)+'</td><td style="color:#92400E;font-size:8.5px;">'+fc+'</td></tr>';
            }).join('')
          + '</tbody></table>'
        : '<div style="font-size:9px;color:var(--green);padding:4px 0;">모든 지표 목표 달성 중 ✓</div>')
    + '</div></div>'
    + '<div class="diag-wrap"><div class="slabel">잘하고 있는 점</div><div class="diag-card good">'
    + '<div class="diag-ttl good">✓ 강점 지표 (달성률 상위)</div>'
    + (strengths.length
        ? '<table class="good-table"><thead><tr><th style="width:22px;">순위</th><th style="width:50px;">KPI</th><th>지난주 달성률</th><th>비고</th></tr></thead><tbody>'
          + strengths.map(function(it,i){
              return '<tr><td><span class="star-badge">'+(i+1)+'</span></td><td style="font-weight:700;">'+it.label+'</td><td><span class="ach-pill">'+pctStr(it)+'</span></td><td style="font-size:8.5px;color:var(--dgray);">목표 대비 +'+(it.p-100)+'%</td></tr>';
            }).join('')
          + '</tbody></table>'
        : '<div style="font-size:9px;color:var(--dgray);padding:4px 0;">목표 110% 초과 지표 없음</div>')
    + '</div></div>'
    + '</div>'
    /* ── SECTION 4: 월별 성과 추이 ── */
    + '<div class="row4"><div class="slabel">월별 성과 추이 (과거 3개월 + 현재 + 미래 예측)</div>'
    + '<div class="chart-card">'
    + '<div class="chart-legend">'
    + '<span class="leg-item" style="color:#2563EB;"><span class="leg-line" style="background:#2563EB;"></span>Appts</span>'
    + '<span class="leg-item" style="color:#0D9488;"><span class="leg-line" style="background:#0D9488;"></span>Visits</span>'
    + '<span class="leg-item" style="color:#D97706;"><span class="leg-line" style="background:#D97706;"></span>Orders</span>'
    + '<span class="leg-item" style="color:#7C3AED;"><span class="leg-line" style="background:#7C3AED;"></span>Sales</span>'
    + '<span class="leg-item" style="color:#64748B;"><span class="leg-bar" style="background:rgba(100,116,139,.3);"></span>Revenue(우축)</span>'
    + '<span class="leg-item" style="color:#94A3B8;"><span class="leg-line" style="background:transparent;border-top:1.5px dashed #94A3B8;height:0;"></span>&nbsp;목표</span>'
    + '<span class="leg-item" style="color:#94A3B8;"><span class="leg-line" style="background:rgba(100,100,200,.35);"></span>&nbsp;예측(연한색)</span>'
    + '</div>'
    + '<div style="position:relative;width:100%;height:160px;"><canvas id="rptChart"></canvas></div>'
    + '<div style="display:flex;justify-content:space-between;margin-top:2px;padding:0 2px;">'
    + '<span style="font-size:7px;color:#94A3B8;">좌축: 건수</span><span style="font-size:7px;color:#94A3B8;">우축: 매출 (M)</span>'
    + '</div>'
    + '</div></div>'
    /* ── SECTION 5: 파이프라인 ── */
    + '<div class="row5">'
    + '<div class="slabel">현재 파이프라인</div>'
    + '<div class="pipe-sub-title" style="color:#1D4ED8;">● 방문 예정 <span class="pipe-count" style="background:#DBEAFE;color:#1D4ED8;">' + upcomingRows.length + '건</span></div>'
    + '<table class="pipe-table"><thead><tr><th>시간</th><th>기회 ID</th><th>파트너 청각사</th></tr></thead>'
    + '<tbody>' + upcomingHtml + '</tbody></table>'
    + '<div class="pipe-sub-title" style="color:#065F46;margin-top:10px;">● 시험착용 진행 중 <span class="pipe-count" style="background:#D1FAE5;color:#065F46;">' + sortedTrialRows.length + '건</span></div>'
    + '<table class="pipe-table"><thead><tr><th>동의서 작성일</th><th>시착일 ▲</th><th>기회 ID</th><th>파트너 청각사</th><th>시착/구매 프로그램</th><th>보청기(좌)</th><th>보청기(우)</th></tr></thead>'
    + '<tbody>' + trialHtml + '</tbody></table>'
    + '</div>'
    /* ── FOOTER ── */
    + '<div class="footer"><p>본 리포트는 자동 생성됩니다</p><p>발행: ' + issueDate + ' &nbsp;|&nbsp; ' + ccName + ' 상담사 전용</p></div>'
    + '</div>'
    /* ── Chart.js + chartjs-plugin-datalabels ── */
    + '<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"><\/script>'
    + '<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.2.0/dist/chartjs-plugin-datalabels.min.js"><\/script>'
    + '<script>'
    + '(function(){'
    + 'Chart.register(ChartDataLabels);'
    + 'var L3M_CNT=' + L3M_CNT + ';'
    + 'var LABELS='     + lbl_js      + ';'
    + 'var ACT_APPTS='  + actAppts_js + ';'
    + 'var ACT_VISITS=' + actVisits_js+ ';'
    + 'var ACT_ORDERS=' + actOrders_js+ ';'
    + 'var ACT_SALES='  + actSales_js + ';'
    + 'var ACT_REVM='   + actRevM_js  + ';'
    + 'var PRJ_APPTS='  + prjAppts_js + ';'
    + 'var PRJ_VISITS=' + prjVisits_js+ ';'
    + 'var PRJ_ORDERS=' + prjOrders_js+ ';'
    + 'var PRJ_SALES='  + prjSales_js + ';'
    + 'var PRJ_REVM='   + prjRevM_js  + ';'
    + 'var TGT_APPTS=' + tgtAppts + ',TGT_VISITS=' + tgtVisits + ',TGT_ORDERS=' + tgtOrders + ',TGT_SALES=' + tgtSales + ',TGT_REVM=' + tgtRevM + ';'
    + 'var CC={appts:"#2563EB",visits:"#0D9488",orders:"#D97706",sales:"#7C3AED"};'
    /* 실적 라인 — 데이터 레이블 상시 표시 (PDF용), 겹치면 자동 숨김 */
    + 'function mkActLine(data,key){'
    +   'var col=CC[key];'
    +   'return{type:"line",label:key,data:data,yAxisID:"y1",'
    +   'borderColor:col,backgroundColor:col,borderWidth:2,pointRadius:3.5,'
    +   'pointBackgroundColor:col,spanGaps:false,tension:0.3,fill:false,'
    +   'datalabels:{color:col,font:{size:7,weight:"700"},anchor:"end",align:"top",offset:2,'
    +   'display:"auto",'
    +   'formatter:function(v){return v!==null?String(v):null;}}};}'
    /* 예측 라인 — 레이블 없음 */
    + 'function mkPrjLine(data,key){'
    +   'var col=CC[key];'
    +   'return{type:"line",label:key+"_prj",data:data,yAxisID:"y1",'
    +   'borderColor:col+"70",backgroundColor:col+"30",borderWidth:1.5,'
    +   'borderDash:[5,3],pointRadius:3,pointBackgroundColor:col+"60",'
    +   'spanGaps:false,tension:0.3,fill:false,'
    +   'datalabels:{display:false}};}'
    /* 목표 점선 — 레이블 없음 */
    + 'function mkTgt(val,key){'
    +   'var col=CC[key];'
    +   'return{type:"line",label:key+"_t",data:LABELS.map(function(){return val;}),'
    +   'yAxisID:"y1",borderColor:col+"55",backgroundColor:"transparent",'
    +   'borderWidth:1.2,borderDash:[4,3],pointRadius:0,fill:false,'
    +   'datalabels:{display:false}};}'
    /* Revenue 바 */
    + 'var revBarAct={type:"bar",label:"rev_act",data:ACT_REVM,yAxisID:"y2",'
    +   'backgroundColor:ACT_REVM.map(function(_,i){return i<L3M_CNT?"rgba(100,116,139,.45)":"rgba(100,116,139,.3)";})'
    +   ',borderColor:"rgba(100,116,139,.7)",borderWidth:1,borderRadius:3,'
    +   'datalabels:{color:"#475569",font:{size:7,weight:"700"},anchor:"end",align:"top",offset:2,'
    +   'display:function(ctx){return ctx.parsed.y!==null&&ctx.parsed.y>0;},'
    +   'formatter:function(v){return v+"M";}}};'
    + 'var revBarPrj={type:"bar",label:"rev_prj",data:PRJ_REVM,yAxisID:"y2",'
    +   'backgroundColor:"rgba(100,116,139,.15)",borderColor:"rgba(100,116,139,.3)",'
    +   'borderWidth:1,borderRadius:3,datalabels:{display:false}};'
    + 'var revTgt={type:"line",label:"rev_t",data:LABELS.map(function(){return TGT_REVM;}),'
    +   'yAxisID:"y2",borderColor:"rgba(100,116,139,.55)",backgroundColor:"transparent",'
    +   'borderWidth:1.2,borderDash:[4,3],pointRadius:0,fill:false,datalabels:{display:false}};'
    + 'var ctx=document.getElementById("rptChart").getContext("2d");'
    /* y1 최대값: 실제 라인 데이터(act+prj) 기반 — 목표선이 축 비율을 왜곡하지 않도록 */
    + 'var _allLine=[].concat(ACT_APPTS,ACT_VISITS,ACT_ORDERS,ACT_SALES,PRJ_APPTS,PRJ_VISITS,PRJ_ORDERS,PRJ_SALES);'
    + 'var _lineMax=Math.max.apply(null,_allLine.filter(function(v){return v!=null&&v>0;})||[0]);'
    + 'var _y1SugMax=_lineMax>0?Math.ceil(_lineMax*1.3):20;'
    /* mkTgt 목표선 제거: yAxisID:"y1" 에 169건 등 고정값이 있으면 y1 축이 250건까지
       강제 확장되어 실제 데이터(160건, 90건 등)도 바닥에 붙어 보이는 문제 해결 */
    + 'new Chart(ctx,{type:"bar",data:{labels:LABELS,datasets:['
    +   'revBarAct,revBarPrj,revTgt,'
    +   'mkActLine(ACT_APPTS,"appts"),mkActLine(ACT_VISITS,"visits"),mkActLine(ACT_ORDERS,"orders"),mkActLine(ACT_SALES,"sales"),'
    +   'mkPrjLine(PRJ_APPTS,"appts"),mkPrjLine(PRJ_VISITS,"visits"),mkPrjLine(PRJ_ORDERS,"orders"),mkPrjLine(PRJ_SALES,"sales")'
    + ']},options:{responsive:true,maintainAspectRatio:false,layout:{padding:{top:14}},interaction:{mode:"index",intersect:false},'
    + 'plugins:{'
    + 'legend:{display:false},'
    /* global datalabels 설정 제거 — 각 dataset에서 개별 제어 (PDF 출력용 상시 레이블) */
    + 'tooltip:{callbacks:{label:function(c){'
    +   'var k=c.dataset.label;if(!k||k.endsWith("_t")||k.endsWith("_prj"))return null;'
    +   'return " "+k+": "+(k==="rev_act"?"₩":"")+c.parsed.y+(k==="rev_act"?"M":"건");'
    + '}}}},'
    + 'scales:{x:{ticks:{font:{size:8.5},color:function(ctx){return ctx.index>=L3M_CNT?"#94A3B8":"#475569";}},grid:{display:false}},'
    + 'y1:{type:"linear",position:"left",min:0,suggestedMax:_y1SugMax,ticks:{font:{size:8},color:"#64748B",callback:function(v){return v+"건";}},grid:{color:"#E8EEF4",lineWidth:.7}},'
    + 'y2:{type:"linear",position:"right",min:0,ticks:{font:{size:8},color:"#94A3B8",callback:function(v){return v+"M";}},grid:{display:false}}}'
    + '}}); })();<\/script>'
    + '</body></html>';

  var w = window.open('', '_blank', 'width=860,height=1180');
  if (!w) { alert('팝업이 차단되었습니다. 브라우저 팝업 허용 후 다시 시도하세요.'); return; }
  w.document.write(html);
  w.document.close();
}

function bindPipTableSort(tableId, which) {
  var table = document.getElementById(tableId);
  if (!table || table._pipSortBound) return;
  table._pipSortBound = true;
  table.querySelectorAll('thead th[data-pip-sort]').forEach(function(th) {
    th.addEventListener('click', function() {
      var key = th.getAttribute('data-pip-sort');
      var st = _pipSortState[which] || { col: null, dir: 1 };
      if (st.col === key) st.dir = -st.dir;
      else { st.col = key; st.dir = 1; }
      _pipSortState[which] = st;
      var base = which === 'main' ? (getPayload().pip || []) : (getPayload().pip_at_risk || []);
      var sorted = base.slice().sort(function(a, b) { return pipCompareRows(a, b, key, st.dir); });
      renderPipTableRows(sorted, which === 'main' ? 'pip-table-body' : 'pip-risk-body');
    });
  });
}
function escapePlCell(s) {
  return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');
}
function buildPipelineRowsHtml(rows) {
  if (!rows || !rows.length) return '<tr><td colspan="8" style="color:#999">Trials 행 없음 (보청기 좌·우 값이 있는 기회만 표시)</td></tr>';
  return rows.map(function(r) {
    var note = r.notes != null ? r.notes : '—';
    return '<tr><td style="text-align:left">' + escapePlCell(r.opp_id) + '</td>'
      + '<td style="white-space:nowrap">' + escapePlCell(r.tad || '—') + '</td>'
      + '<td style="white-space:nowrap">' + escapePlCell(r.sichak) + '</td>'
      + '<td style="text-align:left">' + escapePlCell(r.partner) + '</td>'
      + '<td style="text-align:left">' + escapePlCell(r.program) + '</td>'
      + '<td style="text-align:left">' + escapePlCell(r.left) + '</td>'
      + '<td style="text-align:left">' + escapePlCell(r.right) + '</td>'
      + '<td style="text-align:left;max-width:220px">' + escapePlCell(note) + '</td></tr>';
  }).join('');
}
function bindPipelineDetailSort(initialRows) {
  var table = document.getElementById('pip-pl-table');
  if (!table) return;
  var state = { col: null, dir: 1 };
  table.querySelectorAll('thead th[data-pl-sort]').forEach(function(th) {
    th.addEventListener('click', function() {
      var key = th.getAttribute('data-pl-sort');
      if (state.col === key) state.dir = -state.dir;
      else { state.col = key; state.dir = 1; }
      var arr = initialRows.slice();
      arr.sort(function(a, b) {
        var va = a[key] != null ? a[key] : '', vb = b[key] != null ? b[key] : '';
        return state.dir * String(va).localeCompare(String(vb), 'ko');
      });
      var tb = table.querySelector('tbody');
      if (tb) tb.innerHTML = buildPipelineRowsHtml(arr);
    });
  });
}
function pipdGi(id) { return document.getElementById(id); }
function pipdGv(id) { var e = pipdGi(id); return parseFloat(e && e.value) || 0; }
function pipdCalTargets() {
  var C = window.__pipDetailCtx;
  var mg = (C && C.monthly_goals) || {};
  var td = (C && C.td) || {};
  var gv = pipdGv;
  /* 고정 월별 목표 (70M 기준, 20 영업일 가정) */
  var BDAYS = 20;
  var mAppts  = 169;
  var mVisits = 93;
  var mOrders = 56;
  var mSales  = 28;
  var mRev = gv('pipd-g-rev') || td.rev_m || 70;
  var mABV = gv('t-abv') || (C && C.td && C.td.abv_m) || 2.5;
  var tVr = gv('t-vr') || td.vr || 55;
  var tOr = gv('t-or') || td.or_ || 60;
  var tSr = gv('t-sr') || td.sr || 50;
  return {
    appts:  Math.round(mAppts  / BDAYS),   /* 8 */
    visits: Math.round(mVisits / BDAYS),   /* 5 */
    orders: Math.round(mOrders / BDAYS),   /* 3 */
    sales:  Math.max(1, Math.round(mSales / BDAYS)),  /* 1 */
    rev:    Math.round((mRev / BDAYS) * 10) / 10,     /* 3.5 */
    bin: mg.bin != null ? mg.bin : 1.8,
    mAppts:  mAppts,
    mVisits: mVisits,
    mOrders: mOrders,
    mSales:  mSales,
    mRev:    mRev,
    mABV: mABV,
    tVr: tVr,
    tOr: tOr,
    tSr: tSr,
    pipelineTrials: mg.pipeline_trials != null ? mg.pipeline_trials : (C && C.trialsNow) || 0
  };
}
function pipdPairCls(a, t, isPct) {
  if (isPct) return (a != null && a >= t - 0.05) ? 'h' : 'm';
  return a >= t ? 'h' : 'm';
}
function pipdFutureHint() {
  var C = window.__pipDetailCtx;
  if (!C) return { text: '목표 향해 집중 필요', cls: 'st-warn' };
  var act = C.act || {}, td = C.td || {};
  var gaps = [];
  if ((td.vr  || 55) > 0) gaps.push({ key: 'VR',  gap: ((td.vr  || 55)  - (act.vr  || 0)) / (td.vr  || 55) });
  if ((td.or_ || 60) > 0) gaps.push({ key: 'OR',  gap: ((td.or_ || 60)  - (act.or_ || 0)) / (td.or_ || 60) });
  if ((td.sr  || 50) > 0) gaps.push({ key: 'SR',  gap: ((td.sr  || 50)  - (act.sr  || 0)) / (td.sr  || 50) });
  if ((td.cr2 || 45) > 0) gaps.push({ key: 'CR2', gap: ((td.cr2 || 45)  - (act.cr2 || 0)) / (td.cr2 || 45) });
  if (!gaps.length) return { text: '지속 유지 집중', cls: 'st-warn' };
  gaps.sort(function(a, b) { return b.gap - a.gap; });
  var worst = gaps[0];
  var hints = {
    'VR':  'VR 집중 — 방문 확인 콜 강화 필요',
    'OR':  'OR 집중 — 시착 전환 어필 강화 필요',
    'SR':  'SR 집중 — 클로징 스크립트 강화 필요',
    'CR2': 'CR2 집중 — 예약 전환 개선 필요',
  };
  return { text: hints[worst.key] || '목표 향해 집중 필요', cls: 'st-warn' };
}
function pipdDayComment(ap, v, vd, o, s, rev, tAp, tVr, tOr, tSr, tRev, tSales) {
  var vr = vd > 0 ? (v / vd) * 100 : null;
  var ord = v > 0 ? (o / v) * 100 : null;
  var sr = o > 0 ? (s / o) * 100 : null;
  var apOk = ap >= tAp;
  var vrOk = vr != null && vr >= tVr - 0.1;
  var orOk = ord != null && ord >= tOr - 0.1;
  var srOk = sr != null && sr >= tSr - 0.1;
  var salOk = s >= tSales;
  var revOk = rev >= tRev - 1e-6;
  if (apOk && vrOk && orOk && srOk && salOk && revOk) return { text: '목표 달성 ✔', cls: 'st-ok' };
  if (!apOk) return { text: 'Appts 부족', cls: 'st-bad' };
  if (apOk && vr != null && !vrOk) return { text: 'Appts 충족 → Visits 부족 (방문율 ↑ 필요)', cls: 'st-warn' };
  if (vrOk && ord != null && !orOk) return { text: 'Visits 충족 → OR 부족 (주문율 ↑ 필요)', cls: 'st-warn' };
  if (orOk && sr != null && !srOk) return { text: 'Orders 충족 → SR 부족 (전환 ↑ 필요)', cls: 'st-warn' };
  if (srOk && !salOk) return { text: '전환 충족 → Sales 부족', cls: 'st-warn' };
  if (salOk && !revOk) return { text: 'Sales 달성 → Revenue 부족', cls: 'st-warn' };
  return { text: '분발 필요', cls: 'st-warn' };
}
function pipdMaybeRefreshCal() {
  if (pipdCalDetailMode) { pipdRenderDetailTable(); return; }
  pipdRenderCal();
}
function pipdToggleCalDetail() {
  pipdCalDetailMode = !pipdCalDetailMode;
  var main = pipdGi('pipd-cal-panel-main');
  var det = pipdGi('pipd-cal-panel-detail');
  var btn = pipdGi('pipd-btn-cal-toggle');
  if (main) main.style.display = pipdCalDetailMode ? 'none' : 'block';
  if (det) det.style.display = pipdCalDetailMode ? 'block' : 'none';
  if (btn) btn.textContent = pipdCalDetailMode ? '캘린더 보기' : '세부내용 보기';
  if (pipdCalDetailMode) { pipdFillDetailPeriod(); pipdRenderDetailTable(); }
  else pipdRenderCal();
}
function pipdNormalizeMonthInput(mk) {
  if (!mk) return '';
  var p = String(mk).split('-');
  if (p.length < 2) return '';
  return p[0] + '-' + String(parseInt(p[1], 10)).padStart(2, '0');
}
function pipdParseMonthStr(s) {
  if (!s) return null;
  var p = String(s).split('-');
  if (p.length < 2) return null;
  var y = parseInt(p[0], 10), m = parseInt(p[1], 10);
  if (isNaN(y) || isNaN(m)) return null;
  return { y: y, m: m };
}
function pipdMonthCmp(a, b) {
  if (!a || !b) return 0;
  return (a.y * 12 + a.m) - (b.y * 12 + b.m);
}
function pipdFillDetailPeriod() {
  var fr = pipdGi('pipd-detail-from');
  var to = pipdGi('pipd-detail-to');
  if (!fr || !to) return;
  var C = window.__pipDetailCtx;
  var mon = (C && C.calendar_monitor_months) || [];
  var first = mon.length ? pipdNormalizeMonthInput(mon[0]) : '';
  var last = mon.length ? pipdNormalizeMonthInput(mon[mon.length - 1]) : '';
  fr.min = first;
  fr.max = last;
  to.min = first;
  to.max = last;
  var want = pipdCalYear + '-' + String(pipdCalMonth + 1).padStart(2, '0');
  function inRange(v) {
    if (!v || !first || !last) return false;
    return v >= first && v <= last;
  }
  var curF = fr.value;
  var curT = to.value;
  if (!inRange(curF) || !inRange(curT)) {
    fr.value = want;
    to.value = want;
  } else if (pipdMonthCmp(pipdParseMonthStr(curF), pipdParseMonthStr(curT)) > 0) {
    to.value = curF;
  }
}
function pipdDetailMonthChanged() {
  var fr = pipdGi('pipd-detail-from');
  var to = pipdGi('pipd-detail-to');
  if (!fr || !to) return;
  var a = pipdParseMonthStr(fr.value);
  var b = pipdParseMonthStr(to.value);
  if (a && b && pipdMonthCmp(a, b) > 0) to.value = fr.value;
  pipdRenderDetailTable();
}
function pipdDaysInMonth(y, m1) {
  return new Date(y, m1, 0).getDate();
}
function pipdFmtMoneyM(n, digits) {
  if (n == null || n === '' || (typeof n === 'number' && isNaN(n))) return '—';
  var x = typeof n === 'number' ? n : parseFloat(n);
  if (isNaN(x)) return '—';
  var d = digits != null ? digits : 2;
  return '₩' + (Math.round(x * Math.pow(10, d)) / Math.pow(10, d)).toFixed(d) + 'M';
}
function pipdRenderDetailTable() {
  var fr = pipdGi('pipd-detail-from');
  var to = pipdGi('pipd-detail-to');
  var tb = pipdGi('pipd-detail-tbody');
  var th = pipdGi('pipd-detail-thead');
  if (!fr || !to || !tb || !th) return;
  var T = pipdCalTargets();
  var C = window.__pipDetailCtx;
  var asp0won = (C && C.pipAsp) != null ? C.pipAsp : 0;
  var asp0m = asp0won ? asp0won / 1e6 : 0;
  var abvF = (C && C.td && C.td.abv_m != null) ? C.td.abv_m : 2.5;
  var a0 = pipdParseMonthStr(fr.value);
  var b0 = pipdParseMonthStr(to.value);
  if (!a0 || !b0) return;
  var a = a0, b = b0;
  if (pipdMonthCmp(a, b) > 0) { var t = a; a = b; b = t; }
  var rows = [];
  function addMonth(y, m) {
    var last = pipdDaysInMonth(y, m);
    for (var d = 1; d <= last; d++) rows.push({ y: y, m0: m - 1, d: d });
  }
  var cur = new Date(a.y, a.m - 1, 1);
  var end = new Date(b.y, b.m - 1, 1);
  while (cur <= end) {
    addMonth(cur.getFullYear(), cur.getMonth() + 1);
    cur.setMonth(cur.getMonth() + 1);
  }
  th.innerHTML = '<tr><th style="text-align:left;min-width:88px">일자</th><th>Appts</th><th>VR</th><th>Visits</th><th>OR</th><th>Orders</th><th>SR</th><th>Sales</th><th>BR</th><th>ASP (₩M)</th><th>ABV (₩M)</th><th>Revenue (₩M)</th></tr>';
  var tAp = T.appts, tVr = T.tVr, tOr = T.tOr, tSr = T.tSr, tSal = T.sales, tRev = T.rev;
  tb.innerHTML = rows.map(function (r) {
    var data = pipdGetDayData(r.y, r.m0, r.d) || { appts: 0, visits: 0, orders: 0, sales: 0, rev: 0, bin: 0 };
    var ap = data.appts || 0, vi = data.visits || 0, vd = data.vr_denom || 0, ord = data.orders || 0, sa = data.sales || 0, rv = data.rev || 0, br = data.bin || 0;
    var vr = vd > 0 ? Math.round((vi / vd) * 1000) / 10 : null;
    var orv = vi > 0 ? Math.round((ord / vi) * 1000) / 10 : null;
    var srv = ord > 0 ? Math.round((sa / ord) * 1000) / 10 : null;
    var aspM = sa > 0 ? Math.round((rv / sa) * 100) / 100 : (asp0m ? Math.round(asp0m * 100) / 100 : null);
    var aspCell = aspM != null ? pipdFmtMoneyM(aspM, 2) : '—';
    var abvCell = pipdFmtMoneyM(abvF, 2);
    var revCell = pipdFmtMoneyM(rv, 2);
    var revTgt = pipdFmtMoneyM(tRev, 1);
    var cAp = pipdPairCls(ap, tAp, false), cVr = pipdPairCls(vr, tVr, true), cOr = pipdPairCls(orv, tOr, true), cSr = pipdPairCls(srv, tSr, true), cSal = pipdPairCls(sa, tSal, false), cRv = pipdPairCls(rv, tRev, false);
    var ds = r.m0 + 1 + '/' + r.d;
    return '<tr><td style="text-align:left;font-weight:600">' + ds + '</td><td class="pipd-dtc"><span class="' + cAp + '">' + ap + '</span> / ' + tAp + '</td><td class="pipd-dtc">' + (vr != null ? '<span class="' + cVr + '">' + vr + '%</span> / ' + tVr + '%' : '—') + '</td><td class="pipd-dtc"><span class="' + pipdPairCls(vi, T.visits, false) + '">' + vi + '</span> / ' + T.visits + '</td><td class="pipd-dtc">' + (orv != null ? '<span class="' + cOr + '">' + orv + '%</span> / ' + tOr + '%' : '—') + '</td><td class="pipd-dtc"><span class="' + pipdPairCls(ord, T.orders, false) + '">' + ord + '</span> / ' + T.orders + '</td><td class="pipd-dtc">' + (srv != null ? '<span class="' + cSr + '">' + srv + '%</span> / ' + tSr + '%' : '—') + '</td><td class="pipd-dtc"><span class="' + cSal + '">' + sa + '</span> / ' + tSal + '</td><td class="pipd-dtc">' + (br ? br.toFixed(2) : '—') + '</td><td class="pipd-dtc">' + aspCell + '</td><td class="pipd-dtc">' + abvCell + '</td><td class="pipd-dtc"><span class="' + cRv + '">' + revCell + '</span> / ' + revTgt + '</td></tr>';
  }).join('');
}
function pipdCalc() {
  var C = window.__pipDetailCtx;
  if (!C) return;
  var act = C.act;
  var gv = pipdGv;
  var fields = [
    { id: 'cr2', actual: act.cr2, unit: '%p', cur: false },
    { id: 'vr', actual: act.vr, unit: '%p', cur: false },
    { id: 'or', actual: act.or_, unit: '%p', cur: false },
    { id: 'sr', actual: act.sr, unit: '%p', cur: false },
    { id: 'cr3', actual: act.cr3 != null ? act.cr3 : (act.vr * act.or_ * act.sr) / 1e4, unit: '%p', cur: false },
    { id: 'visits', actual: act.visits, unit: '건', cur: false },
    { id: 'orders', actual: act.orders != null ? act.orders : act.trials, unit: '건', cur: false },
    { id: 'sales', actual: act.sales, unit: '건', cur: false },
    { id: 'abv', actual: act.abv_m, unit: 'M', cur: true, tScale: 1 },
    { id: 'rev', actual: act.revenue_m, unit: 'M', cur: true, tScale: 1 }
  ];
  fields.forEach(function (f) {
    var tval = gv('t-' + f.id) * (f.tScale || 1);
    if (f.id === 'cr3') tval = gv('t-cr3');
    var gap = tval - f.actual;
    var pct = tval > 0 ? Math.min(Math.round((f.actual / tval) * 100), 999) : 0;
    var gEl = pipdGi('g-' + f.id), aEl = pipdGi('a-' + f.id);
    if (gEl) {
      var sign = gap > 0 ? '▼ ' : '▲ ';
      var abs = Math.abs(gap);
      gEl.textContent = sign + (f.cur ? '₩' + abs.toFixed(1) + 'M' : abs.toFixed(1) + f.unit);
      gEl.className = gap > 0 ? 'pipd-gd' : 'pipd-gn';
    }
    if (aEl) {
      aEl.className = 'badge ' + (pct >= 100 ? 'b-g' : pct >= 85 ? 'b-y' : 'b-r');
      aEl.textContent = pct + '%';
    }
  });
  var sVR = gv('s-vr'), sOR = gv('s-or'), sSR = gv('s-sr');
  var sABV = gv('s-abv') * 1e6;
  var sTri = gv('s-trials');
  var sCR3 = (sVR * sOR * sSR) / 1e6;
  var sSales = Math.round(sTri * (sSR / 100));
  var sRev = Math.round((sSales * sABV) / 1e6 * 10) / 10;
  window.__pipdSimRevM = sRev;
  if (pipdGi('r-cr2')) pipdGi('r-cr2').textContent = gv('s-cr2') + '%';
  if (pipdGi('r-vr')) pipdGi('r-vr').textContent = sVR + '%';
  if (pipdGi('r-or')) pipdGi('r-or').textContent = sOR + '%';
  if (pipdGi('r-sr')) pipdGi('r-sr').textContent = sSR + '%';
  if (pipdGi('r-cr3')) pipdGi('r-cr3').textContent = (sCR3 * 100).toFixed(1) + '%';
  if (pipdGi('r-visits')) pipdGi('r-visits').textContent = gv('s-visits') + '건';
  if (pipdGi('r-trials')) pipdGi('r-trials').textContent = sTri + '건';
  if (pipdGi('r-sales')) pipdGi('r-sales').textContent = sSales + '건';
  if (pipdGi('r-abv')) pipdGi('r-abv').textContent = '₩' + gv('s-abv').toFixed(1) + 'M';
  if (pipdGi('r-rev')) pipdGi('r-rev').textContent = '₩' + sRev + 'M';
  var tRev = gv('t-rev') || 0;
  var diff = Math.round((sRev - act.revenue_m) * 10) / 10;
  var strip = pipdGi('pipd-sim-strip');
  if (strip) {
    strip.innerHTML =
      '<span>시뮬 CR3 <strong>' + (sCR3 * 100).toFixed(1) + '%</strong></span>' +
      '<span>예측 Sales <strong>' + sSales + '건</strong></span>' +
      '<span>예측 Revenue <strong style="color:' + (sRev >= tRev ? '#3B6D11' : '#A32D2D') + '">₩' + sRev + 'M</strong></span>' +
      '<span>현재 대비 <strong style="color:' + (diff >= 0 ? '#3B6D11' : '#A32D2D') + '">' + (diff >= 0 ? '+' : '') + '₩' + diff + 'M</strong></span>' +
      '<span style="color:' + (sRev >= tRev ? '#3B6D11' : '#A32D2D') + '">' + (sRev >= tRev ? '✓ 목표 달성 가능' : '✗ ₩' + (tRev - sRev).toFixed(1) + 'M 추가 필요') + '</span>';
  }
  pipdMaybeRefreshCal();
  pipdUpdateChart();
}
function pipdUpdateChart() {
  var C = window.__pipDetailCtx;
  if (!C || typeof Chart === 'undefined') return;
  var cg = C.cg || {};
  var labels = cg.labels || [];
  var sim = window.__pipdSimRevM != null ? window.__pipdSimRevM : (cg.partial_m || 0);
  var past = cg.past_m || [0, 0, 0];
  var part = cg.partial_m != null ? cg.partial_m : null;
  var fc = cg.pipeline_fcst_m || [0, 0, 0];
  var U = undefined;
  var n = labels.length || 7;
  if (n < 7) labels = ['M1', 'M2', 'M3', '부분', '+1', '+2', '+3'];
  var p0 = past[0] != null ? past[0] : 0, p1 = past[1] != null ? past[1] : 0, p2 = past[2] != null ? past[2] : 0;
  var actualLine = [p0, p1, p2, part, U, U, U];
  var pipelineLine = [U, U, U, U, fc[0], fc[1], fc[2]];
  var simVals = [U, U, U, U, Math.round(sim * 0.95), Math.round(sim), Math.round(sim * 1.08)];
  var ctx = pipdGi('pipdc');
  if (!ctx) return;
  if (typeof _pipDChart !== 'undefined' && _pipDChart) {
    try { _pipDChart.destroy(); } catch (e) {}
    _pipDChart = null;
  }
  var t1 = cg.goal_line_1_m != null ? cg.goal_line_1_m : 75;
  var t2 = cg.goal_line_2_m != null ? cg.goal_line_2_m : 100;
  var nums = [p0, p1, p2, part, fc[0], fc[1], fc[2], simVals[4], simVals[5], simVals[6], t1, t2].filter(function (x) { return x != null && x !== U && !isNaN(x); });
  var ymax = nums.length ? Math.max(130, Math.round(Math.max.apply(null, nums) * 1.15)) : 130;
  _pipDChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: labels,
      datasets: [
        { label: '실적', data: actualLine, borderColor: '#185FA5', backgroundColor: 'rgba(24,95,165,.12)', fill: true, tension: 0.25, pointRadius: 4, pointBackgroundColor: '#185FA5', borderWidth: 2.5, spanGaps: false },
        { label: 'Pipeline 예측', data: pipelineLine, borderColor: '#7DB8E8', borderDash: [6, 5], fill: false, tension: 0.2, pointRadius: 3, pointBackgroundColor: '#7DB8E8', borderWidth: 2 },
        { label: '시뮬레이션', data: simVals, borderColor: '#8B7FD8', borderDash: [4, 4], fill: false, tension: 0.2, pointRadius: 3, pointBackgroundColor: '#8B7FD8', borderWidth: 2 },
        { label: '₩75M 목표', data: new Array(labels.length).fill(t1), borderColor: '#EF9F27', borderWidth: 1.5, borderDash: [5, 4], pointRadius: 0, fill: false },
        { label: '₩100M 목표', data: new Array(labels.length).fill(t2), borderColor: '#1D9E75', borderWidth: 1.5, borderDash: [5, 4], pointRadius: 0, fill: false }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: function (c) { var v = c.raw; return v == null || v === U ? '' : c.dataset.label + ': ₩' + v + 'M'; } } } },
      scales: {
        x: { grid: { display: false }, ticks: { font: { size: 10 }, color: '#888' } },
        y: { min: 0, max: ymax, grid: { color: '#F0EEE8' }, ticks: { font: { size: 10 }, color: '#888', callback: function (v) { return '₩' + v + 'M'; } } }
      },
      layout: { padding: { right: 10 } }
    }
  });
}
var pipdCalView = 'monthly';
var pipdCalDetailMode = false;
var pipdCalYear = 2025;
var pipdCalMonth = 0;
var pipdCalWeekOffset = 0;
var pipdCalDay = 16;
function pipdDkey(y, m1, d) { return y + '-' + m1 + '-' + d; }
function pipdIsRefToday(y, m0, d) {
  var C = window.__pipDetailCtx;
  if (!C) return false;
  return y === C.refY && m0 === C.refM1 - 1 && d === C.refD;
}
function pipdIsPipStart(y, m0, d) {
  var C = window.__pipDetailCtx;
  if (!C || C.pip_start_y == null) return false;
  return y === C.pip_start_y && C.pip_start_m1 === m0 + 1 && d === (C.pip_start_d || 15);
}
function pipdIsPast(y, m0, d) {
  var C = window.__pipDetailCtx;
  if (!C) return false;
  var ref = new Date(C.refY, C.refM1 - 1, C.refD);
  return new Date(y, m0, d) < ref;
}
function pipdGetDayData(y, m0, d) {
  var C = window.__pipDetailCtx;
  var months = (C && C.calendar_months) || {};
  var mk = y + '-' + (m0 + 1);
  var blob = months[mk];
  if (!blob) return null;
  return blob[pipdDkey(y, m0 + 1, d)];
}
function pipdSetPeriod(p, el) {
  var root = el && el.parentElement;
  if (root) root.querySelectorAll('.pipd-tag').forEach(function (t) { t.classList.remove('pipd-on'); });
  if (el) el.classList.add('pipd-on');
  var presets = {
    monthly: { cr2: 20, vr: 75, or: 50, sr: 85, visits: 20, trials: 10, sales: 9, abv: 2.5, rev: 10000, cr3: 45 },
    weekly: { cr2: 5, vr: 75, or: 50, sr: 85, visits: 5, trials: 2, sales: 2, abv: 2.5, rev: 2500, cr3: 45 },
    d30: { cr2: 20, vr: 75, or: 50, sr: 85, visits: 20, trials: 10, sales: 9, abv: 2.5, rev: 7500, cr3: 45 },
    d60: { cr2: 20, vr: 78, or: 52, sr: 87, visits: 22, trials: 12, sales: 11, abv: 2.5, rev: 9000, cr3: 46 },
    d90: { cr2: 20, vr: 80, or: 55, sr: 90, visits: 24, trials: 14, sales: 13, abv: 2.5, rev: 10000, cr3: 48 }
  };
  var t = presets[p] || presets.monthly;
  ['cr2', 'vr', 'or', 'sr', 'cr3', 'visits', 'trials', 'sales', 'abv', 'rev'].forEach(function (k) {
    var el2 = pipdGi('t-' + k);
    if (el2 && t[k] != null) el2.value = t[k];
  });
  pipdCalc();
}
function pipdSetCalView(v, el) {
  pipdCalDetailMode = false;
  var pm = pipdGi('pipd-cal-panel-main'), pd = pipdGi('pipd-cal-panel-detail'), bt = pipdGi('pipd-btn-cal-toggle');
  if (pm) pm.style.display = 'block';
  if (pd) pd.style.display = 'none';
  if (bt) bt.textContent = '세부내용 보기';
  pipdCalView = v;
  ['pipd-tab-daily', 'pipd-tab-weekly', 'pipd-tab-monthly'].forEach(function (id) {
    var x = pipdGi(id);
    if (x) x.classList.remove('pipd-on');
  });
  if (el) el.classList.add('pipd-on');
  var wn = pipdGi('pipd-week-nav');
  var dn = pipdGi('pipd-day-nav');
  if (wn) { wn.style.display = v === 'weekly' ? 'flex' : 'none'; }
  if (dn) { dn.style.display = v === 'daily' ? 'flex' : 'none'; }
  pipdRenderCal();
}
/* ── 캘린더 스크롤 포커스 관리 ──
   캘린더 패널을 클릭하면 스크롤 활성화(cal-active),
   패널 외부를 클릭하면 비활성화 → 비활성 상태에서 휠은 페이지로 전달 */
var _pipdCalScrollReady = false;
function pipdSetupCalScroll() {
  if (_pipdCalScrollReady) return;
  var cp = pipdGi('pipd-cal-panel-main');
  if (!cp) return;
  _pipdCalScrollReady = true;
  /* 클릭 시 활성화 */
  cp.addEventListener('click', function() {
    cp.classList.add('cal-active');
  }, { passive: true });
  /* 외부 클릭 시 비활성화 */
  document.addEventListener('click', function(e) {
    if (!cp.contains(e.target)) { cp.classList.remove('cal-active'); }
  }, { passive: true });
  /* 비활성 상태에서 휠 이벤트 → 페이지 스크롤로 전달 */
  cp.addEventListener('wheel', function(e) {
    if (!cp.classList.contains('cal-active')) {
      e.preventDefault();
      window.scrollBy({ top: e.deltaY, behavior: 'auto' });
    }
  }, { passive: false });
}
function pipdMoveMonth(delta) {
  pipdCalMonth += delta;
  if (pipdCalMonth < 0) { pipdCalMonth = 11; pipdCalYear--; }
  if (pipdCalMonth > 11) { pipdCalMonth = 0; pipdCalYear++; }
  if (pipdCalDetailMode) { pipdFillDetailPeriod(); pipdRenderDetailTable(); }
  pipdRenderCal();
}
function pipdMoveWeek(delta) { pipdCalWeekOffset += delta; pipdRenderCal(); }
function pipdMoveDay(delta) {
  var dt = new Date(pipdCalYear, pipdCalMonth, pipdCalDay + delta);
  pipdCalYear = dt.getFullYear();
  pipdCalMonth = dt.getMonth();
  pipdCalDay = dt.getDate();
  pipdRenderCal();
}
function pipdDrillDay(y, m0, d) {
  pipdCalYear = y;
  pipdCalMonth = m0;
  pipdCalDay = d;
  pipdSetCalView('daily', pipdGi('pipd-tab-daily'));
}
function pipdRenderCal() {
  var title = pipdGi('pipd-cal-title');
  if (title) title.textContent = pipdCalYear + '년 ' + (pipdCalMonth + 1) + '월';
  if (pipdSummaryOpen) pipdRenderSummary();
  if (pipdCalDetailMode) return;
  if (pipdCalView === 'monthly') pipdRenderMonthly();
  else if (pipdCalView === 'weekly') pipdRenderWeekly();
  else pipdRenderDaily();
}
/* ─── 요약보기 테이블 (캘린더 위) ───────────────────────────────── */
var pipdSummaryOpen = false;
function pipdToggleSummary() {
  pipdSummaryOpen = !pipdSummaryOpen;
  var panel = pipdGi('pipd-summary-panel');
  var btn   = pipdGi('pipd-btn-summary');
  if (!panel) return;
  panel.style.display = pipdSummaryOpen ? 'block' : 'none';
  if (btn) btn.style.background = pipdSummaryOpen ? '#185FA5' : '';
  if (btn) btn.style.color      = pipdSummaryOpen ? '#fff' : '';
  if (pipdSummaryOpen) pipdRenderSummary();
}

function pipdSumRange(startDate, endDate) {
  /* startDate, endDate: Date objects. Returns aggregated metrics. */
  var s = { appts:0, visits:0, orders:0, sales:0, rev:0, vr_denom:0, cnt:0 };
  var d = new Date(startDate);
  while (d <= endDate) {
    var data = pipdGetDayData(d.getFullYear(), d.getMonth(), d.getDate());
    if (data && !data.future) {
      s.appts    += data.appts    || 0;
      s.visits   += data.visits   || 0;
      s.orders   += data.orders   || 0;
      s.sales    += data.sales    || 0;
      s.rev      += data.rev      || 0;
      s.vr_denom += data.vr_denom || 0;
      s.cnt++;
    }
    d.setDate(d.getDate() + 1);
  }
  s.rev = Math.round(s.rev * 100) / 100;
  s.vr  = s.vr_denom > 0 ? Math.round(s.visits / s.vr_denom * 1000) / 10 : null;
  s.or_ = s.visits   > 0 ? Math.round(s.orders / s.visits   * 1000) / 10 : null;
  s.sr  = s.orders   > 0 ? Math.round(s.sales  / s.orders   * 1000) / 10 : null;
  return s;
}

function pipdRenderSummary() {
  var C = window.__pipDetailCtx;
  var panel = pipdGi('pipd-summary-panel');
  if (!panel || !C) return;
  var T   = pipdCalTargets();
  var now = new Date(C.refY, C.refM1-1, C.refD);

  /* 날짜 포맷: YY.M.D */
  function fmtD(d) {
    return (String(d.getFullYear()).slice(2)) + '.' + (d.getMonth()+1) + '.' + d.getDate();
  }
  function fmtRange(s, e) { return fmtD(s) + '~' + fmtD(e); }

  /* 기간 정의 */
  var dow2 = now.getDay();
  var thisWeekStart = new Date(now); thisWeekStart.setDate(now.getDate() - (dow2===0?6:dow2-1));
  var thisWeekEnd   = new Date(Math.min(new Date(thisWeekStart).setDate(thisWeekStart.getDate()+6), now));
  var lastWeekStart = new Date(thisWeekStart); lastWeekStart.setDate(thisWeekStart.getDate()-7);
  var lastWeekEnd   = new Date(thisWeekStart); lastWeekEnd.setDate(thisWeekStart.getDate()-1);
  var last2Start    = new Date(thisWeekStart); last2Start.setDate(thisWeekStart.getDate()-14);
  var prevMonthStart = new Date(C.refY, C.refM1-2, 1);
  var prevMonthEnd   = new Date(C.refY, C.refM1-1, 0);
  var thisMonthStart = new Date(C.refY, C.refM1-1, 1);
  var thisMonthEnd   = now;

  /* 각 기간 집계 */
  var sLW  = pipdSumRange(lastWeekStart, lastWeekEnd);
  var sTW  = pipdSumRange(thisWeekStart, new Date(thisWeekEnd));
  var sL2  = pipdSumRange(last2Start,    lastWeekEnd);
  var sPM  = pipdSumRange(prevMonthStart, prevMonthEnd);
  var sTM  = pipdSumRange(thisMonthStart, thisMonthEnd);

  /* 목표값 세트 — metrics 순서: Appts,Visits,Orders,Sales,VR,OR,SR,Rev */
  var mT = [T.mAppts,T.mVisits,T.mOrders,T.mSales,T.tVr,T.tOr,T.tSr,T.mRev];
  var wT = [Math.round(T.mAppts/4),Math.round(T.mVisits/4),Math.round(T.mOrders/4),
            Math.round(T.mSales/4),T.tVr,T.tOr,T.tSr,Math.round(T.mRev/4*10)/10];
  var w2T= [Math.round(T.mAppts/2),Math.round(T.mVisits/2),Math.round(T.mOrders/2),
            Math.round(T.mSales/2),T.tVr,T.tOr,T.tSr,Math.round(T.mRev/2*10)/10];

  /* 컬럼 정의: label, dateRange, sumObj, targets */
  var cols = [
    { lbl:'지난주',   dates: fmtRange(lastWeekStart, lastWeekEnd),   s:sLW, tgt:wT  },
    { lbl:'이번주',   dates: fmtRange(thisWeekStart, new Date(thisWeekEnd)), s:sTW, tgt:wT  },
    { lbl:'지난 2주', dates: fmtRange(last2Start,    lastWeekEnd),   s:sL2, tgt:w2T },
    { lbl:'지난달',   dates: fmtRange(prevMonthStart,prevMonthEnd),  s:sPM, tgt:mT  },
    { lbl:'이번달',   dates: fmtRange(thisMonthStart,thisMonthEnd),  s:sTM, tgt:mT  },
  ];

  var metrics = [
    { k:'Appts',  field:'appts',  rate:false },
    { k:'Visits', field:'visits', rate:false },
    { k:'Orders', field:'orders', rate:false },
    { k:'Sales',  field:'sales',  rate:false },
    { k:'VR',     field:'vr',     rate:true  },
    { k:'OR',     field:'or_',    rate:true  },
    { k:'SR',     field:'sr',     rate:true  },
    { k:'Rev',    field:'rev',    rate:false },
  ];
  var mIdxMap = { appts:0, visits:1, orders:2, sales:3, vr:4, or_:5, sr:6, rev:7 };

  function cellVal(s, m) {
    var v = s[m.field];
    if (m.rate) return v;        /* already % */
    return v != null ? v : null;
  }
  function fmtV(v, rate) {
    if (v == null) return '<span style="color:#ccc">—</span>';
    if (rate) return v + '%';
    return String(v);
  }
  function getClr(v, t, rate) {
    if (v == null || t == null) return '#555';
    return v >= t ? '#1D9E75' : v >= t * (rate ? 0.9 : 0.8) ? '#EF9F27' : '#E24B4A';
  }

  /* 강화 필요: 이번달 + 지난주 모두 목표 미달인 지표 */
  var weakMap = {};
  metrics.forEach(function(m, i) {
    var vTM = cellVal(sTM, m), tTM = mT[i];
    var vLW = cellVal(sLW, m), tLW = wT[i];
    var gap = 0;
    if (vTM != null && tTM) gap += Math.max(0, (tTM - vTM) / tTM);
    if (vLW != null && tLW) gap += Math.max(0, (tLW - vLW) / tLW);
    if (gap > 0.1) weakMap[m.k] = Math.round(gap * 50);  /* 부족 점수 */
  });
  var weakSorted = Object.keys(weakMap).sort(function(a,b){ return weakMap[b]-weakMap[a]; });

  /* HTML 생성 */
  var th = function(lbl, sub) {
    return '<th style="padding:6px 8px;text-align:center;background:#F4F3EF;color:#555;font-size:13px;white-space:nowrap;font-weight:700">'
      + lbl + '<br><span style="font-size:11px;font-weight:400;color:#aaa">' + sub + '</span></th>';
  };

  var html = '<div style="margin-bottom:10px">';
  /* 헤더 안내 */
  html += '<div style="font-size:12px;color:#aaa;margin-bottom:6px">* 실제값 / 목표값</div>';
  html += '<div style="overflow-x:auto"><table style="border-collapse:collapse;font-size:13px;width:100%;min-width:640px">';
  html += '<thead><tr>'
    + '<th style="padding:6px 8px;text-align:left;background:#F4F3EF;color:#888;font-size:13px;white-space:nowrap">지표</th>';
  cols.forEach(function(c) { html += th(c.lbl, c.dates); });
  html += '</tr></thead><tbody>';

  metrics.forEach(function(m) {
    var mi = mIdxMap[m.field];
    html += '<tr style="border-bottom:1px solid #F4F3EF">';
    html += '<td style="padding:5px 8px;font-weight:600;color:#555;background:#FAFAF8">' + m.k + '</td>';
    cols.forEach(function(c) {
      var v = cellVal(c.s, m);
      var t = c.tgt[mi];
      var clr = getClr(v, t, m.rate);
      var bg  = v!=null && t!=null ? (v >= t ? 'background:#F0FAF5;' : v >= t*(m.rate?0.9:0.8) ? '' : 'background:#FFF5F5;') : '';
      var disp = fmtV(v, m.rate);
      var tDisp = t!=null ? '<span style="font-size:10px;color:#bbb;margin-left:2px">/ '+(m.rate?t+'%':t)+'</span>' : '';
      html += '<td style="padding:5px 8px;text-align:center;'+bg+'">'
            + '<span style="font-weight:700;color:'+clr+'">'+disp+'</span>'+tDisp+'</td>';
    });
    html += '</tr>';
  });

  /* 강화 필요 행 */
  html += '<tr style="border-top:2px solid #E5E4DF;background:#FFF9F0">';
  html += '<td style="padding:5px 8px;font-weight:700;color:#854F0B;font-size:12px;background:#FFF3E0">강화 필요</td>';
  /* 각 컬럼별 가장 부족한 지표 */
  cols.forEach(function(c) {
    var colWeak = [];
    metrics.forEach(function(m, i) {
      var v = cellVal(c.s, m), t = c.tgt[mIdxMap[m.field]];
      if (v != null && t != null && v < t) {
        var gap = (t - v) / t;
        colWeak.push({ k: m.k, gap: gap });
      }
    });
    colWeak.sort(function(a,b){ return b.gap-a.gap; });
    var topWeak = colWeak.slice(0,2).map(function(x){ return x.k; });
    html += '<td style="padding:5px 8px;text-align:center;font-size:12px;color:#E24B4A;font-weight:600">'
          + (topWeak.length ? topWeak.join(', ') : '<span style="color:#1D9E75">✓</span>')
          + '</td>';
  });
  html += '</tr>';

  html += '</tbody></table></div>';

  /* 예상 매출 한줄 */
  var act = C.act || {};
  var cr3 = (act.cr3 || 15) / 100;
  var abvM = act.abv_m || 2.5;
  var daysLeft = new Date(C.refY, C.refM1, 0).getDate() - C.refD;
  var projAdd = Math.round(daysLeft * T.appts * cr3 * abvM * 10) / 10;
  var projThis = Math.round((sTM.rev + projAdd) * 10) / 10;
  html += '<div style="margin-top:8px;font-size:13px;color:#555">'
        + '이번달 예상 매출: <strong style="color:#185FA5">₩'+projThis+'M</strong>'
        + '<span style="font-size:11px;color:#aaa;margin-left:6px">(실적 ₩'+sTM.rev+'M + 잔여 ₩'+projAdd+'M)</span></div>';

  html += '</div>';
  panel.innerHTML = html;
}

/* ─── 단일 월 캘린더 HTML 생성 (수직 스크롤용) ─────────────────── */
function pipdBuildMonthHtml(year, month0, T) {
  var C = window.__pipDetailCtx;
  var first = new Date(year, month0, 1).getDay();
  var offset = first === 0 ? 6 : first - 1;
  var days   = new Date(year, month0 + 1, 0).getDate();
  var cumA = 0, cumV = 0, cumO = 0, cumS = 0, cumR = 0;
  for (var d = 1; d <= days; d++) {
    var vx = pipdGetDayData(year, month0, d);
    if (vx && !vx.future) {
      cumA += vx.appts||0; cumV += vx.visits||0; cumO += vx.orders||0;
      cumS += vx.sales||0; cumR += vx.rev||0;
    }
  }
  var dow = ['월','화','수','목','금','토','일'];
  var html = '<div style="margin-bottom:24px">';
  /* 월 헤더 */
  html += '<div style="font-size:16px;font-weight:700;color:#333;margin-bottom:8px;padding:4px 0;border-bottom:2px solid #E5E4DF">'
        + year + '년 ' + (month0+1) + '월</div>';
  /* 요일 헤더 */
  html += '<div class="pipd-mg-wrap" style="margin-bottom:4px">';
  dow.forEach(function(d,i){ html += '<div class="pipd-mg-dow'+(i>=5?' we':'')+'">' + d + '</div>'; });
  html += '</div><div class="pipd-mg-wrap">';
  for (var i = 0; i < offset; i++) html += '<div class="pipd-mc empty"></div>';
  for (var di = 1; di <= days; di++) {
    var dt  = new Date(year, month0, di);
    var isWE = dt.getDay()===0 || dt.getDay()===6;
    var data = pipdGetDayData(year, month0, di);
    var tod  = pipdIsRefToday(year, month0, di);
    var pip  = pipdIsPipStart(year, month0, di);
    var cls  = 'pipd-mc';
    if (isWE) cls += ' weekend';
    if (tod)  cls += ' today';
    else if (pip) cls += ' pip-start';
    var inner = '<div class="pipd-mc-date">' + di;
    if (tod) inner += '<span class="pipd-tdot"></span>';
    if (pip) inner += '<span class="pipd-pdot"></span>';
    inner += '</div>';
    if (pip) inner += '<div class="pipd-mc-row"><span class="pipd-mc-lbl" style="color:#A32D2D;font-size:11px">PIP시작</span></div>';
    if (data && !data.future) {
      var ap = data.appts||0, vi = data.visits||0, vd = data.vr_denom||0;
      var ord = data.orders||0, sa = data.sales||0, rv = data.rev||0;
      var cm = pipdDayComment(ap, vi, vd, ord, sa, rv, T.appts, T.tVr, T.tOr, T.tSr, T.rev, T.sales);
      cls += ' ' + cm.cls;
      var h = function(a,t,pct){ return pipdPairCls(a,t,pct); };
      inner += '<div class="pipd-mc-row"><span class="pipd-mc-lbl">Appts</span><span class="pipd-mc-val"><span class="'+h(ap,T.appts,false)+'">'+ap+'</span></span></div>';
      inner += '<div class="pipd-mc-row"><span class="pipd-mc-lbl">Visits</span><span class="pipd-mc-val"><span class="'+h(vi,T.visits,false)+'">'+vi+'</span></span></div>';
      inner += '<div class="pipd-mc-row"><span class="pipd-mc-lbl">Orders</span><span class="pipd-mc-val"><span class="'+h(ord,T.orders,false)+'">'+ord+'</span></span></div>';
      inner += '<div class="pipd-mc-row"><span class="pipd-mc-lbl">Sales</span><span class="pipd-mc-val"><span class="'+h(sa,T.sales,false)+'">'+sa+'</span></span></div>';
      inner += '<div class="pipd-mc-row"><span class="pipd-mc-lbl">Rev</span><span class="pipd-mc-val"><span class="'+h(rv,T.rev,false)+'">₩'+rv+'M</span></span></div>';
      var ct = data.call_time_display || '';
      if (ct) inner += '<div class="pipd-mc-row"><span class="pipd-mc-lbl" style="color:#888">Call</span><span class="pipd-mc-val" style="font-size:10px;color:#666">'+ct+'</span></div>';
      inner += '<div class="pipd-mc-cm '+cm.cls+'">'+escapePlCell(cm.text)+'</div>';
    } else if (data && data.future) {
      var hint = pipdFutureHint();
      cls += ' st-hint';
      inner += '<div class="pipd-mc-cm st-warn" style="margin-top:6px;font-size:12px">'+hint.text+'</div>';
    } else if (!pip) {
      inner += '<div style="font-size:12px;color:#ddd;margin-top:4px">—</div>';
    }
    html += '<div class="'+cls+'">' + inner + '</div>';
  }
  html += '</div>';
  /* 월 합계 바 */
  var pct = function(v,t){ return t>0?Math.min(Math.round(v/t*100),100):0; };
  var cc2 = function(v,t){ return v>=t?'#3B6D11':'#A32D2D'; };
  html += '<div class="pipd-wsum" style="margin-top:6px;flex-wrap:wrap">'
    + '<div class="pipd-ws-item"><div class="pipd-ws-l">Appts</div><div class="pipd-ws-v" style="color:'+cc2(cumA,T.mAppts)+'">'+cumA+'건</div><div class="pipd-prog"><div class="pipd-prog-fill" style="width:'+pct(cumA,T.mAppts)+'%;background:'+cc2(cumA,T.mAppts)+'"></div></div></div>'
    + '<div class="pipd-ws-item"><div class="pipd-ws-l">Visits</div><div class="pipd-ws-v" style="color:'+cc2(cumV,T.mVisits)+'">'+cumV+'건</div><div class="pipd-prog"><div class="pipd-prog-fill" style="width:'+pct(cumV,T.mVisits)+'%;background:'+cc2(cumV,T.mVisits)+'"></div></div></div>'
    + '<div class="pipd-ws-item"><div class="pipd-ws-l">Orders</div><div class="pipd-ws-v" style="color:'+cc2(cumO,T.mOrders)+'">'+cumO+'건</div><div class="pipd-prog"><div class="pipd-prog-fill" style="width:'+pct(cumO,T.mOrders)+'%;background:'+cc2(cumO,T.mOrders)+'"></div></div></div>'
    + '<div class="pipd-ws-item"><div class="pipd-ws-l">Sales</div><div class="pipd-ws-v" style="color:'+cc2(cumS,T.mSales)+'">'+cumS+'건</div><div class="pipd-prog"><div class="pipd-prog-fill" style="width:'+pct(cumS,T.mSales)+'%;background:'+cc2(cumS,T.mSales)+'"></div></div></div>'
    + '<div class="pipd-ws-item"><div class="pipd-ws-l">Revenue</div><div class="pipd-ws-v" style="color:'+cc2(cumR,T.mRev)+'">₩'+(Math.round(cumR*10)/10)+'M</div><div class="pipd-prog"><div class="pipd-prog-fill" style="width:'+pct(cumR,T.mRev)+'%;background:'+cc2(cumR,T.mRev)+'"></div></div></div>'
    + '</div>';
  html += '</div>';
  return html;
}

function pipdCalUpdateMonthLabel() {
  var C = window.__pipDetailCtx;
  var container = pipdGi('pipd-cal-panel-main');
  var monthEl = pipdGi('pipd-cal-month-text');
  var todayEl = pipdGi('pipd-cal-today-text');
  if (!container || !monthEl) return;
  var cRect = container.getBoundingClientRect();
  var cells = container.querySelectorAll('[data-ym]');
  var curYm = null;
  /* 현재 스크롤에서 40% 지점 위에 있는 마지막 월 마커 = 현재 표시 월 */
  var midY = cRect.top + cRect.height * 0.4;
  cells.forEach(function(cell) {
    var r = cell.getBoundingClientRect();
    if (r.top <= midY) curYm = cell.getAttribute('data-ym');
  });
  if (!curYm && cells.length > 0) curYm = cells[0].getAttribute('data-ym');
  if (curYm) {
    var pts = curYm.split('-');
    monthEl.textContent = pts[0] + '년 ' + parseInt(pts[1]) + '월';
  }
  if (todayEl && C && C.refY) {
    var d = new Date(C.refY, C.refM1 - 1, C.refD);
    var dayN = ['일','월','화','수','목','금','토'];
    todayEl.textContent = '오늘 ' + C.refM1 + '월 ' + C.refD + '일 ' + dayN[d.getDay()] + '요일';
  }
}

function pipdRenderMonthly() {
  var C = window.__pipDetailCtx;
  var T = pipdCalTargets();
  /* 모든 캘린더 월을 하나의 연속 그리드로 렌더 (Windows 달력 스타일) */
  var months = (C && C.calendar_monitor_months) || [];
  if (!months.length) {
    months = [pipdCalYear + '-' + String(pipdCalMonth+1).padStart(2,'0')];
  }
  /* 시작 날짜의 요일 offset 계산 */
  var firstMk = months[0].split('-');
  var gridStartY = parseInt(firstMk[0]), gridStartM0 = parseInt(firstMk[1]) - 1;
  var firstDow = new Date(gridStartY, gridStartM0, 1).getDay();
  var startOffset = firstDow === 0 ? 6 : firstDow - 1;  /* 월=0 기준 */
  var dow = ['월','화','수','목','금','토','일'];

  /* 주간 목표 (월/4) */
  var wAppts  = Math.round(T.mAppts  / 4);
  var wVisits = Math.round(T.mVisits / 4);
  var wOrders = Math.round(T.mOrders / 4);
  var wSales  = Math.round(T.mSales  / 4);
  var wRev    = Math.round(T.mRev    / 4 * 10) / 10;

  /* 주간 요약 셀 생성 */
  function buildWeekSum(weekDates) {
    var tot = { appts:0, visits:0, orders:0, sales:0, rev:0, hasPast:false };
    weekDates.forEach(function(d) {
      if (!d) return;
      var data = pipdGetDayData(d.y, d.m0, d.di);
      if (data && !data.future) {
        tot.appts  += data.appts  || 0;
        tot.visits += data.visits || 0;
        tot.orders += data.orders || 0;
        tot.sales  += data.sales  || 0;
        tot.rev    += data.rev    || 0;
        tot.hasPast = true;
      }
    });
    tot.rev = Math.round(tot.rev * 10) / 10;
    var realDates = weekDates.filter(function(d){ return d != null; });
    var lbl = '';
    if (realDates.length) {
      var f = realDates[0], l = realDates[realDates.length-1];
      lbl = (f.m0+1)+'/'+f.di + '~' + (l.m0+1)+'/'+l.di;
    }
    /* 갭 클래스 & 포맷 */
    var gCls = function(v, t) { return v >= t ? 'wks-gp' : 'wks-gn'; };
    var gVal = function(v, t, rev) {
      var diff = rev ? Math.round((v - t) * 10) / 10 : (v - t);
      return (diff >= 0 ? '+' : '') + (rev ? diff.toFixed(1) : diff);
    };
    var vClr = function(v, t) {
      return v >= t ? '#0D6E4E' : v >= t * 0.8 ? '#7A5000' : '#A32D2D';
    };
    var s = '<div class="pipd-mc pipd-wk-sum">';
    s += '<div style="font-size:10px;color:#3A6A8A;font-weight:700;text-align:center;margin-bottom:5px;padding-bottom:4px;border-bottom:1px solid rgba(255,255,255,.7)">'
       + (lbl || '주간') + '</div>';
    if (!tot.hasPast) {
      s += '<div style="font-size:11px;color:#4A7EA0;text-align:center;margin-top:8px">집계 전</div>';
    } else {
      s += '<table>'
        + '<tr><th>지표</th><th>목표</th><th>실제</th><th>갭</th></tr>';
      var rows = [
        { k:'Appts',  v:tot.appts,  t:wAppts,  rev:false, sfx:'건' },
        { k:'Visits', v:tot.visits, t:wVisits, rev:false, sfx:'건' },
        { k:'Orders', v:tot.orders, t:wOrders, rev:false, sfx:'건' },
        { k:'Sales',  v:tot.sales,  t:wSales,  rev:false, sfx:'건' },
        { k:'Rev',    v:tot.rev,    t:wRev,    rev:true,  sfx:'M'  },
      ];
      rows.forEach(function(r) {
        var vStr = r.rev ? '₩'+r.v+'M' : r.v+r.sfx;
        var tStr = r.rev ? '₩'+r.t+'M' : r.t+r.sfx;
        s += '<tr>'
          + '<td>' + r.k + '</td>'
          + '<td style="color:#4A7EA0">' + tStr + '</td>'
          + '<td style="color:' + vClr(r.v, r.t) + ';font-weight:700">' + vStr + '</td>'
          + '<td class="' + gCls(r.v, r.t) + '">' + gVal(r.v, r.t, r.rev) + '</td>'
          + '</tr>';
      });
      s += '</table>';
    }
    s += '</div>';
    return s;
  }

  /* ── sticky 헤더: 현재 월 표시 + 요일 행 (8컬럼) ── */
  var html = '<div class="pipd-cal-sticky-header">';
  html += '<div style="display:flex;align-items:center;justify-content:space-between;padding:2px 4px 4px">'
        + '<span id="pipd-cal-month-text" style="font-size:15px;font-weight:700;color:#333"></span>'
        + '<span id="pipd-cal-today-text" style="font-size:13px;color:#185FA5;font-weight:500"></span>'
        + '</div>';
  html += '<div class="pipd-mg-wrap-8">';
  dow.forEach(function(d,i){ html += '<div class="pipd-mg-dow'+(i>=5?' we':'')+'">' + d + '</div>'; });
  html += '<div class="pipd-mg-dow" style="color:#8CAAC4;font-size:12px;text-align:center;background:#EEF3F8;border-radius:4px">주간합계</div>';
  html += '</div></div>';

  html += '<div class="pipd-mg-wrap-8">';
  /* 빈 셀 (첫 주 패딩) */
  var cellCount = 0;
  var weekBuf = [];
  var tspan = function(t) {
    return '<span style="color:#C8C4BB;font-size:10px"> /' + t + '</span>';
  };
  for (var k = 0; k < startOffset; k++) {
    html += '<div class="pipd-mc empty"></div>';
    weekBuf.push(null);
    cellCount++;
    if (cellCount % 7 === 0) { html += buildWeekSum(weekBuf); weekBuf = []; }
  }
  /* 모든 달 날짜 렌더 */
  months.forEach(function(mk) {
    var parts = mk.split('-');
    var y = parseInt(parts[0]), m0 = parseInt(parts[1]) - 1;
    var daysInMonth = new Date(y, m0 + 1, 0).getDate();
    for (var di = 1; di <= daysInMonth; di++) {
      var dt   = new Date(y, m0, di);
      var isWE = dt.getDay() === 0 || dt.getDay() === 6;
      var data = pipdGetDayData(y, m0, di);
      var tod  = pipdIsRefToday(y, m0, di);
      var pip  = pipdIsPipStart(y, m0, di);
      var isFirst = di === 1;
      var cls = 'pipd-mc';
      if (isFirst) cls += ' month-first';
      if (isWE) cls += ' weekend';
      if (tod)  cls += ' today';
      else if (pip) cls += ' pip-start';
      var inner = '<div class="pipd-mc-date">';
      if (isFirst) inner += '<span style="font-size:10px;color:#185FA5;font-weight:700;display:block;line-height:1">' + (m0+1) + '월</span>';
      inner += di;
      if (tod) inner += '<span class="pipd-tdot"></span>';
      if (pip) inner += '<span class="pipd-pdot"></span>';
      inner += '</div>';
      if (pip) inner += '<div class="pipd-mc-row"><span class="pipd-mc-lbl" style="color:#A32D2D;font-size:11px">PIP시작</span></div>';
      if (data && !data.future) {
        var ap = data.appts||0, vi = data.visits||0, vd = data.vr_denom||0;
        var ord = data.orders||0, sa = data.sales||0, rv = data.rev||0;
        var cm = pipdDayComment(ap, vi, vd, ord, sa, rv, T.appts, T.tVr, T.tOr, T.tSr, T.rev, T.sales);
        cls += ' ' + cm.cls;
        var h = function(a,t,pct){ return pipdPairCls(a,t,pct); };
        var ts = isWE ? '' : tspan(T.appts);
        inner += '<div class="pipd-mc-row"><span class="pipd-mc-lbl">Appts</span><span class="pipd-mc-val"><span class="'+h(ap,T.appts,false)+'">'+ap+'</span>'+ts+'</span></div>';
        ts = isWE ? '' : tspan(T.visits);
        inner += '<div class="pipd-mc-row"><span class="pipd-mc-lbl">Visits</span><span class="pipd-mc-val"><span class="'+h(vi,T.visits,false)+'">'+vi+'</span>'+ts+'</span></div>';
        ts = isWE ? '' : tspan(T.orders);
        inner += '<div class="pipd-mc-row"><span class="pipd-mc-lbl">Orders</span><span class="pipd-mc-val"><span class="'+h(ord,T.orders,false)+'">'+ord+'</span>'+ts+'</span></div>';
        ts = isWE ? '' : tspan(T.sales);
        inner += '<div class="pipd-mc-row"><span class="pipd-mc-lbl">Sales</span><span class="pipd-mc-val"><span class="'+h(sa,T.sales,false)+'">'+sa+'</span>'+ts+'</span></div>';
        ts = isWE ? '' : tspan(T.rev);
        inner += '<div class="pipd-mc-row"><span class="pipd-mc-lbl">Rev</span><span class="pipd-mc-val"><span class="'+h(rv,T.rev,false)+'">₩'+rv+'M</span>'+ts+'</span></div>';
        var ct = data.call_time_display || '';
        if (ct) inner += '<div class="pipd-mc-row"><span class="pipd-mc-lbl" style="color:#888">Call</span><span class="pipd-mc-val" style="font-size:10px;color:#666">'+ct+'</span></div>';
        inner += '<div class="pipd-mc-cm '+cm.cls+'">'+escapePlCell(cm.text)+'</div>';
      } else if (data && data.future) {
        var hint = pipdFutureHint();
        cls += ' st-hint';
        inner += '<div class="pipd-mc-cm st-warn" style="margin-top:6px;font-size:12px">'+hint.text+'</div>';
      } else if (!pip) {
        inner += '<div style="font-size:12px;color:#ddd;margin-top:4px">—</div>';
      }
      var ymAttr = isFirst ? ' data-ym="'+y+'-'+(m0+1)+'"' : '';
      html += '<div class="'+cls+'"'+ymAttr+'>' + inner + '</div>';
      weekBuf.push({y:y, m0:m0, di:di});
      cellCount++;
      if (cellCount % 7 === 0) { html += buildWeekSum(weekBuf); weekBuf = []; }
    }
  });
  /* 마지막 불완전 주 처리 */
  if (weekBuf.length > 0) {
    var rem = 7 - weekBuf.length;
    for (var r = 0; r < rem; r++) html += '<div class="pipd-mc empty"></div>';
    html += buildWeekSum(weekBuf);
  }
  html += '</div>';
  var body = pipdGi('pipd-cal-body');
  if (body) body.innerHTML = html;
  /* 스크롤 시 월 라벨 업데이트 */
  var panel = pipdGi('pipd-cal-panel-main');
  if (panel) {
    panel.removeEventListener('scroll', pipdCalUpdateMonthLabel);
    panel.addEventListener('scroll', pipdCalUpdateMonthLabel);
    /* 오늘 날짜 셀을 패널 중앙에 오도록 초기 스크롤 */
    setTimeout(function() {
      var todayCell = panel.querySelector('.pipd-mc.today');
      if (todayCell) {
        /* getBoundingClientRect 기준으로 패널 내 상대 위치 계산 */
        var panelRect = panel.getBoundingClientRect();
        var cellRect  = todayCell.getBoundingClientRect();
        var cellRelTop = cellRect.top - panelRect.top + panel.scrollTop;
        panel.scrollTop = Math.max(0, cellRelTop - panel.clientHeight / 2 + cellRect.height / 2);
      }
      pipdCalUpdateMonthLabel();
    }, 120);
  } else {
    setTimeout(pipdCalUpdateMonthLabel, 60);
  }
}
function pipdRenderWeekly() {
  var T = pipdCalTargets();
  var base = new Date(pipdCalYear, pipdCalMonth, pipdCalDay);
  base.setDate(base.getDate() + pipdCalWeekOffset * 7);
  var dow2 = base.getDay();
  var mon = new Date(base);
  mon.setDate(base.getDate() - (dow2 === 0 ? 6 : dow2 - 1));
  var fmt = function (d) { return d.getMonth() + 1 + '/' + d.getDate(); };
  var wEnd = new Date(mon);
  wEnd.setDate(mon.getDate() + 6);
  var wl = pipdGi('pipd-week-label');
  if (wl) wl.textContent = fmt(mon) + ' ~ ' + fmt(wEnd);
  var dow = ['월', '화', '수', '목', '금', '토', '일'];
  var html = '<div class="pipd-wg-wrap" style="margin-bottom:4px">';
  dow.forEach(function (d, i) { html += '<div class="pipd-mg-dow' + (i >= 5 ? ' we' : '') + '">' + d + '</div>'; });
  html += '</div><div class="pipd-wg-wrap">';
  var tA = 0, tV = 0, tO = 0, tS = 0, tR = 0;
  for (var i = 0; i < 7; i++) {
    var dt = new Date(mon);
    dt.setDate(mon.getDate() + i);
    var y = dt.getFullYear(), m = dt.getMonth(), d = dt.getDate();
    var isWE = i >= 5;
    var tod = pipdIsRefToday(y, m, d);
    var pip = pipdIsPipStart(y, m, d);
    var data = pipdGetDayData(y, m, d);
    if (data) { tA += data.appts || 0; tV += data.visits || 0; tO += data.orders || 0; tS += data.sales || 0; tR += data.rev || 0; }
    var cls = 'pipd-wd' + (tod ? ' today' : '') + (pip ? ' pip' : '') + (isWE ? ' weekend' : '');
    var inner = '<div class="pipd-wd-date">' + d + '<span style="font-size:13px;color:#aaa;font-weight:400">일</span>';
    if (tod) inner += '<span style="width:6px;height:6px;border-radius:50%;background:#185FA5;display:inline-block;margin-left:2px"></span>';
    if (pip) inner += '<span style="font-size:11px;color:#A32D2D;font-weight:700">PIP</span>';
    inner += '</div>';
    if (data) {
      var h = function (v, t) { return v >= t ? 'h' : 'm'; };
      inner += '<div class="pipd-wm"><span class="pipd-wm-l">Appts</span><span class="pipd-wm-v ' + h(data.appts, T.appts) + '">' + data.appts + '</span></div>';
      inner += '<div class="pipd-wm"><span class="pipd-wm-l">Visits</span><span class="pipd-wm-v ' + h(data.visits, T.visits) + '">' + data.visits + '</span></div>';
      inner += '<div class="pipd-wm"><span class="pipd-wm-l">Trials</span><span class="pipd-wm-v ' + h(data.orders, T.orders) + '">' + data.orders + '</span></div>';
      inner += '<div class="pipd-wm"><span class="pipd-wm-l">Sales</span><span class="pipd-wm-v ' + h(data.sales, T.sales) + '">' + data.sales + '</span></div>';
    } else inner += '<div style="font-size:13px;color:#ddd;margin-top:8px">영업없음</div>';
    html += '<div class="' + cls + '">' + inner + '</div>';
  }
  html += '</div>';
  var wT = { appts: T.appts * 7, visits: T.visits * 7, orders: T.orders * 7, sales: T.sales * 7, rev: Math.round(T.rev * 7 * 10) / 10 };
  var cc2 = function (v, t) { return v >= t ? '#3B6D11' : '#A32D2D'; };
  html += '<div class="pipd-wsum" style="margin-top:8px;flex-wrap:wrap">' +
    '<div class="pipd-ws-item"><div class="pipd-ws-l">주간 Appts</div><div class="pipd-ws-v" style="color:' + cc2(tA, wT.appts) + '">' + tA + '건 <span style="font-size:13px;color:#aaa">/ 목표 ' + wT.appts + '건</span></div></div>' +
    '<div class="pipd-ws-item"><div class="pipd-ws-l">주간 Visits</div><div class="pipd-ws-v" style="color:' + cc2(tV, wT.visits) + '">' + tV + '건 <span style="font-size:13px;color:#aaa">/ 목표 ' + wT.visits + '건</span></div></div>' +
    '<div class="pipd-ws-item"><div class="pipd-ws-l">주간 Orders (시착)</div><div class="pipd-ws-v" style="color:' + cc2(tO, wT.orders) + '">' + tO + '건 <span style="font-size:13px;color:#aaa">/ 목표 ' + wT.orders + '건</span></div></div>' +
    '<div class="pipd-ws-item"><div class="pipd-ws-l">주간 Sales</div><div class="pipd-ws-v" style="color:' + cc2(tS, wT.sales) + '">' + tS + '건 <span style="font-size:13px;color:#aaa">/ 목표 ' + wT.sales + '건</span></div></div>' +
    '<div class="pipd-ws-item"><div class="pipd-ws-l">주간 Revenue</div><div class="pipd-ws-v" style="color:' + cc2(tR, wT.rev) + '">₩' + tR + 'M <span style="font-size:13px;color:#aaa">/ 목표 ₩' + wT.rev + 'M</span></div></div>' +
    '<div class="pipd-ws-item"><div class="pipd-ws-l">일 평균 Appts</div><div class="pipd-ws-v">' + (tA / 7).toFixed(1) + '건 <span style="font-size:13px;color:#854F0B">목표 ' + T.appts + '건</span></div></div></div>';
  var body = pipdGi('pipd-cal-body');
  if (body) body.innerHTML = html;
}
function pipdRenderDaily() {
  var T = pipdCalTargets();
  var y = pipdCalYear, m = pipdCalMonth, d = pipdCalDay;
  var dl = pipdGi('pipd-day-label');
  if (dl) dl.textContent = y + '년 ' + (m + 1) + '월 ' + d + '일';
  var data = pipdGetDayData(y, m, d) || { appts: 0, visits: 0, orders: 0, sales: 0, rev: 0, bin: 0 };
  var tod = pipdIsRefToday(y, m, d);
  var pip = pipdIsPipStart(y, m, d);
  var isFut = data.future || (!pipdIsPast(y, m, d) && !tod);
  var items = [
    { l: 'Appts', v: data.appts, t: T.appts, unit: '건', pre: '' },
    { l: 'Visits', v: data.visits, t: T.visits, unit: '건', pre: '' },
    { l: 'Trials (시착·Orders)', v: data.orders, t: T.orders, unit: '건', pre: '' },
    { l: 'Sales', v: data.sales, t: T.sales, unit: '건', pre: '' },
    { l: 'Revenue', v: data.rev, t: T.rev, unit: 'M', pre: '₩' },
    { l: 'BIN Rate', v: data.bin || 0, t: T.bin, unit: '', pre: '' }
  ];
  var note = '';
  if (pip) note = '<div style="padding:8px 12px;background:#FCEBEB;border-radius:7px;font-size:14px;color:#A32D2D;margin-bottom:8px">🔴 PIP 시작일 — 부분월로 집계</div>';
  if (tod) note = '<div style="padding:8px 12px;background:#EBF3FC;border-radius:7px;font-size:14px;color:#185FA5;margin-bottom:8px">📍 기준일(D) — 일별 상세</div>';
  var cards = items.map(function (it) {
    var pct = it.t > 0 ? Math.min(Math.round((it.v / it.t) * 100), 100) : 0;
    var col = pct >= 100 ? '#1D9E75' : pct >= 70 ? '#EF9F27' : '#E24B4A';
    return '<div class="pipd-dv-card"><div class="pipd-dv-l">' + it.l + '</div><div class="pipd-dv-v" style="color:' + (isFut ? '#185FA5' : col) + '">' + it.pre + it.v + it.unit + '</div><div class="pipd-dv-s">목표 ' + it.pre + it.t + it.unit + ' · ' + pct + '% 달성</div><div class="pipd-dv-prog"><div class="pipd-dv-fill" style="width:' + pct + '%;background:' + col + '"></div></div></div>';
  }).join('');
  var body = pipdGi('pipd-cal-body');
  if (body) {
    body.innerHTML = '<div style="display:flex;flex-direction:column;gap:8px">' + note + '<div style="font-size:17px;font-weight:700;color:#333;padding-bottom:8px;border-bottom:1px solid #EEE">' + y + '년 ' + (m + 1) + '월 ' + d + '일 ' + '일월화수목금토'[new Date(y, m, d).getDay()] + '요일</div><div class="pipd-dv-grid">' + cards + '</div></div>';
  }
}
function pipdInitCalendar(refY, refM0, refD) {
  pipdCalDetailMode = false;
  pipdCalYear = refY;
  pipdCalMonth = refM0;
  pipdCalDay = refD;
  pipdCalWeekOffset = 0;
  var btn = pipdGi('pipd-btn-cal-toggle');
  var main = pipdGi('pipd-cal-panel-main');
  var det = pipdGi('pipd-cal-panel-detail');
  if (btn) btn.textContent = '세부내용 보기';
  if (main) main.style.display = 'block';
  if (det) det.style.display = 'none';
  var pn = pipdGi('pipd-pip-note');
  var C = window.__pipDetailCtx;
  var mon = (C && C.calendar_monitor_months) || [];
  var pip = (C && C.calendar_pip_months) || [];
  if (pn) {
    if (pip.length) {
      pn.textContent = '선정: ' + mon[0] + '~' + mon[mon.length > 3 ? 2 : mon.length - 1]
        + '  |  PIP 모니터링: ' + pip[0] + '~' + pip[pip.length - 1];
    } else {
      pn.textContent = mon.length ? ('모니터링 ' + mon[0] + ' ~ ' + mon[mon.length - 1])
        : ('PIP ' + (C && C.pip_start_m1 || 1) + '/' + (C && C.pip_start_d || 1));
    }
  }
  var wn = pipdGi('pipd-week-nav'), dn = pipdGi('pipd-day-nav');
  if (wn) wn.style.display = 'none';
  if (dn) dn.style.display = 'none';
  pipdRenderCal();
  /* 담당자 전환 시에도 새 DOM 요소에 리스너를 재등록하도록 플래그 초기화 */
  _pipdCalScrollReady = false;
  pipdSetupCalScroll();
}
/* ===== PIP 퍼널 진단 / 월별 목표 / 시나리오 비교 ===== */
var _pipdScChart = null;
var pipdMonthActuals = [null, null, null];
var WD_PIPD = 20;

function pipdBCol(p) { return p >= 100 ? '#1D9E75' : p >= 75 ? '#EF9F27' : '#E24B4A'; }
function pipdPctCalc(v, t) { if (!t) return 0; return Math.min(Math.round(v / t * 100), 100); }

function pipdCalcFunnelRev(baseAppts, cr2, vr, or_, sr, abvM) {
  var a = baseAppts * cr2 / 100;
  var v = a * vr / 100;
  var t = v * or_ / 100;
  var s = t * sr / 100;
  return {
    appts:  Math.round(a * 10) / 10,
    visits: Math.round(v * 10) / 10,
    trials: Math.round(t * 10) / 10,
    sales:  Math.round(s * 10) / 10,
    rev:    Math.round(s * abvM * 10) / 10
  };
}

function pipdRenderFunnel() {
  var C = window.__pipDetailCtx;
  if (!C) return;
  var act = C.act || {}, td = C.td || {};
  var steps = [
    {key:'cr2', label:'Lead → 예약', stage:'CR2 (예약전환)', act:act.cr2||0, tgt:td.cr2||50, unit:'%',
     good:'예약 전환 양호', bad:'리드 초기 응대 개선 / Recall 비율 분석'},
    {key:'vr',  label:'예약 → 방문', stage:'VR (방문율)',    act:act.vr||0,  tgt:td.vr||55,  unit:'%',
     good:'방문율 양호', bad:'예약 확인 콜 강화 / 방문 동기 부여'},
    {key:'or_', label:'방문 → 시착', stage:'OR (시착율)',    act:act.or_||0, tgt:td.or_||60, unit:'%',
     good:'시착율 양호', bad:'시착 체험 장점 어필 / 장벽 제거 필요'},
    {key:'sr',  label:'시착 → 판매', stage:'SR (판매전환)', act:act.sr||0,  tgt:td.sr||50,  unit:'%',
     good:'판매전환 양호', bad:'클로징 스크립트 강화 / 구매 확신 유도'},
    {key:'abv', label:'판매 단가',    stage:'ABV (객단가)', act:act.abv_m||0, tgt:td.abv_m||2.5, unit:'M',
     good:'단가 양호', bad:'양이 판매 권장 / 프리미엄 모델 추천'},
  ];
  var gaps = steps.map(function(s) { return s.tgt > 0 ? (s.tgt - s.act) / s.tgt : 0; });
  var maxIdx = 0;
  gaps.forEach(function(g, i) { if (g > gaps[maxIdx]) maxIdx = i; });
  var html = '';
  steps.forEach(function(s, i) {
    var p = pipdPctCalc(s.act, s.tgt);
    var col = pipdBCol(p);
    var isMain = i === maxIdx;
    var cls = p >= 100 ? 'ok' : p >= 80 ? 'warn' : 'prob';
    var issTag = isMain
      ? '<div class="pipd-fn-issue p">🔴 집중: ' + s.bad.split(' / ')[0] + '</div>'
      : (p < 90 ? '<div class="pipd-fn-issue w">⚠ ' + s.bad.split(' / ')[0] + '</div>'
                : '<div class="pipd-fn-issue g">✓ ' + s.good + '</div>');
    html += '<div class="pipd-fn-step"><div class="pipd-fn-box ' + cls + '">'
      + '<div class="pipd-fn-stage">' + s.stage + '</div>'
      + '<div class="pipd-fn-val" style="color:' + col + '">' + s.act.toFixed(1) + s.unit + '</div>'
      + '<div class="pipd-fn-vs">목표 <strong>' + s.tgt + s.unit + '</strong> · ' + p + '%</div>'
      + '<div class="pipd-fn-bar"><div class="pipd-fn-fill" style="width:' + p + '%;background:' + col + '"></div></div>'
      + '<div style="font-size:13px;color:#666;margin-top:4px">' + s.label + '</div>'
      + issTag + '</div></div>';
    if (i < steps.length - 1) html += '<div class="pipd-fn-arr">›</div>';
  });
  var fnRow = document.getElementById('pipd-fn-row');
  if (fnRow) fnRow.innerHTML = html;
  var fs = steps[maxIdx];
  var badge = document.getElementById('pipd-fn-focus-badge');
  if (badge) badge.innerHTML = '<span class="badge b-r" style="font-size:14px;padding:4px 10px">🔴 핵심 문제: ' + fs.stage + ' ' + fs.act.toFixed(1) + fs.unit + ' → 목표 ' + fs.tgt + fs.unit + '</span>';
  var insightTexts = [
    '예약 수 자체가 부족합니다. Lead 확보 및 초기 상담 품질을 높여 예약 전환율을 개선해야 합니다.',
    '예약은 충분하지만 실제 방문으로 이어지지 않습니다. 예약 후 방문 확인 콜 및 방문 동기 부여 멘트를 강화하세요.',
    '방문은 이뤄지지만 시착으로 연결이 약합니다. 방문 중 체험의 장점을 적극 어필하고 장벽을 낮추세요.',
    '시착 수는 충분하지만 구매 전환이 낮습니다. 시착 기간 클로징 멘트와 팔로업을 강화하세요.',
    '매출 건수는 나오지만 단가가 낮습니다. 양이 착용 및 프리미엄 모델 추천 역량을 높이세요.',
  ];
  var insight = document.getElementById('pipd-fn-insight');
  if (insight) insight.innerHTML = '<div style="padding:10px 14px;background:#FFF5F5;border:1px solid #F7C1C1;border-radius:8px;font-size:15px;color:#5a1a1a;line-height:1.7">'
    + '<strong>📋 PIP 선정 원인</strong>: ' + insightTexts[maxIdx] + '<br>'
    + '<span style="font-size:14px;color:#A32D2D">→ 집중 개선 액션: ' + fs.bad + '</span></div>';
  // ── 월별 세부 테이블 + TTFA/MT ──────────────────────────────
  var mb = (act.monthly_breakdown) || [];
  var mnDiv = document.getElementById('pipd-fn-monthly');
  if (mnDiv && mb.length > 0) {
    var tgt = C.td || {};
    var tv = tgt.vr || 55, to = tgt.or_ || 60, ts = tgt.sr || 50;
    var bench = (typeof pipBench === 'function') ? pipBench() : {};
    var benchMb = bench.monthly_breakdown || [];  /* 월별 non-PIP 평균 배열 */

    /* 건수: actual (평균)  — 실제값에 색 적용 */
    function fmtCnt(v, b) {
      if (b == null) return String(v);
      var col = v >= b ? '#1D9E75' : v >= b * 0.8 ? '#EF9F27' : '#E24B4A';
      return '<span style="color:' + col + ';font-weight:700">' + String(v) + '</span>'
           + '<span style="font-size:12px;color:#aaa"> (' + b + ')</span>';
    }
    function fmtRate(v, b, unit) {
      if (b == null) return v + unit;
      var col = v >= b ? '#1D9E75' : v >= b * 0.85 ? '#EF9F27' : '#E24B4A';
      return '<span style="color:' + col + ';font-weight:700">' + String(v) + unit + '</span>'
           + '<span style="font-size:12px;color:#aaa"> (' + b + unit + ')</span>';
    }
    var tcr2 = (tgt.cr2 || 45);
    var th = function(label, sub) { return '<th style="padding:4px 6px;text-align:center;color:#888;font-size:13px">' + label + (sub ? '<br><span style="font-weight:400;color:#bbb;font-size:12px">' + sub + '</span>' : '') + '</th>'; };
    var mh = '<div style="font-size:13px;font-weight:700;color:#999;letter-spacing:.04em;text-transform:uppercase;margin-bottom:6px">월별 세부 지표</div>'
      + '<div style="font-size:13px;color:#aaa;margin-bottom:6px">괄호 안: PIP 제외 인원의 평균값</div>'
      + '<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse;font-size:14px;min-width:780px">'
      + '<thead><tr style="border-bottom:2px solid #E5E4DF">'
      + '<th style="text-align:left;padding:4px 8px;color:#888;font-size:13px">월</th>'
      + th('Leads', 'LCD')
      + th('Appts', 'OCD')
      + th('CR2', '목표 ' + tcr2 + '%')
      + th('Visits', 'VD')
      + th('VR', '목표 ' + tv + '%')
      + th('Orders', 'TAD')
      + th('OR', '목표 ' + to + '%')
      + th('Closed_Orders', 'QID')
      + th('Sales', 'QID')
      + th('SR', '목표 ' + ts + '%')
      + th('ABV', 'QID')
      + th('Revenue', 'QID')
      + '</tr></thead><tbody>';
    /* benchMb를 월 라벨로 매핑 (현재월도 최근달 평균으로 대체) */
    var benchByLabel = {};
    benchMb.forEach(function(b) { if (b.label) benchByLabel[b.label] = b; });
    var benchAvg = benchMb.length ? benchMb[benchMb.length-1] : {};  /* 현재월은 마지막달 평균 사용 */
    mb.forEach(function(mo, idx) {
      var bm = benchByLabel[mo.label] || (mo.is_current ? benchAvg : {});
      var bLd  = bm.leads        != null ? bm.leads        : null;
      var bOp  = bm.oppts        != null ? bm.oppts        : null;
      var bCr2 = null; /* 벤치 CR2는 미제공 */
      var bVi  = bm.visits       != null ? bm.visits       : null;
      var bOrd = bm.orders       != null ? bm.orders       : null;
      var bCo  = bm.closed_orders!= null ? bm.closed_orders: null;
      var bSal = bm.sales        != null ? bm.sales        : null;
      var bVr  = bm.vr           != null ? Math.round(bm.vr  * 10) / 10 : null;
      var bOr  = bm.or_          != null ? Math.round(bm.or_ * 10) / 10 : null;
      var bSr  = bm.sr           != null ? Math.round(bm.sr  * 10) / 10 : null;
      var bAbv = bm.abv_m        != null ? bm.abv_m        : null;
      var bRev = bm.revenue_m    != null ? bm.revenue_m    : null;
      var vc  = mo.vr  >= tv  ? '#1D9E75' : mo.vr  >= tv  * 0.85 ? '#EF9F27' : '#E24B4A';
      var oc  = mo.or_ >= to  ? '#1D9E75' : mo.or_ >= to  * 0.85 ? '#EF9F27' : '#E24B4A';
      var sc  = mo.sr  >= ts  ? '#1D9E75' : mo.sr  >= ts  * 0.85 ? '#EF9F27' : '#E24B4A';
      var cc2 = (mo.cr2||0) >= tcr2 ? '#1D9E75' : (mo.cr2||0) >= tcr2*0.85 ? '#EF9F27' : '#E24B4A';
      var rowStyle = mo.is_current ? 'background:#F0F6FF;' : '';
      mh += '<tr style="border-bottom:1px solid #F4F3EF;' + rowStyle + '">'
        + '<td style="padding:5px 8px;font-weight:700;color:#444">' + mo.label + '</td>'
        + '<td style="padding:5px 6px;text-align:center;color:#555">' + fmtCnt(mo.leads||0, bLd) + '</td>'
        + '<td style="padding:5px 6px;text-align:center;color:#555">' + fmtCnt(mo.oppts, bOp) + '</td>'
        + '<td style="padding:5px 6px;text-align:center;font-weight:700;color:' + cc2 + '">' + fmtRate(mo.cr2||0, bCr2, '%') + '</td>'
        + '<td style="padding:5px 6px;text-align:center;color:#555">' + fmtCnt(mo.visits, bVi) + '</td>'
        + '<td style="padding:5px 6px;text-align:center;font-weight:700;color:' + vc + '">' + fmtRate(mo.vr, bVr, '%') + '</td>'
        + '<td style="padding:5px 6px;text-align:center;color:#555">' + fmtCnt(mo.orders, bOrd) + '</td>'
        + '<td style="padding:5px 6px;text-align:center;font-weight:700;color:' + oc + '">' + fmtRate(mo.or_, bOr, '%') + '</td>'
        + '<td style="padding:5px 6px;text-align:center;color:#555">' + fmtCnt(mo.closed_orders||0, bCo) + '</td>'
        + '<td style="padding:5px 6px;text-align:center;color:#555">' + fmtCnt(mo.sales, bSal) + '</td>'
        + '<td style="padding:5px 6px;text-align:center;font-weight:700;color:' + sc + '">' + fmtRate(mo.sr, bSr, '%') + '</td>'
        + '<td style="padding:5px 6px;text-align:center;color:#555">' + fmtRate(mo.abv_m, bAbv, 'M') + '</td>'
        + '<td style="padding:5px 6px;text-align:center;font-weight:700;color:#185FA5">' + fmtCnt(mo.revenue_m, bRev) + '</td>'
        + '</tr>';
    });
    mh += '</tbody></table></div>';
    var ttfa = act.ttfa_days, mt = act.mt_days;
    if (ttfa != null || mt != null) {
      mh += '<div style="margin-top:8px;display:flex;gap:20px;flex-wrap:wrap;font-size:14px;padding:6px 0">';
      if (ttfa != null) mh += '<div><span style="color:#888">TTFA (예약→방문 평균)</span> <strong style="color:#185FA5">' + ttfa + '일</strong></div>';
      if (mt   != null) mh += '<div><span style="color:#888">MT (시착→종료 평균)</span> <strong style="color:#185FA5">' + mt + '일</strong></div>';
      mh += '</div>';
    }
    mnDiv.innerHTML = mh;
  }
}

function pipdMonthSums(year, month0) {
  var daysInMonth = new Date(year, month0 + 1, 0).getDate();
  var s = {appts:0, visits:0, orders:0, sales:0, rev:0};
  for (var d = 1; d <= daysInMonth; d++) {
    var day = pipdGetDayData(year, month0, d);
    if (day && !day.future) {
      s.appts  += day.appts  || 0;
      s.visits += day.visits || 0;
      s.orders += day.orders || 0;
      s.sales  += day.sales  || 0;
      s.rev    += day.rev    || 0;
    }
  }
  s.rev = Math.round(s.rev * 10) / 10;
  return s;
}
function pipdRenderGoals() {
  var C = window.__pipDetailCtx;
  if (!C) return;
  var td = C.td || {};
  function gv(id) { var e = document.getElementById(id); return e ? parseFloat(e.value) || 0 : 0; }
  var mRev = gv('pipd-g-rev') || td.rev_m || 70;
  var gABV = gv('pipd-g-abv') || (td.abv_m ? Math.round(td.abv_m * 100) : 250);
  var gSR  = gv('pipd-g-sr')  || td.sr   || 50;
  var gOR  = gv('pipd-g-or')  || td.or_  || 60;
  var gVR  = gv('pipd-g-vr')  || td.vr   || 75;
  var gCR2 = gv('pipd-g-cr2') || td.cr2  || 25;
  var abvM = gABV / 100;

  var reqSales  = Math.round(mRev / abvM * 10) / 10;
  var reqTrials = Math.round(reqSales  / (gSR  / 100));
  var reqVisits = Math.round(reqTrials / (gOR  / 100));
  var reqAppts  = Math.round(reqVisits / (gVR  / 100));
  var reqLeads  = Math.round(reqAppts  / (gCR2 / 100));

  var goals = [mRev, mRev, mRev];
  if (pipdMonthActuals[0] !== null) {
    var rem = mRev * 3 - pipdMonthActuals[0];
    goals[1] = Math.round(rem / 2 * 10) / 10;
    goals[2] = Math.round(rem / 2 * 10) / 10;
  }
  if (pipdMonthActuals[1] !== null) {
    var rem2 = mRev * 3 - (pipdMonthActuals[0] || mRev) - pipdMonthActuals[1];
    goals[2] = Math.round(Math.max(0, rem2) * 10) / 10;
  }

  var startM = C.pip_start_m1 || 1;
  var pipY = C.pip_start_year || new Date().getFullYear();
  var pipM0 = (C.pip_start_month_1 || startM) - 1;
  var pipMonths = [startM + '월 (D+1~30)', (startM + 1) + '월 (D+31~60)', (startM + 2) + '월 (D+61~90)'];

  /* 월별 실적 집계 (캘린더 데이터 합산) */
  var mSums = [];
  for (var si = 0; si < 3; si++) {
    var sy = pipY, sm0 = pipM0 + si;
    if (sm0 >= 12) { sy++; sm0 -= 12; }
    mSums.push(pipdMonthSums(sy, sm0));
  }

  var html = pipMonths.map(function(label, i) {
    var isCur = i === 0;
    var isDone = pipdMonthActuals[i] !== null;
    var cls = isDone ? 'done' : isCur ? 'cur' : '';
    var act = pipdMonthActuals[i];
    var achPct = act !== null ? pipdPctCalc(act, goals[i]) : null;
    var thisRev = goals[i];
    var tSales  = Math.round(thisRev / abvM * 10) / 10;
    var tTrials = Math.round(tSales  / (gSR  / 100));
    var tVisits = Math.round(tTrials / (gOR  / 100));
    var tAppts  = Math.round(tVisits / (gVR  / 100));
    var tLeads  = Math.round(tAppts  / (gCR2 / 100));
    var tdRevD  = Math.round(thisRev / WD_PIPD * 10) / 10;
    var tdSalesD= Math.round(tSales  / WD_PIPD * 10) / 10;
    var tdApptsD= Math.max(1, Math.round(tAppts / WD_PIPD));
    var bBadge  = isDone ? '<span class="badge b-g">완료</span>' : isCur ? '<span class="badge b-b">진행중</span>' : '<span class="badge b-gray">예정</span>';

    /* 현재 실적 */
    var ms = mSums[i] || {};
    var hasData = (ms.sales > 0 || ms.visits > 0 || ms.appts > 0) || isDone;
    var curRev    = isDone && act !== null ? act : ms.rev;
    var curSales  = ms.sales  || 0;
    var curVisit  = ms.visits || 0;
    var curAppts  = ms.appts  || 0;
    var curOrders = ms.orders || 0;

    /* 고정 월별 목표 (70M 기준, 20영업일) */
    var fxSales  = 28;
    var fxOrders = 56;
    var fxVisits = 93;
    var fxAppts  = 169;

    function gapFmt(cur, tgt) {
      if (!hasData) return '<span class="na">—</span>';
      var g = Math.round((cur - tgt) * 10) / 10;
      var cls2 = g >= 0 ? 'gp' : 'gn';
      return '<span class="' + cls2 + '">' + (g >= 0 ? '+' : '') + g + '</span>';
    }
    function curFmt(v, suffix) {
      return hasData ? v + suffix : '<span class="na">—</span>';
    }

    var revGap   = Math.round((curRev   - goals[i]) * 10) / 10;
    var revGapCls= revGap >= 0 ? 'gp' : 'gn';

    var tblHtml = '<table class="pipd-gm-tbl">'
      + '<tr><th>지표</th><th>목표</th><th>현재</th><th>갭</th></tr>'
      + '<tr><td>매출</td><td>₩' + goals[i] + 'M</td><td>' + (hasData ? '₩' + curRev + 'M' : '<span class="na">—</span>') + '</td><td>' + (hasData ? '<span class="' + revGapCls + '">' + (revGap >= 0 ? '+' : '') + revGap + 'M</span>' : '<span class="na">—</span>') + '</td></tr>'
      + '<tr><td>Sales</td><td>' + fxSales + '건</td><td>' + curFmt(curSales, '건') + '</td><td>' + gapFmt(curSales, fxSales) + '</td></tr>'
      + '<tr><td>Orders</td><td>' + fxOrders + '건</td><td>' + curFmt(curOrders, '건') + '</td><td>' + gapFmt(curOrders, fxOrders) + '</td></tr>'
      + '<tr><td>Visits</td><td>' + fxVisits + '건</td><td>' + curFmt(curVisit, '건') + '</td><td>' + gapFmt(curVisit, fxVisits) + '</td></tr>'
      + '<tr><td>Appts</td><td>' + fxAppts + '건</td><td>' + curFmt(curAppts, '건') + '</td><td>' + gapFmt(curAppts, fxAppts) + '</td></tr>'
      + '</table>';

    return '<div class="pipd-gm-card ' + cls + '">'
      + '<div class="pipd-gm-title">' + bBadge + ' ' + label + '</div>'
      + tblHtml
      + '<div class="pipd-gm-derived" style="margin-top:7px">일별 목표 (÷' + WD_PIPD + '일) → Revenue <strong>₩' + tdRevD + 'M</strong> · Sales <strong>' + tdSalesD + '건</strong> · Appts <strong>' + tdApptsD + '건</strong></div>'
      + (act !== null ? '<div class="pipd-prog" style="margin-top:6px"><div class="pipd-prog-fill" style="width:' + achPct + '%;background:' + pipdBCol(achPct) + '"></div></div><div style="font-size:14px;font-weight:700;color:' + pipdBCol(achPct) + ';margin-top:3px">실적 ₩' + act + 'M · ' + achPct + '% 달성</div>' : '')
      + (isCur ? '<div style="font-size:13px;color:#185FA5;margin-top:6px">실적 입력: <input class="pipd-sc-inp" type="number" placeholder="—" style="width:70px;font-size:14px" onchange="pipdMonthActuals[' + i + ']=parseFloat(this.value)||null;pipdRenderGoals()"> M</div>' : '')
      + '</div>';
  }).join('');
  var gmGrid = document.getElementById('pipd-gm-grid');
  if (gmGrid) gmGrid.innerHTML = html;
  pipdRenderScenario();
}

function pipdRenderScenario() {
  var C = window.__pipDetailCtx;
  if (!C) return;
  var act = C.act || {}, td = C.td || {};
  function gvNum(id, def) { var e = document.getElementById(id); return e ? (parseFloat(e.value) !== 0 ? parseFloat(e.value) || def : 0) : def; }
  var curGoal = gvNum('pipd-g-rev', td.rev_m || 70);

  /* 월간 예약 건수(baseAppts): monthly_breakdown L3M 실제 평균 oppts 우선, 없으면 VR 역산 */
  /* monthly_breakdown 은 pip_ui.actual 안에 있으므로 C.act 에서 읽어야 함 */
  var _mb = (C.act && C.act.monthly_breakdown) || [];
  var _l3mMb = _mb.filter(function(m){ return !m.is_current; });
  var l3mAppts = _l3mMb.length > 0
    ? Math.round(_l3mMb.reduce(function(s,m){ return s+(m.oppts||0); }, 0) / _l3mMb.length)
    : ((act.visits > 0 && act.vr > 0)
        ? Math.round(act.visits / (act.vr / 100))
        : (C.monthly_goals && C.monthly_goals.m_appts ? C.monthly_goals.m_appts : 20));
  var baseAppts = l3mAppts;

  var base = {cr2: act.cr2||0, vr: act.vr||0, or_: act.or_||0, sr: act.sr||0, abvM: act.abv_m||2.5};

  /* 시나리오 B/C — 각 지표 독립 설정 (Appts 포함) */
  var bAppts = gvNum('pipd-sc-b-appts', baseAppts);
  var bRates = {
    cr2:  gvNum('pipd-sc-b-cr2', base.cr2),
    vr:   gvNum('pipd-sc-b-vr',  base.vr),
    or_:  gvNum('pipd-sc-b-or',  base.or_),
    sr:   gvNum('pipd-sc-b-sr',  base.sr),
    abvM: gvNum('pipd-sc-b-abv', base.abvM * 100) / 100
  };
  var cAppts = gvNum('pipd-sc-c-appts', baseAppts);
  var cRates = {
    cr2:  gvNum('pipd-sc-c-cr2', base.cr2),
    vr:   gvNum('pipd-sc-c-vr',  base.vr),
    or_:  gvNum('pipd-sc-c-or',  base.or_),
    sr:   gvNum('pipd-sc-c-sr',  base.sr),
    abvM: gvNum('pipd-sc-c-abv', base.abvM * 100) / 100
  };

  var cur = pipdCalcFunnelRev(baseAppts, base.cr2,    base.vr,    base.or_,    base.sr,    base.abvM);
  var scB = pipdCalcFunnelRev(bAppts,    bRates.cr2,  bRates.vr,  bRates.or_,  bRates.sr,  bRates.abvM);
  var scC = pipdCalcFunnelRev(cAppts,    cRates.cr2,  cRates.vr,  cRates.or_,  cRates.sr,  cRates.abvM);

  function pctBadge(v, t) {
    var p = pipdPctCalc(v, t);
    return '<span class="badge ' + (p>=100?'b-g':p>=75?'b-y':'b-r') + '" style="margin-left:4px">' + p + '%</span>';
  }

  /* ── 일별 예약→종료 기반 월별 예측 ─────────── */
  var l3mRevM = (C.d && C.d.month_actual)
    ? C.d.month_actual.map(function(v) { return Math.round(v / 1e6 * 10) / 10; })
    : [0, 0, 0];
  var last = l3mRevM[2] || 0;
  var trialsNowSc = C.trialsNow || 0;
  var atcDays = (act.appt_to_close_days != null ? act.appt_to_close_days : 45);
  var T_sc = pipdCalTargets();
  var dAppts = T_sc.appts || 8;

  function countWorkdays(s, e) {
    var n = 0, d = new Date(s.getFullYear(), s.getMonth(), s.getDate()),
        ed = new Date(e.getFullYear(), e.getMonth(), e.getDate());
    while (d <= ed) { var dw = d.getDay(); if (dw >= 1 && dw <= 5) n++; d.setDate(d.getDate() + 1); }
    return n;
  }

  /* X축 레이블 (YYMM 형식) */
  function addM(y, m1, delta) { var dt = new Date(y, m1 - 1 + delta, 1); return {y:dt.getFullYear(), m:dt.getMonth()+1}; }
  function yymm(ym) { return String(ym.y).slice(2) + (ym.m < 10 ? '0' + ym.m : String(ym.m)); }
  var rY = C.ref_year || new Date().getFullYear();
  var rM = C.ref_month_1 || new Date().getMonth() + 1;
  var pY = C.pip_start_year || rY;
  var pM = C.pip_start_month_1 || rM;
  var labels = [
    yymm(addM(rY, rM, -3)), yymm(addM(rY, rM, -2)), yymm(addM(rY, rM, -1)),
    yymm({y:pY, m:pM}), yymm(addM(pY, pM, 1)), yymm(addM(pY, pM, 2))
  ];

  /* 현재월 실적 (이미 기록된 매출) + 잔여 영업일 */
  var mSums0 = pipdMonthSums(pY, pM - 1);
  var actualRev0 = mSums0 ? (Math.round((mSums0.rev || 0) * 10) / 10) : 0;
  var refDay = C.refD || 1;
  var pipM1End = new Date(pY, pM, 0);
  var remWD0 = countWorkdays(new Date(pY, pM - 1, refDay + 1), pipM1End);

  function projPipeline(rates, apptsCnt) {
    var cr3R = (rates.vr / 100) * (rates.or_ / 100) * (rates.sr / 100);
    var dailyAppts = (apptsCnt || baseAppts) / 22;  /* 월 영업일 기준 일별 예약 */
    var pipStart = new Date(pY, pM - 1, 1);
    var revArr = [];
    for (var mi = 0; mi < 3; mi++) {
      var my = pY, mm = pM + mi;
      if (mm > 12) { my++; mm -= 12; }
      var mStart = new Date(my, mm - 1, 1), mEnd = new Date(my, mm, 0);
      var rawWS = new Date(mStart); rawWS.setDate(rawWS.getDate() - Math.round(atcDays));
      var rawWE = new Date(mEnd);   rawWE.setDate(rawWE.getDate() - Math.round(atcDays));
      var winS = rawWS < pipStart ? pipStart : rawWS, winE = rawWE;
      var wd = (winS <= winE) ? countWorkdays(winS, winE) : 0;
      var rev = Math.round(wd * dailyAppts * cr3R * rates.abvM * 10) / 10;
      if (mi === 0) {
        /* D+30: 현재 파이프라인(trials) 기여 + 이미 기록된 이번달 실적 */
        var trialRev = Math.round(trialsNowSc * (rates.sr / 100) * rates.abvM * 10) / 10;
        rev = Math.round((rev + trialRev + actualRev0) * 10) / 10;
      }
      revArr.push(rev);
    }
    return revArr;
  }
  var projBase = projPipeline(base, baseAppts), projB = projPipeline(bRates, bAppts), projC = projPipeline(cRates, cAppts);
  /* ────────────────────────────────────────────────────────────── */

  /* 변경된 지표 하이라이트 */
  function diffB(key) { return Math.abs((bRates[key]||0) - (base[key]||0)) > 0.05; }
  function diffC(key) { return Math.abs((cRates[key]||0) - (base[key]||0)) > 0.05; }

  var sectLabel0 = '월별 예상 매출 (예약→종료 평균 ' + Math.round(atcDays) + '일 기준'
    + (remWD0 > 0 ? ' · ' + labels[3] + ' 잔여 ' + remWD0 + ' 영업일' : '') + ')';

  /* diffAppts: 시나리오 Appts vs L3M 기준 */
  var diffApptB = Math.abs(bAppts - baseAppts) >= 1;
  var diffApptC = Math.abs(cAppts - baseAppts) >= 1;

  var rows = [
    {sect:'예약수 (월간)'},
    {lbl:'Appts',
     cur:'L3M 기반 ' + baseAppts + '건',
     b:bAppts + '건',  c:cAppts + '건',
     isAppts:true, curV:baseAppts, bV:bAppts, cV:cAppts, hB:diffApptB, hC:diffApptC},
    {sect:'전환율'},
    {lbl:'CR2', cur:base.cr2.toFixed(1)+'%',      b:bRates.cr2.toFixed(1)+'%',    c:cRates.cr2.toFixed(1)+'%',    hB:diffB('cr2'),  hC:diffC('cr2')},
    {lbl:'VR',  cur:base.vr.toFixed(1)+'%',       b:bRates.vr.toFixed(1)+'%',     c:cRates.vr.toFixed(1)+'%',     hB:diffB('vr'),   hC:diffC('vr')},
    {lbl:'OR',  cur:base.or_.toFixed(1)+'%',      b:bRates.or_.toFixed(1)+'%',    c:cRates.or_.toFixed(1)+'%',    hB:diffB('or_'),  hC:diffC('or_')},
    {lbl:'SR',  cur:base.sr.toFixed(1)+'%',       b:bRates.sr.toFixed(1)+'%',     c:cRates.sr.toFixed(1)+'%',     hB:diffB('sr'),   hC:diffC('sr')},
    {lbl:'ABV', cur:'₩'+base.abvM.toFixed(1)+'M', b:'₩'+bRates.abvM.toFixed(1)+'M', c:'₩'+cRates.abvM.toFixed(1)+'M', hB:diffB('abvM'), hC:diffC('abvM')},
    {sect:'예상 결과 (월간 기준)'},
    {lbl:'Visits', cur:cur.visits+'건', b:scB.visits+'건', c:scC.visits+'건', isNum:true, curV:cur.visits, bV:scB.visits, cV:scC.visits},
    {lbl:'Trials', cur:cur.trials+'건', b:scB.trials+'건', c:scC.trials+'건', isNum:true, curV:cur.trials, bV:scB.trials, cV:scC.trials},
    {lbl:'Sales',  cur:cur.sales+'건',  b:scB.sales+'건',  c:scC.sales+'건',  isNum:true, curV:cur.sales,  bV:scB.sales,  cV:scC.sales},
    {sect: sectLabel0},
    {lbl:labels[3]+' D+30', cur:'₩'+projBase[0]+'M', b:'₩'+projB[0]+'M', c:'₩'+projC[0]+'M',
     isRev:true, curV:projBase[0], bV:projB[0], cV:projC[0]},
    {lbl:labels[4]+' D+60', cur:'₩'+projBase[1]+'M', b:'₩'+projB[1]+'M', c:'₩'+projC[1]+'M', isRev:true, curV:projBase[1], bV:projB[1], cV:projC[1]},
    {lbl:labels[5]+' D+90', cur:'₩'+projBase[2]+'M', b:'₩'+projB[2]+'M', c:'₩'+projC[2]+'M', isRev:true, curV:projBase[2], bV:projB[2], cV:projC[2]},
  ];

  var html = '<thead><tr>'
    + '<th style="width:120px">지표</th>'
    + '<th style="background:#F4F3EF;color:#555">현재 L3M 추세</th>'
    + '<th style="background:#EEF2FF;color:#534AB7">시나리오 B</th>'
    + '<th style="background:#EDFBF5;color:#0F6E56">시나리오 C</th>'
    + '</tr></thead><tbody>';

  rows.forEach(function(r) {
    if (r.sect) { html += '<tr class="sect"><td colspan="4">' + r.sect + '</td></tr>'; return; }
    if (r.isAppts) {
      var dAbB = r.hB ? ' <span style="font-size:12px;color:'+(r.bV>r.curV?'#3B6D11':'#aaa')+'">('+(r.bV>=r.curV?'+':'')+(r.bV-r.curV)+')</span>' : '';
      var dAbC = r.hC ? ' <span style="font-size:12px;color:'+(r.cV>r.curV?'#3B6D11':'#aaa')+'">('+(r.cV>=r.curV?'+':'')+(r.cV-r.curV)+')</span>' : '';
      html += '<tr>'
        + '<td style="font-weight:700;color:#854F0B">' + r.lbl + '</td>'
        + '<td style="background:#FFF8F0;color:#854F0B;font-weight:600">' + r.cur + '</td>'
        + '<td style="background:#EEF2FF"><span style="font-weight:700;color:#534AB7">' + r.b + '</span>' + dAbB + '</td>'
        + '<td style="background:#EDFBF5"><span style="font-weight:700;color:#0F6E56">' + r.c + '</span>' + dAbC + '</td>'
        + '</tr>';
      return;
    }
    var bBg = r.hB ? 'background:#EEF2FF' : 'background:#F5F6FF';
    var cBg = r.hC ? 'background:#EDFBF5' : 'background:#F5FBF8';
    var bSt = r.hB ? 'font-weight:700;color:#534AB7' : 'color:#534AB7';
    var cSt = r.hC ? 'font-weight:700;color:#0F6E56' : 'color:#0F6E56';
    if (r.isRev) {
      var noteHtml = r.note ? ' <span style="font-size:11px;color:#888;font-weight:400">('+r.note+')</span>' : '';
      html += '<tr class="rev-row">'
        + '<td>Revenue' + noteHtml + '</td>'
        + '<td style="background:#F9F8F6">₩' + r.curV + 'M' + pctBadge(r.curV, curGoal) + '</td>'
        + '<td style="' + bBg + '"><span style="' + bSt + '">₩' + r.bV + 'M</span>' + pctBadge(r.bV, curGoal) + '</td>'
        + '<td style="' + cBg + '"><span style="' + cSt + '">₩' + r.cV + 'M</span>' + pctBadge(r.cV, curGoal) + '</td>'
        + '</tr>';
    } else {
      var dB = r.isNum ? ' <span style="font-size:12px;color:' + (r.bV > r.curV ? '#3B6D11' : '#aaa') + '">' + (r.bV >= r.curV ? '+' : '') + Math.round((r.bV - r.curV) * 10) / 10 + '</span>' : '';
      var dC = r.isNum ? ' <span style="font-size:12px;color:' + (r.cV > r.curV ? '#3B6D11' : '#aaa') + '">' + (r.cV >= r.curV ? '+' : '') + Math.round((r.cV - r.curV) * 10) / 10 + '</span>' : '';
      html += '<tr>'
        + '<td>' + r.lbl + '</td>'
        + '<td style="background:#F9F8F6">' + r.cur + '</td>'
        + '<td style="' + bBg + '"><span style="' + bSt + '">' + r.b + '</span>' + dB + '</td>'
        + '<td style="' + cBg + '"><span style="' + cSt + '">' + r.c + '</span>' + dC + '</td>'
        + '</tr>';
    }
  });
  html += '</tbody>';
  var scTbl = document.getElementById('pipd-sc-tbl');
  if (scTbl) scTbl.innerHTML = html;

  /* 상세 탭 영역 초기화 (제거됨) */
  var scDetail = document.getElementById('pipd-sc-detail');
  if (scDetail) scDetail.innerHTML = '';

  /* 차트 */
  if (_pipdScChart) { try { _pipdScChart.destroy(); } catch(e) {} _pipdScChart = null; }
  var canvas = document.getElementById('pipd-sc-chart');
  if (canvas && typeof Chart !== 'undefined') {
    _pipdScChart = new Chart(canvas, {
      type: 'line',
      data: { labels: labels, datasets: [
        {label:'실적 (L3M)', data:[l3mRevM[0],l3mRevM[1],l3mRevM[2],null,null,null],
         borderColor:'#B4B2A9', borderWidth:2, borderDash:[4,3], pointRadius:4, pointBackgroundColor:'#B4B2A9', fill:false, spanGaps:false},
        {label:'현재 추세',  data:[null,null,last].concat(projBase),
         borderColor:'#378ADD', borderWidth:2.5, pointRadius:4, pointBackgroundColor:'#378ADD', fill:false, tension:0.35, spanGaps:false},
        {label:'시나리오 B', data:[null,null,last].concat(projB),
         borderColor:'#534AB7', borderWidth:2.5, pointRadius:4, pointBackgroundColor:'#534AB7', fill:false, tension:0.35, spanGaps:false},
        {label:'시나리오 C', data:[null,null,last].concat(projC),
         borderColor:'#0F6E56', borderWidth:2.5, pointRadius:4, pointBackgroundColor:'#0F6E56', fill:false, tension:0.35, spanGaps:false},
        {label:'월 목표', data:Array(6).fill(curGoal),
         borderColor:'#EF9F27', borderWidth:1.5, borderDash:[5,4], pointRadius:0, fill:false},
      ]},
      options: {
        responsive:true, maintainAspectRatio:false,
        plugins:{
          legend:{display:true, position:'top', labels:{font:{size:11}, color:'#555', boxWidth:14, padding:16}},
          tooltip:{callbacks:{label:function(c){ if(c.raw==null) return ''; return c.dataset.label+': ₩'+c.raw+'M'; }}}
        },
        scales:{
          x:{grid:{display:false}, ticks:{font:{size:10}, color:'#888'}},
          y:{
            min:0,
            max: Math.max(curGoal * 1.4, Math.max.apply(null, projB.concat(projC).filter(function(v){return v!=null;})) * 1.2, 10),
            grid:{color:'#F0EEE8'},
            ticks:{font:{size:10}, color:'#888', callback:function(v){ return '₩'+v+'M'; }}
          }
        },
        layout:{padding:{top:8, right:14, bottom:8}}
      }
    });
  }
}
/* ===== END PIP 퍼널/목표/시나리오 ===== */

function pipdSyncPipTargets() {
  var s = pipdGv('t-sales');
  if (s < 0) s = 0;
  var vr = pipdGv('t-vr'), or_ = pipdGv('t-or'), sr = pipdGv('t-sr');
  var tOrd = sr > 0 ? Math.round((s * 100 / sr) * 10) / 10 : 0;
  var tVis = or_ > 0 ? Math.round((tOrd * 100 / or_) * 10) / 10 : 0;
  var cr3 = (vr * or_ * sr) / 10000;
  var iv = pipdGi('t-visits'), io = pipdGi('t-orders'), ic = pipdGi('t-cr3');
  if (iv) iv.value = tVis;
  if (io) io.value = tOrd;
  if (ic) ic.value = Math.round(cr3 * 100) / 100;
}
function pipdWireCalc() {
  pipdSyncPipTargets();
  pipdCalc();
  ['t-cr2','t-vr','t-or','t-sr','t-sales','t-abv','t-rev','s-cr2','s-vr','s-or','s-sr','s-visits','s-trials','s-abv'].forEach(function (id) {
    var el = pipdGi(id);
    if (!el) return;
    el.addEventListener('input', function () {
      if (id === 't-vr' || id === 't-or' || id === 't-sr' || id === 't-sales') pipdSyncPipTargets();
      pipdCalc();
    });
  });
}
function renderPIPDetailContent(ccName) {
  var pip = findPipRow(ccName);
  var content = document.getElementById('pip-detail-content');
  if (!content) return;
  if (typeof _pipDChart !== 'undefined' && _pipDChart) { try { _pipDChart.destroy(); } catch (e) {} _pipDChart = null; }
  if (!pip) {
    content.innerHTML = '<div class="card"><p style="color:#999;padding:12px">해당 담당자 PIP/접근위험 데이터 없음.</p></div>';
    return;
  }
  var d = pip.detail || {};
  var ui = d.pip_ui || {};
  var sim = d.sim || {};
  var byOwner = getPayload().by_owner || {};
  var mL3m = (byOwner[ccName] || {}).L3M || {};
  var goals = (getPayload().goals_by_period || {}).L3M || {};
  var kind = pip.pip_kind === 'risk' ? '접근 위험' : 'PIP';
  var badgeClass = pip.pip_kind === 'risk' ? 'b-y' : 'b-r';
  var l3mAvg = pip.l3m_rev_month_avg != null ? pip.l3m_rev_month_avg : (pip.l3m_m1 != null ? Math.round((pip.l3m_m1 + pip.l3m_m2 + pip.l3m_m3) / 3) : pip.l3m_revenue);
  var l3mM = l3mAvg != null ? (Number(l3mAvg) / 1e6) : 0;
  var l3mCol = l3mM < 75 ? '#A32D2D' : '#222';
  var trialsNow = sim.pipeline_trials != null ? sim.pipeline_trials : (d.pipeline_trials || 0);
  var pr = d.pipeline_rows || [];
  var plBody = buildPipelineRowsHtml(pr);
  var act = ui.actual || {};
  if (act.revenue_m == null && act.cr2 == null) {
    var v0 = mL3m.visits || 0, o0 = mL3m.orders || 0, s0 = mL3m.sales || 0, msum = ((pip.l3m_m1 || 0) + (pip.l3m_m2 || 0) + (pip.l3m_m3 || 0));
    act = {
      cr2: pip.cr2 || 0, vr: pip.vr || 0, or_: pip.or_ || 0, sr: pip.sr || 0, cr3: pip.cr3 || 0,
      visits: v0 / 3, orders: o0 / 3, sales: s0 / 3,
      abv_m: (pip.abv || 0) / 1e6,
      revenue_m: msum / 3 / 1e6
    };
  }
  var td = ui.targets_default || {};
  var cg = ui.chart || {};
  var ps = ui.pipeline_summary || {};
  var refY = ui.ref_year, refM1 = ui.ref_month_1 || 1, refD = ui.ref_day || 1;
  var callG = ui.call_goal != null ? ui.call_goal : (goals.call_score || 80);
  var callSc = ui.call_score != null ? ui.call_score : (pip.call_monitor != null ? pip.call_monitor : pip.call_count);
  var esc = escapePlCell;
  var kpiPct = function (v) { return (v != null && !isNaN(v)) ? (Number(v).toFixed(1) + '%') : '—'; };
  var defAbvM = td.abv_m != null ? td.abv_m : 2.5;
  var defRevM = td.rev_m != null ? td.rev_m : 70;
  var tcr2 = td.cr2 != null ? td.cr2 : 50;
  /* baseAppts: monthly_breakdown L3M 실제 평균 oppts 우선, 없으면 VR 역산 (시나리오 입력 기본값용) */
  /* monthly_breakdown 은 pip_ui.actual 안에 있으므로 act 에서 읽어야 함 */
  var _initMb = (act.monthly_breakdown || []).filter(function(m){ return !m.is_current; });
  var baseAppts = _initMb.length > 0
    ? Math.round(_initMb.reduce(function(s,m){ return s+(m.oppts||0); }, 0) / _initMb.length)
    : ((act.visits > 0 && act.vr > 0)
        ? Math.round(act.visits / (act.vr / 100))
        : ((ui.monthly_goals && ui.monthly_goals.m_appts) ? ui.monthly_goals.m_appts : 20));
  var tvr = td.vr != null ? td.vr : 60;
  var tor = td.or_ != null ? td.or_ : 70;
  var tsr = td.sr != null ? td.sr : 60;
  var tsal = td.sales != null ? td.sales : 35;
  var tord = td.orders != null ? td.orders : (tsr > 0 ? Math.round(tsal * 1000 / tsr) / 10 : 0);
  var tvis = td.visits != null ? td.visits : (tor > 0 ? Math.round(tord * 1000 / tor) / 10 : 0);
  var tcr3 = td.cr3 != null ? td.cr3 : Math.round((tvr * tor * tsr) / 100) / 100;
  window.__pipDetailCtx = {
    ui: ui, d: d, sim: sim, act: act, td: td, cg: cg, trialsNow: trialsNow,
    refY: refY, refM1: refM1, refD: refD, goals: goals,
    calendar_months: ui.calendar_months || {}, calendar_monitor_months: ui.calendar_monitor_months || [],
    calendar_pip_months: ui.calendar_pip_months || [],
    ccName: ccName,
    monthly_goals: ui.monthly_goals || {},
    pip_start_y: ui.pip_start_year, pip_start_m1: ui.pip_start_month_1, pip_start_d: ui.pip_start_mdom || 15,
    pipAsp: pip.asp != null ? pip.asp : 0
  };
  content.innerHTML =
    '<div class="pip-detail-stack">' +
    '<div class="card">' +
    '<div class="pipd-hdr">' +
    '<div class="pipd-avatar">' + esc((ccName || '')[0] || '') + '</div>' +
    '<div class="pipd-hi"><div class="pipd-name">' + esc(ccName) + ' <span class="badge ' + badgeClass + '">' + kind + '</span></div>' +
    '<div class="pipd-sub">' + esc(pip.team_display || '') + ' · 입사 ' + esc(pip.hire_yy_mm || '—') + ' · L3M 실적 / N3M 예측</div></div>' +
    '<div class="pipd-hkpi"><div class="pipd-hkpi-l">L3M 평균</div><div class="pipd-hkpi-v" style="color:' + l3mCol + '">' + wonFmt(l3mAvg) + '</div><div class="pipd-hkpi-s">목표 ₩70M</div></div>' +
    '<div class="pipd-hkpi"><div class="pipd-hkpi-l">CR2</div><div class="pipd-hkpi-v">' + kpiPct(pip.cr2) + '</div><div class="pipd-hkpi-s">목표 ' + (goals.cr2 || 45) + '%</div></div>' +
    '<div class="pipd-hkpi"><div class="pipd-hkpi-l">CR3</div><div class="pipd-hkpi-v">' + kpiPct(pip.cr3) + '</div><div class="pipd-hkpi-s">목표 ' + (goals.cr3 || 16) + '%</div></div>' +
    '<div class="pipd-hkpi"><div class="pipd-hkpi-l">Trials</div><div class="pipd-hkpi-v">' + trialsNow + '건</div><div class="pipd-hkpi-s">현재 pipeline</div></div>' +
    '</div></div>' +
    /* ── 퍼널 단계별 진단 ── */
    '<div class="card">' +
    '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">' +
    '<div><div class="pipd-sec" style="margin-bottom:1px">퍼널 단계별 진단 — L3M 실적 기준 (PIP 선정 원인 분석)</div>' +
    '<div style="font-size:14px;color:#aaa">최근 3개월 평균 전환율과 목표 대비 어느 단계에서 매출 누수가 발생했는지 확인합니다</div></div>' +
    '<div id="pipd-fn-focus-badge"></div></div>' +
    '<div class="pipd-fn-wrap" id="pipd-fn-row"></div>' +
    '<div id="pipd-fn-insight" style="margin-top:8px"></div>' +
    '<div id="pipd-fn-monthly" style="margin-top:10px"></div>' +
    '</div>' +
    /* ── 월별 목표 설정 (PIP 3개월) ── */
    '<div class="card">' +
    '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;flex-wrap:wrap;gap:8px">' +
    '<div class="pipd-sec" style="margin-bottom:0">월별 목표 설정 (PIP 3개월)</div>' +
    '<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">' +
    '<span style="font-size:14px;color:#555;font-weight:600">월 목표 매출</span>' +
    '<input id="pipd-g-rev" class="pipd-sc-inp" type="number" value="' + defRevM + '" style="width:70px;font-size:17px" oninput="pipdRenderGoals()"> M' +
    '<span style="font-size:14px;color:#555;font-weight:600;margin-left:8px">목표 ABV</span>' +
    '<input id="pipd-g-abv" class="pipd-sc-inp" type="number" value="' + Math.round(defAbvM * 100) + '" style="width:70px;font-size:16px" oninput="pipdRenderGoals()"> 만원' +
    '<span style="font-size:14px;color:#555;font-weight:600;margin-left:8px">SR</span>' +
    '<input id="pipd-g-sr" class="pipd-sc-inp" type="number" value="' + Math.round(tsr) + '" style="width:56px" oninput="pipdRenderGoals()"> %' +
    '<span style="font-size:14px;color:#555;font-weight:600;margin-left:4px">OR</span>' +
    '<input id="pipd-g-or" class="pipd-sc-inp" type="number" value="' + Math.round(tor) + '" style="width:56px" oninput="pipdRenderGoals()"> %' +
    '<span style="font-size:14px;color:#555;font-weight:600;margin-left:4px">VR</span>' +
    '<input id="pipd-g-vr" class="pipd-sc-inp" type="number" value="' + Math.round(tvr) + '" style="width:56px" oninput="pipdRenderGoals()"> %' +
    '<span style="font-size:14px;color:#555;font-weight:600;margin-left:4px">CR2</span>' +
    '<input id="pipd-g-cr2" class="pipd-sc-inp" type="number" value="' + Math.round(tcr2) + '" style="width:56px" oninput="pipdRenderGoals()"> %' +
    '</div></div>' +
    '<div class="pipd-gm-grid" id="pipd-gm-grid"></div>' +
    '</div>' +
    /* ── 시나리오 비교 ── */
    '<div class="card">' +
    '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;flex-wrap:wrap;gap:8px">' +
    '<div class="pipd-sec" style="margin-bottom:0">시나리오 비교 — 현재 추세 vs 지표 개선 시나리오</div>' +
    '<div style="font-size:12px;color:#aaa">각 시나리오별 지표를 자유롭게 조합해 설정하세요</div>' +
    '</div>' +
    '<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px">' +
    /* 시나리오 B 입력 */
    '<div style="background:#F5F6FF;border:1.5px solid #C7C3F0;border-radius:8px;padding:10px 14px">' +
    '<div style="font-size:14px;color:#534AB7;font-weight:700;margin-bottom:8px">시나리오 B</div>' +
    '<div style="display:grid;grid-template-columns:repeat(6,1fr);gap:6px">' +
    '<div><div style="font-size:11px;color:#854F0B;font-weight:600;margin-bottom:3px">Appts 건/월</div><input id="pipd-sc-b-appts" class="pipd-sc-inp" type="number" step="1" min="1" value="' + baseAppts + '" style="width:100%;min-width:0" oninput="pipdRenderScenario()"></div>' +
    '<div><div style="font-size:11px;color:#888;margin-bottom:3px">CR2 %</div><input id="pipd-sc-b-cr2" class="pipd-sc-inp" type="number" step="0.1" min="0" max="100" value="' + Math.round((act.cr2 || tcr2) * 10) / 10 + '" style="width:100%;min-width:0" oninput="pipdRenderScenario()"></div>' +
    '<div><div style="font-size:11px;color:#888;margin-bottom:3px">VR %</div><input id="pipd-sc-b-vr"  class="pipd-sc-inp" type="number" step="0.1" min="0" max="100" value="' + Math.round((act.vr  || tvr ) * 10) / 10 + '" style="width:100%;min-width:0" oninput="pipdRenderScenario()"></div>' +
    '<div><div style="font-size:11px;color:#888;margin-bottom:3px">OR %</div><input id="pipd-sc-b-or"  class="pipd-sc-inp" type="number" step="0.1" min="0" max="100" value="' + Math.round((act.or_ || tor ) * 10) / 10 + '" style="width:100%;min-width:0" oninput="pipdRenderScenario()"></div>' +
    '<div><div style="font-size:11px;color:#888;margin-bottom:3px">SR %</div><input id="pipd-sc-b-sr"  class="pipd-sc-inp" type="number" step="0.1" min="0" max="100" value="' + Math.round((act.sr  || tsr ) * 10) / 10 + '" style="width:100%;min-width:0" oninput="pipdRenderScenario()"></div>' +
    '<div><div style="font-size:11px;color:#888;margin-bottom:3px">ABV 만원</div><input id="pipd-sc-b-abv" class="pipd-sc-inp" type="number" step="1" min="1" value="' + Math.round((act.abv_m || defAbvM) * 100) + '" style="width:100%;min-width:0" oninput="pipdRenderScenario()"></div>' +
    '</div></div>' +
    /* 시나리오 C 입력 */
    '<div style="background:#EDFBF5;border:1.5px solid #8FD6B7;border-radius:8px;padding:10px 14px">' +
    '<div style="font-size:14px;color:#0F6E56;font-weight:700;margin-bottom:8px">시나리오 C</div>' +
    '<div style="display:grid;grid-template-columns:repeat(6,1fr);gap:6px">' +
    '<div><div style="font-size:11px;color:#854F0B;font-weight:600;margin-bottom:3px">Appts 건/월</div><input id="pipd-sc-c-appts" class="pipd-sc-inp" type="number" step="1" min="1" value="' + baseAppts + '" style="width:100%;min-width:0" oninput="pipdRenderScenario()"></div>' +
    '<div><div style="font-size:11px;color:#888;margin-bottom:3px">CR2 %</div><input id="pipd-sc-c-cr2" class="pipd-sc-inp" type="number" step="0.1" min="0" max="100" value="' + Math.round((act.cr2 || tcr2) * 10) / 10 + '" style="width:100%;min-width:0" oninput="pipdRenderScenario()"></div>' +
    '<div><div style="font-size:11px;color:#888;margin-bottom:3px">VR %</div><input id="pipd-sc-c-vr"  class="pipd-sc-inp" type="number" step="0.1" min="0" max="100" value="' + tvr  + '" style="width:100%;min-width:0" oninput="pipdRenderScenario()"></div>' +
    '<div><div style="font-size:11px;color:#888;margin-bottom:3px">OR %</div><input id="pipd-sc-c-or"  class="pipd-sc-inp" type="number" step="0.1" min="0" max="100" value="' + tor  + '" style="width:100%;min-width:0" oninput="pipdRenderScenario()"></div>' +
    '<div><div style="font-size:11px;color:#888;margin-bottom:3px">SR %</div><input id="pipd-sc-c-sr"  class="pipd-sc-inp" type="number" step="0.1" min="0" max="100" value="' + tsr  + '" style="width:100%;min-width:0" oninput="pipdRenderScenario()"></div>' +
    '<div><div style="font-size:11px;color:#888;margin-bottom:3px">ABV 만원</div><input id="pipd-sc-c-abv" class="pipd-sc-inp" type="number" step="1" min="1" value="' + Math.round(defAbvM * 100) + '" style="width:100%;min-width:0" oninput="pipdRenderScenario()"></div>' +
    '</div></div>' +
    '</div>' +
    '<div style="display:grid;grid-template-columns:max-content 1fr;gap:16px;align-items:stretch;min-width:0">' +
    '<div style="min-width:0"><table class="pipd-sc-tbl" id="pipd-sc-tbl" style="width:max-content"></table></div>' +
    '<div style="position:relative;min-height:400px;min-width:0"><canvas id="pipd-sc-chart"></canvas></div>' +
    '</div>' +
    '<div id="pipd-sc-detail" data-open="" style="margin-top:4px"></div>' +
    '</div>' +
    '<div class="card">' +
    '<div class="pipd-cal-hdr">' +
    '<div class="pipd-sec" style="margin-bottom:0">일정 · 성과 캘린더</div>' +
    '<div style="display:flex;gap:5px">' +
    '<button type="button" class="pipd-tag pipd-on" id="pipd-tab-monthly" onclick="pipdSetCalView(&quot;monthly&quot;,this)">월별</button></div>' +
    '<span style="font-size:13px;color:#aaa;margin-left:4px" id="pipd-pip-note">PIP</span>' +
    '<span id="pipd-week-nav" style="display:none;gap:6px;align-items:center;margin-left:auto">' +
    '<button type="button" class="pipd-cal-nav" onclick="pipdMoveWeek(-1)">‹</button><span id="pipd-week-label" style="font-size:14px;font-weight:600;min-width:120px;text-align:center"></span><button type="button" class="pipd-cal-nav" onclick="pipdMoveWeek(1)">›</button></span>' +
    '<span id="pipd-day-nav" style="display:none;gap:6px;align-items:center;margin-left:auto">' +
    '<button type="button" class="pipd-cal-nav" onclick="pipdMoveDay(-1)">‹</button><span id="pipd-day-label" style="font-size:14px;font-weight:600;min-width:100px;text-align:center"></span><button type="button" class="pipd-cal-nav" onclick="pipdMoveDay(1)">›</button></span>' +
    '<div style="display:flex;gap:6px;margin-left:auto">' +
    '<button type="button" class="pipd-cal-toggle" id="pipd-btn-summary" onclick="pipdToggleSummary()">요약보기</button>' +
    '<button type="button" class="pipd-cal-toggle" id="pipd-btn-cal-toggle" onclick="pipdToggleCalDetail()">세부내용 보기</button></div></div>' +
    '<div id="pipd-summary-panel" style="display:none;margin-bottom:12px;padding:12px 14px;background:#FAFAF8;border:1px solid #E5E4DF;border-radius:10px"></div>' +
    '<div style="display:flex;gap:0">' +
    '<div id="pipd-cal-panel-main" style="flex:1;min-width:0;max-height:800px;overflow-y:auto;padding-right:4px"><div id="pipd-cal-body"></div></div>' +
    '</div>' +
    '<div id="pipd-cal-panel-detail" style="display:none">' +
    '<div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;flex-wrap:wrap">' +
    '<span style="font-size:14px;color:#666;font-weight:600">기간</span>' +
    '<input type="month" id="pipd-detail-from" class="pipd-detail-month" title="시작월" onchange="pipdDetailMonthChanged()" />' +
    '<span style="font-size:15px;color:#888;font-weight:600">~</span>' +
    '<input type="month" id="pipd-detail-to" class="pipd-detail-month" title="종료월" onchange="pipdDetailMonthChanged()" /></div>' +
    '<div style="overflow-x:auto"><table class="pipd-dt" id="pipd-detail-table"><thead id="pipd-detail-thead"></thead><tbody id="pipd-detail-tbody"></tbody></table></div>' +
    '</div></div>' +
    '<div class="card">' +
    '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;flex-wrap:wrap;gap:8px">' +
    '<div class="pipd-sec" style="margin-bottom:0">현재 파이프라인 (Trials 중 · ' + (ps.count != null ? ps.count : trialsNow) + '건)</div>' +
    '<div style="font-size:14px;color:#aaa">예상 Sales: <strong style="color:#3B6D11">' + (ps.expected_sales_hint != null ? ps.expected_sales_hint : '—') + '건</strong> · SR(가중) <strong style="color:#185FA5">' + (ps.weighted_sr != null ? ps.weighted_sr + '%' : '—') + '</strong></div></div>' +
    '<div style="overflow-x:auto"><table class="pipd-pl tt tt-sortable" id="pip-pl-table" style="min-width:960px">' +
    '<thead><tr><th style="text-align:left" data-pl-sort="opp_id">기회 ID</th><th data-pl-sort="tad" style="white-space:nowrap">동의서 작성일</th><th data-pl-sort="sichak" style="white-space:nowrap">시착일</th><th style="text-align:left" data-pl-sort="partner">파트너 청각사</th><th style="text-align:left" data-pl-sort="program">시착/구매 프로그램</th><th style="text-align:left" data-pl-sort="left">보청기 - 좌</th><th style="text-align:left" data-pl-sort="right">보청기 - 우</th><th style="text-align:left" data-pl-sort="notes">특이사항</th></tr></thead><tbody>' + plBody + '</tbody></table></div>' +
    '<div style="margin-top:8px;padding:7px 12px;background:#F8F7F3;border-radius:6px;display:flex;gap:20px;font-size:14px;color:#555;flex-wrap:wrap">' +
    '<span>파이프라인 ' + (ps.count != null ? ps.count : trialsNow) + '건</span>' +
    '<span>SR 가중평균 <strong>' + (ps.weighted_sr != null ? ps.weighted_sr + '%' : '—') + '</strong></span>' +
    '<span>예상 Revenue <strong style="color:#185FA5">₩' + (ps.expected_rev_m != null ? ps.expected_rev_m : '—') + 'M</strong></span>' +
    '<span style="color:#854F0B" id="pipd-pl-warn"></span></div></div></div>';
  setTimeout(function () {
    pipdMonthActuals = [null, null, null];
    if (_pipdScChart) { try { _pipdScChart.destroy(); } catch(e) {} _pipdScChart = null; }
    pipdRenderFunnel();
    pipdRenderGoals();
    pipdInitCalendar(refY, refM1 - 1, refD);
    var w = pipdGi('pipd-pl-warn');
    if (w && ps.gap_to_75m_m != null) {
      if (ps.gap_to_75m_m > 0) w.textContent = '⚠ ₩75M 목표까지 약 ₩' + ps.gap_to_75m_m + 'M 부족 — Trials 추가 검토';
      else w.textContent = '✓ 파이프라인 예상이 ₩75M 목표에 도달합니다.';
    }
    bindPipelineDetailSort(pr);
  }, 80);
}

function renderPipTableRows(list, tbodyId) {
  var b = pipBench();
  var tbody = document.getElementById(tbodyId);
  if (!tbody) return;
  tbody.innerHTML = (list || []).map(function(p) {
    var ccEsc = (p.cc || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
    var m1 = p.l3m_m1 != null ? p.l3m_m1 : 0, m2 = p.l3m_m2 != null ? p.l3m_m2 : 0, m3 = p.l3m_m3 != null ? p.l3m_m3 : 0;
    var avgm = p.l3m_rev_month_avg != null ? p.l3m_rev_month_avg : Math.round((m1 + m2 + m3) / 3);
    var s1 = pipCellStyle(m1, b.rev_m1), s2 = pipCellStyle(m2, b.rev_m2), s3 = pipCellStyle(m3, b.rev_m3), savg = pipCellStyle(avgm, b.rev_month_avg);
    var sc2 = pipCellStyle(p.cr2, b.cr2), sc3 = pipCellStyle(p.cr3, b.cr3), svr = pipCellStyle(p.vr, b.vr), sor = pipCellStyle(p.or_, b.or_), ssr = pipCellStyle(p.sr, b.sr);
    var sas = pipCellStyle(p.asp, b.asp), sab = pipCellStyle(p.abv, b.abv);
    /* L3M 수익 / 수익률 */
    var margin = p.l3m_margin != null ? p.l3m_margin : null;
    var mrate  = p.l3m_margin_rate != null ? p.l3m_margin_rate : null;
    var marginFmt = margin != null ? (margin >= 0 ? '+' : '') + Math.round(margin / 1e6 * 10) / 10 + 'M' : '—';
    var mrateFmt  = mrate  != null ? mrate.toFixed(1) + '%' : '—';
    var marginCol = margin == null ? '#aaa' : margin >= 0 ? '#1D9E75' : '#E24B4A';
    var mrateCol  = mrate  == null ? '#aaa' : mrate  >= 30 ? '#1D9E75' : mrate >= 15 ? '#EF9F27' : '#E24B4A';
    return '<tr><td style="text-align:left"><span class="pip-name-link" data-pip-cc="' + ccEsc + '" style="cursor:pointer;font-weight:700;color:#185FA5;text-decoration:underline">' + (p.cc || '') + '</span></td><td>' + (p.team_display || '') + '</td>' +
      '<td style="' + s1 + '">' + wonFmt(m1) + '</td><td style="' + s2 + '">' + wonFmt(m2) + '</td><td style="' + s3 + '">' + wonFmt(m3) + '</td>' +
      '<td style="' + savg + '">' + wonFmt(avgm) + '</td>' +
      '<td style="' + sc2 + '">' + pctFmt(p.cr2) + '</td><td style="' + sc3 + '">' + pctFmt(p.cr3) + '</td><td style="' + svr + '">' + pctFmt(p.vr) + '</td><td style="' + sor + '">' + pctFmt(p.or_) + '</td><td style="' + ssr + '">' + pctFmt(p.sr) + '</td>' +
      '<td style="' + sas + '">' + wonFmt(p.asp) + '</td><td style="' + sab + '">' + wonFmt(p.abv) + '</td>' +
      '<td style="font-weight:700;color:' + marginCol + '">' + marginFmt + '</td>' +
      '<td style="font-weight:700;color:' + mrateCol  + '">' + mrateFmt  + '</td></tr>';
  }).join('');
}
function fmtVsBenchSub(pipAvg, bench) {
  if (bench == null || isNaN(bench) || bench === 0) return 'PIP 제외 평균 대비 —';
  var pct = Math.round(pipAvg / bench * 100);
  return 'PIP 제외 평균 대비 ' + pct + '%';
}
function applyPipConfigLabels() {
  var cfg = getPayload().pip_config || {};
  var ref = cfg.reference_label || '—';
  var mon = cfg.monitoring_label || '—';
  var pp = cfg.pip_pct != null ? cfg.pip_pct : 10;
  var rp = cfg.risk_pct != null ? cfg.risk_pct : 25;
  var sub = document.getElementById('pip-page-sub');
  if (sub) sub.textContent = '';
  var badge = document.getElementById('pip-criteria-badge');
  if (badge) {
    badge.textContent = '참조 ' + ref + ' · PIP 하위 ' + pp + '% · 위험 하위 ' + rp + '%(PIP 제외) · 모니터링 ' + mon;
  }
  var riskT = document.getElementById('pip-risk-sec-title');
  if (riskT) {
    riskT.textContent = 'PIP 접근 위험 (참조 매출 하위 ' + rp + '% 중 PIP 하위 ' + pp + '% 제외 구간)';
  }
  var note = document.getElementById('pip-note-pip-list');
  if (note) {
    note.innerHTML = '참조기간 <strong>' + ref + '</strong> 월별 매출 외 열은 <strong>대시보드 L3M</strong> 기준 지표입니다.';
  }
}
function renderPip() {
  applyPipConfigLabels();
  var pip = getPayload().pip || [];
  var risk = getPayload().pip_at_risk || [];
  var owners = getPayload().owners || [];
  var b = pipBench();
  var ml = getPayload().pip_month_labels || [];
  var h1 = document.getElementById('pip-h-m1'), h2 = document.getElementById('pip-h-m2'), h3 = document.getElementById('pip-h-m3');
  if (h1 && ml[0]) h1.textContent = ml[0];
  if (h2 && ml[1]) h2.textContent = ml[1];
  if (h3 && ml[2]) h3.textContent = ml[2];
  document.querySelectorAll('.pip-risk-mh').forEach(function(el, i) { if (ml[i]) el.textContent = ml[i]; });
  var alertEl = document.getElementById('pip-alert');
  if (alertEl) alertEl.textContent = pip.length > 0 ? '현재 ' + pip.length + '명이 PIP 대상입니다. 즉각적인 1:1 면담 및 성과향상 계획(PIP) 수립이 필요합니다.' : '현재 PIP 대상 없음.';
  var s1 = document.getElementById('pip-s1');
  if (s1) s1.textContent = pip.length + '명';
  var s1sub = document.getElementById('pip-s1-sub');
  var elig = (getPayload().pip_summary || {}).eligible_n;
  if (s1sub) s1sub.textContent = (elig != null && elig > 0) ? ('선정 풀 ' + elig + '명 중') : ('담당자 ' + (owners.length || 0) + '명 기준');
  var nn = pip.length || 1;
  var avgRevM = pip.length ? pip.reduce(function(s, p) { return s + (p.l3m_rev_month_avg != null ? p.l3m_rev_month_avg : 0); }, 0) / nn : 0;
  var sumCr2 = 0, sumCr3 = 0;
  pip.forEach(function(p) { sumCr2 += (p.cr2 || 0); sumCr3 += (p.cr3 || 0); });
  var avgCr2 = pip.length ? (sumCr2 / nn) : null;
  var avgCr3 = pip.length ? (sumCr3 / nn) : null;
  var s2 = document.getElementById('pip-s2');
  if (s2) s2.textContent = pip.length ? wonFmt(avgRevM) : '—';
  var s2sub = document.getElementById('pip-s2-sub');
  if (s2sub) s2sub.textContent = pip.length ? fmtVsBenchSub(avgRevM, b.rev_month_avg) : 'PIP 제외 평균 대비 —';
  var s3 = document.getElementById('pip-s3');
  if (s3) s3.textContent = avgCr2 != null ? (avgCr2.toFixed(1) + '%') : '—';
  var s3sub = document.getElementById('pip-s3-sub');
  if (s3sub) s3sub.textContent = avgCr2 != null ? fmtVsBenchSub(avgCr2, b.cr2) : 'PIP 제외 평균 대비 —';
  var s4 = document.getElementById('pip-s4');
  if (s4) s4.textContent = avgCr3 != null ? (avgCr3.toFixed(1) + '%') : '—';
  var s4sub = document.getElementById('pip-s4-sub');
  if (s4sub) s4sub.textContent = avgCr3 != null ? fmtVsBenchSub(avgCr3, b.cr3) : 'PIP 제외 평균 대비 —';
  var totT = (getPayload().pip_kpi || {}).total_trials != null ? getPayload().pip_kpi.total_trials : pip.reduce(function(s, p) { return s + (p.trials || 0); }, 0);
  var s5 = document.getElementById('pip-s5');
  if (s5) s5.textContent = pip.length ? (totT + '건') : '—';
  var s5sub = document.getElementById('pip-s5-sub');
  if (s5sub) s5sub.textContent = pip.length ? fmtVsBenchSub(totT / nn, b.trials_avg) : 'PIP 제외 평균 대비 —';
  renderPipTableRows(pip, 'pip-table-body');
  renderPipTableRows(risk, 'pip-risk-body');
  var best = getPayload().pip_best || [];
  renderPipTableRows(best, 'pip-best-body');
  /* Best 월 헤더 */
  document.querySelectorAll('.pip-best-mh').forEach(function(el, i) { if (ml[i]) el.textContent = ml[i]; });
  /* Best 카드 가시성 */
  var bestCard = document.getElementById('pip-best-card');
  if (bestCard) bestCard.style.display = best.length ? '' : 'none';
  var sel = document.getElementById('pip-person-select');
  if (sel) {
    var opts = [];
    if (pip.length) opts.push('<optgroup label="PIP 대상">');
    pip.forEach(function(p) { opts.push('<option value="' + (p.cc || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;') + '">' + p.cc + ' (PIP)</option>'); });
    if (pip.length) opts.push('</optgroup>');
    if (risk.length) opts.push('<optgroup label="접근 위험">');
    risk.forEach(function(p) { opts.push('<option value="' + (p.cc || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;') + '">' + p.cc + ' (위험)</option>'); });
    if (risk.length) opts.push('</optgroup>');
    sel.innerHTML = opts.length ? opts.join('') : '<option value="">대상 없음</option>';
    if (pip.length) sel.value = pip[0].cc;
    else if (risk.length) sel.value = risk[0].cc;
  }
  bindPipTableSort('pip-main-table', 'main');
  bindPipTableSort('pip-risk-table', 'risk');
}
function renderPipCharts() {
  if (_pipC1) { try { _pipC1.destroy(); } catch(e) {} _pipC1 = null; }
  if (_pipC2) { try { _pipC2.destroy(); } catch(e) {} _pipC2 = null; }
}
function goalBarColor(pct) {
  if (pct == null || isNaN(pct)) return 'E0DED8';
  pct = Math.min(100, Math.max(0, pct));
  if (pct >= 100) return '1D9E75';
  if (pct <= 0) return 'E24B4A';
  var r = Math.round(225 + (29 - 225) * pct / 100);
  var g = Math.round(158 + (75 - 158) * pct / 100);
  var b = Math.round(117 + (74 - 117) * pct / 100);
  return (r < 16 ? '0' : '') + r.toString(16) + (g < 16 ? '0' : '') + g.toString(16) + (b < 16 ? '0' : '') + b.toString(16);
}
function renderGoalsPage() {
  var periods = getPayload().periods || {};
  var labels = getPayload().period_labels || {};
  var cards = '';
  ['daily','weekly','monthly'].forEach(function(key) {
    var p = periods[key] || {};
    var g = getGoals(key);
    var label = labels[key] || key;
    var lead = p.lead || 0, qual = p.qualified_count != null ? p.qualified_count : 0, oppts = p.oppts || 0;
    var visits = p.visits || 0, trials = p.trials || 0, sales = p.sales || 0, revenue = p.revenue != null ? p.revenue : 0;
    var goals = [
      { name: 'Leads', actual: lead, goal: lead, pct: 100 },
      { name: '기회전환수', actual: qual, goal: lead ? Math.round(lead * 0.5) : 0, pct: lead ? Math.round(qual / (lead * 0.5) * 100) : 0 },
      { name: 'Appts', actual: oppts, goal: oppts, pct: 100 },
      { name: 'Visits', actual: visits, goal: oppts ? Math.round(oppts * 0.6) : 0, pct: (oppts && oppts * 0.6) ? Math.round(visits / (oppts * 0.6) * 100) : 0 },
      { name: 'Trials', actual: trials, goal: visits ? Math.round(visits * 0.7) : 0, pct: (visits && visits * 0.7) ? Math.round(trials / (visits * 0.7) * 100) : 0 },
      { name: 'Sales', actual: sales, goal: trials ? Math.round(trials * 0.6) : 0, pct: (trials && trials * 0.6) ? Math.round(sales / (trials * 0.6) * 100) : 0 },
      { name: 'Revenue', actual: revenue, goal: g.revenue != null ? g.revenue : 30e8, pct: g.revenue ? Math.round(revenue / g.revenue * 100) : 0 },
      { name: 'CR2', actual: p.cr2, goal: g.cr2 != null ? g.cr2 : 50, pct: g.cr2 ? Math.round((p.cr2 || 0) / g.cr2 * 100) : 0 },
      { name: 'CR2 CC', actual: p.cr2_cc, goal: g.cr2_cc != null ? g.cr2_cc : 60, pct: g.cr2_cc ? Math.round((p.cr2_cc || 0) / g.cr2_cc * 100) : 0 },
      { name: 'CR3', actual: p.cr3, goal: g.cr3 != null ? g.cr3 : 16, pct: g.cr3 ? Math.round((p.cr3 || 0) / g.cr3 * 100) : 0 },
      { name: 'VR', actual: p.vr, goal: g.vr != null ? g.vr : 60, pct: g.vr ? Math.round((p.vr || 0) / g.vr * 100) : 0 },
      { name: 'OR', actual: p.or_, goal: g.or_ != null ? g.or_ : 70, pct: g.or_ ? Math.round((p.or_ || 0) / g.or_ * 100) : 0 },
      { name: 'SR', actual: p.sr, goal: g.sr != null ? g.sr : 60, pct: g.sr ? Math.round((p.sr || 0) / g.sr * 100) : 0 },
      { name: 'BIN Rate', actual: p.bin_rate, goal: g.bin_rate != null ? g.bin_rate : 1.8, pct: g.bin_rate ? Math.round((p.bin_rate || 0) / g.bin_rate * 100) : 0 }
    ];
    var rows = goals.map(function(r) {
      var bar = r.goal != null && r.goal !== 0 ? Math.min(100, r.pct) : 0;
      var cls = goalBarColor(r.pct);
      var av = r.name === 'Revenue' ? wonFmt(r.actual) : (typeof r.actual === 'number' && r.actual >= 1e4 ? (r.actual >= 1e8 ? (r.actual/1e8).toFixed(2) + 'B' : (r.actual/1e6).toFixed(0) + 'M') : r.actual);
      var gv = r.name === 'Revenue' ? (r.goal != null ? wonFmt(r.goal) : '-') : (r.goal != null ? (typeof r.goal === 'number' && r.goal >= 1e4 ? (r.goal >= 1e8 ? (r.goal/1e8).toFixed(2) + 'B' : (r.goal/1e6).toFixed(0) + 'M') : r.goal) : '-');
      return '<div class="g-row"><div class="g-label">' + r.name + '</div><div style="flex:1;margin:0 10px"><div class="g-bar-wrap"><div class="g-bar-fill" style="width:' + bar + '%;background:#' + cls + '"></div></div></div><div class="g-vals"><span class="g-actual">' + av + '</span><span class="g-target">/ ' + gv + '</span><span class="g-pct" style="background:#' + cls + ';color:#fff;padding:1px 6px;border-radius:4px">' + r.pct + '%</span></div></div>';
    }).join('');
    cards += '<div class="goal-card"><div class="goal-period">' + label + '</div>' + rows + '</div>';
  });
  var el = document.getElementById('goals-cards');
  if (el) el.innerHTML = cards;
  var qCards = '<div class="goal-card"><div class="goal-period">Q1 (10월~12월)</div><div class="g-row"><div class="g-label">분기 목표</div><div class="g-vals"><span class="g-target" style="color:#999">추후 입력</span></div></div></div>';
  qCards += '<div class="goal-card"><div class="goal-period">Q2 (1월~3월)</div><div class="g-row"><div class="g-label">분기 목표</div><div class="g-vals"><span class="g-target" style="color:#999">추후 입력</span></div></div></div>';
  var qEl = document.getElementById('goals-quarter-cards');
  if (qEl) qEl.innerHTML = qCards;
}
function refreshSimCur() {
  var p = getPeriod('weekly');
  function set(id, t) { var e = document.getElementById(id); if (e) e.textContent = t; }
  set('sim-cur-leads', (p.lead != null ? Number(p.lead).toLocaleString() : '—'));
  set('sim-cur-cr2', pctFmt(p.cr2));
  set('sim-cur-vr', pctFmt(p.vr));
  set('sim-cur-or', pctFmt(p.or_));
  set('sim-cur-sr', pctFmt(p.sr));
  set('sim-cur-asp', wonFmt(p.asp));
  set('sim-cur-bin', p.bin_rate != null ? Number(p.bin_rate).toFixed(2) : '—');
}
function runSim() {
  if (!document.getElementById('s-leads')) return;  /* settings 페이지 없으면 무시 */
  var leads = parseFloat(document.getElementById('s-leads').value, 10) || 0;
  var cr2 = parseFloat(document.getElementById('s-cr2').value, 10) || 0;
  var vr = parseFloat(document.getElementById('s-vr').value, 10) || 0;
  var or_ = parseFloat(document.getElementById('s-or').value, 10) || 0;
  var sr = parseFloat(document.getElementById('s-sr').value, 10) || 0;
  var aspMan = parseFloat(document.getElementById('s-asp').value, 10) || 0;
  var oppts = Math.round(leads * cr2 / 100);
  var visits = Math.round(oppts * vr / 100);
  var trials = Math.round(visits * or_ / 100);
  var sales = Math.round(trials * sr / 100);
  var revWon = sales * aspMan * 10000;
  var p = getPeriod('weekly');
  var gw = getGoals('weekly');
  var curOppts = p.oppts || 0, curVisits = p.visits || 0, curTrials = p.trials || 0, curSales = p.sales || 0, curRev = p.revenue || 0;
  var grid = document.getElementById('sim-result-grid');
  if (grid) {
    grid.innerHTML =
      '<div class="smc"><div class="smc-label">예상 Appts</div><div class="smc-val">' + oppts.toLocaleString() + '</div><div class="smc-sub">이번주 ' + curOppts + ' (' + (oppts - curOppts >= 0 ? '+' : '') + (oppts - curOppts) + ')</div></div>' +
      '<div class="smc"><div class="smc-label">예상 Visits</div><div class="smc-val">' + visits.toLocaleString() + '</div><div class="smc-sub">이번주 ' + curVisits + '</div></div>' +
      '<div class="smc"><div class="smc-label">예상 Sales</div><div class="smc-val">' + sales + '건</div><div class="smc-sub">이번주 ' + curSales + '건</div></div>' +
      '<div class="smc"><div class="smc-label">예상 Revenue</div><div class="smc-val">' + wonFmt(revWon) + '</div><div class="smc-sub">주 목표 ' + wonFmt(gw.revenue) + '</div></div>';
  }
  var revGoal = gw.revenue || 8e8;
  var revPct = revGoal ? Math.round(revWon / revGoal * 100) : 0;
  var sum = document.getElementById('sim-summary');
  if (sum) {
    sum.innerHTML =
      '<div style="font-size:14px;font-weight:700;color:#185FA5;margin-bottom:6px">시뮬레이션 요약 (월 Leads 기준 퍼널 역산)</div>' +
      '<div class="sim-res-row"><span class="sim-res-label">입력 CR2·VR·OR·SR 적용 시 예상 Sales</span><span class="sim-res-val">' + sales + '건</span></div>' +
      '<div class="sim-res-row"><span class="sim-res-label">예상 Revenue (ASP ' + aspMan + '만원)</span><span class="sim-res-val">' + wonFmt(revWon) + '</span></div>' +
      '<div class="sim-res-row"><span class="sim-res-label">주간 매출 목표 대비 (참고)</span><span class="sim-res-val" style="color:' + (revPct >= 100 ? '#3B6D11' : '#A32D2D') + '">' + revPct + '%</span></div>';
  }
}
function resetSim() {
  if (!document.getElementById('s-leads')) return;  /* settings 페이지 없으면 무시 */
  var pm = getPeriod('monthly');
  var defLead = pm.lead != null && pm.lead > 0 ? Math.round(pm.lead) : 1400;
  document.getElementById('s-leads').value = defLead;
  document.getElementById('s-cr2').value = 50;
  document.getElementById('s-vr').value = 60;
  document.getElementById('s-or').value = 70;
  document.getElementById('s-sr').value = 60;
  document.getElementById('s-asp').value = 1200;
  document.getElementById('s-bin').value = 1.8;
  refreshSimCur();
  runSim();
}
document.addEventListener('DOMContentLoaded', function() {
  __PAYLOAD__ = __DATA_JSON__;
  try {
    if (localStorage.getItem('salesDashSb') === '1') {
      var sh = document.getElementById('app-shell');
      var op = document.getElementById('sb-open');
      if (sh) sh.classList.add('sidebar-collapsed');
      if (op) op.style.display = 'flex';
    }
  } catch (e) {}
  renderOverview('weekly');
  renderGoalsPage();
  renderTeamPage();
  renderPip();
  function pipNameClick(ev) {
    var el = ev.target.closest && ev.target.closest('.pip-name-link');
    if (!el) return;
    ev.stopPropagation();
    var cc = el.getAttribute('data-pip-cc');
    if (!cc) return;
    var sel = document.getElementById('pip-person-select');
    if (sel) sel.value = cc;
    showPIPView('detail');
    renderPIPDetailContent(cc);
  }
  var pipTable = document.getElementById('pip-main-table');
  var pipRisk = document.getElementById('pip-risk-table');
  if (pipTable && !pipTable._pipClick) { pipTable._pipClick = true; pipTable.addEventListener('click', pipNameClick); }
  if (pipRisk && !pipRisk._pipClick) { pipRisk._pipClick = true; pipRisk.addEventListener('click', pipNameClick); }
});
</script>
</body>
</html>
"""
    return html_template.replace("__DATA_JSON__", data_json)


def main():
    excel_path = EXCEL_PATH
    output_path = OUTPUT_HTML
    args = [a for a in sys.argv[1:] if a and not a.startswith("-")]
    if len(args) >= 2:
        excel_path, output_path = args[0], args[1]
    elif len(args) == 1:
        excel_path = args[0]
    ref_dt = datetime.now()
    cc, leads, perf, call, supply = load_excel(excel_path)
    payload = run_metrics(cc, leads, perf, call, ref_dt, supply_price_df=supply)
    html = build_html(payload)
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"대시보드 저장: {output_path}")


if __name__ == "__main__":
    main()
