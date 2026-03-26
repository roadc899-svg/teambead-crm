from fastapi import FastAPI, UploadFile, File, Query, Form, Request
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse, Response
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker, declarative_base
import pandas as pd
import shutil
import uuid
import os
from urllib.parse import urlencode
from html import escape
import io
import csv

# =========================================
# BLOCK 1 — DATABASE
# =========================================
DATABASE_URL = "sqlite:///./test.db"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


# =========================================
# BLOCK 2 — MODEL
# =========================================
class FBRow(Base):
    __tablename__ = "fb_rows"

    id = Column(Integer, primary_key=True, index=True)
    uploader = Column(String)  # internally оставляем старое имя поля для совместимости с БД
    ad_name = Column(String)

    launch_date = Column(String)
    platform = Column(String)
    manager = Column(String)
    geo = Column(String)
    offer = Column(String)
    creative = Column(String)

    leads = Column(Float)
    reg = Column(Float)
    ftd = Column(Float)
    clicks = Column(Float)
    spend = Column(Float)
    cpc = Column(Float)
    ctr = Column(Float)

    date_start = Column(String)
    date_end = Column(String)


Base.metadata.create_all(bind=engine)


# =========================================
# BLOCK 3 — APP
# =========================================
app = FastAPI(title="TEAMbead CRM")


@app.api_route("/", methods=["GET", "HEAD"])
def home(request: Request):
    if request.method == "HEAD":
        return Response(status_code=200)
    return RedirectResponse(url="/grouped", status_code=302)


# =========================================
# BLOCK 4 — HELPERS
# =========================================
def safe_number(value):
    if value is None:
        return 0.0
    try:
        if pd.isna(value):
            return 0.0
        if isinstance(value, str):
            value = value.replace(" ", "").replace("$", "").replace("%", "")
            value = value.replace(",", ".")
        return float(value)
    except Exception:
        return 0.0



def format_int_or_float(value):
    if value is None:
        return "0"
    try:
        value = float(value)
        if value.is_integer():
            return str(int(value))
        return f"{value:.2f}".rstrip("0").rstrip(".")
    except Exception:
        return str(value)



def format_money(value):
    try:
        return f"${float(value or 0):,.2f}"
    except Exception:
        return "$0.00"



def format_percent(value):
    try:
        return f"{float(value or 0):.2f}%"
    except Exception:
        return "0.00%"



def parse_ad_name(ad_name):
    if not ad_name:
        return {
            "launch_date": "",
            "platform": "",
            "manager": "",
            "geo": "",
            "offer": "",
            "creative": "",
        }

    parts = str(ad_name).split("/")
    return {
        "launch_date": parts[0].strip() if len(parts) > 0 else "",
        "platform": parts[1].strip() if len(parts) > 1 else "",
        "manager": parts[2].strip() if len(parts) > 2 else "",
        "geo": parts[3].strip() if len(parts) > 3 else "",
        "offer": parts[4].strip() if len(parts) > 4 else "",
        "creative": parts[5].strip() if len(parts) > 5 else "",
    }



def calc_metrics(clicks, reg, ftd, spend, leads=0):
    clicks = clicks or 0
    reg = reg or 0
    ftd = ftd or 0
    spend = spend or 0
    leads = leads or 0

    return {
        "cpc_real": spend / clicks if clicks > 0 else 0,
        "cpl_real": spend / leads if leads > 0 else 0,
        "cpa_real": spend / ftd if ftd > 0 else 0,
        "cr_reg": (reg / clicks) * 100 if clicks > 0 else 0,
        "cr_ftd": (ftd / clicks) * 100 if clicks > 0 else 0,
        "l2ftd": (ftd / leads) * 100 if leads > 0 else 0,
        "r2d": (ftd / reg) * 100 if reg > 0 else 0,
    }



def build_query_string(**kwargs):
    clean = {k: v for k, v in kwargs.items() if v not in [None, ""]}
    return urlencode(clean)



def make_options(options, selected_value):
    html = '<option value="">Все</option>'
    for option in options:
        option_text = escape(str(option))
        selected = "selected" if str(option) == str(selected_value) else ""
        html += f'<option value="{option_text}" {selected}>{option_text}</option>'
    return html



def parse_uploaded_dataframe(df, buyer):
    colmap = {str(c).strip().lower(): c for c in df.columns}

    def get_col(*names):
        for name in names:
            if name.lower() in colmap:
                return colmap[name.lower()]
        return None

    ad_col = get_col("Название объявления", "Ad name", "Ad Name")
    leads_col = get_col("Лиды", "Leads")
    reg_col = get_col("Завершенные регистрации", "Регистрации", "REG")
    ftd_col = get_col("Покупки", "FTD")
    clicks_col = get_col("Клики по ссылке", "Clicks", "Link Clicks")
    spend_col = get_col("Потраченная сумма (USD)", "Spend", "Amount spent (USD)")
    cpc_col = get_col("CPC (цена за клик по ссылке)", "CPC")
    ctr_col = get_col("CTR (все)", "CTR")
    ds_col = get_col("Дата начала отчетности", "Date Start")
    de_col = get_col("Дата окончания отчетности", "Date End")

    items = []
    for _, row in df.iterrows():
        ad_name = str(row.get(ad_col) or "") if ad_col else ""
        parsed = parse_ad_name(ad_name)

        items.append(
            FBRow(
                uploader=buyer,
                ad_name=ad_name,
                launch_date=parsed["launch_date"],
                platform=parsed["platform"],
                manager=parsed["manager"],
                geo=parsed["geo"],
                offer=parsed["offer"],
                creative=parsed["creative"],
                leads=safe_number(row.get(leads_col)) if leads_col else 0,
                reg=safe_number(row.get(reg_col)) if reg_col else 0,
                ftd=safe_number(row.get(ftd_col)) if ftd_col else 0,
                clicks=safe_number(row.get(clicks_col)) if clicks_col else 0,
                spend=safe_number(row.get(spend_col)) if spend_col else 0,
                cpc=safe_number(row.get(cpc_col)) if cpc_col else 0,
                ctr=safe_number(row.get(ctr_col)) if ctr_col else 0,
                date_start=str(row.get(ds_col) or "") if ds_col else "",
                date_end=str(row.get(de_col) or "") if de_col else "",
            )
        )
    return items


# =========================================
# BLOCK 5 — DATA ACCESS
# =========================================
def get_all_rows():
    db = SessionLocal()
    try:
        return db.query(FBRow).all()
    finally:
        db.close()



def get_filtered_data(buyer="", manager="", geo="", offer="", search=""):
    rows = get_all_rows()
    filtered = []
    search_lower = (search or "").lower().strip()

    for row in rows:
        if buyer and (row.uploader or "") != buyer:
            continue
        if manager and (row.manager or "") != manager:
            continue
        if geo and (row.geo or "") != geo:
            continue
        if offer and (row.offer or "") != offer:
            continue
        if search_lower:
            haystack = " | ".join([
                row.ad_name or "",
                row.platform or "",
                row.manager or "",
                row.geo or "",
                row.offer or "",
                row.creative or "",
                row.uploader or "",
            ]).lower()
            if search_lower not in haystack:
                continue
        filtered.append(row)
    return filtered



def get_filter_options():
    rows = get_all_rows()
    return (
        sorted({r.uploader for r in rows if r.uploader}),
        sorted({r.manager for r in rows if r.manager}),
        sorted({r.geo for r in rows if r.geo}),
        sorted({r.offer for r in rows if r.offer}),
    )


# =========================================
# BLOCK 6 — AGGREGATION
# =========================================
def aggregate_grouped_rows(rows):
    grouped = {}
    for row in rows:
        key = f"{row.uploader or ''}|||{row.ad_name or ''}"
        if key not in grouped:
            grouped[key] = {
                "buyer": row.uploader or "",
                "ad_name": row.ad_name or "",
                "launch_date": row.launch_date or "",
                "platform": row.platform or "",
                "manager": row.manager or "",
                "geo": row.geo or "",
                "offer": row.offer or "",
                "creative": row.creative or "",
                "clicks": 0.0,
                "leads": 0.0,
                "reg": 0.0,
                "ftd": 0.0,
                "spend": 0.0,
                "rows_combined": 0,
                "date_start": row.date_start or "",
                "date_end": row.date_end or "",
            }

        grouped[key]["clicks"] += row.clicks or 0
        grouped[key]["leads"] += row.leads or 0
        grouped[key]["reg"] += row.reg or 0
        grouped[key]["ftd"] += row.ftd or 0
        grouped[key]["spend"] += row.spend or 0
        grouped[key]["rows_combined"] += 1

    result = list(grouped.values())
    for item in result:
        item.update(calc_metrics(item["clicks"], item["reg"], item["ftd"], item["spend"], item["leads"]))
    return result



def aggregate_totals(rows):
    totals = {
        "clicks": sum(r["clicks"] for r in rows),
        "leads": sum(r["leads"] for r in rows),
        "reg": sum(r["reg"] for r in rows),
        "ftd": sum(r["ftd"] for r in rows),
        "spend": sum(r["spend"] for r in rows),
    }
    totals.update(calc_metrics(totals["clicks"], totals["reg"], totals["ftd"], totals["spend"], totals["leads"]))
    return totals



def aggregate_for_hierarchy(rows, keys):
    if not keys:
        return []

    first_key = keys[0]
    buckets = {}
    for row in rows:
        bucket = (row.get(first_key) or "Без значения").strip() or "Без значения"
        buckets.setdefault(bucket, []).append(row)

    result = []
    for bucket_name in sorted(buckets.keys()):
        bucket_rows = buckets[bucket_name]
        metrics = aggregate_totals(bucket_rows)
        node = {
            "name": bucket_name,
            "key": first_key,
            "metrics": metrics,
            "children": aggregate_for_hierarchy(bucket_rows, keys[1:]),
            "rows": bucket_rows,
        }
        result.append(node)
    return result


# =========================================
# BLOCK 7 — UI HELPERS
# =========================================
def sidebar_html(active_page):
    items = [
        ("grouped", "/grouped", "📘", "FB", [("/grouped", "Export", active_page == "grouped"), ("/hierarchy", "Statistic", active_page == "hierarchy")]),
        ("finance", "/finance", "💸", "Finance", []),
        ("caps", "/caps", "🧢", "Caps", []),
        ("chatterfy", "/chatterfy", "💬", "Chatterfy", []),
        ("holdwager", "/hold-wager", "🎯", "Hold/Wager", []),
    ]

    html = '''
    <aside class="sidebar">
        <div class="sidebar-brand">
            <span class="brand-mark"></span>
            <span>TEAMbead CRM</span>
        </div>
    '''

    for key, href, icon, title, children in items:
        if children:
            open_attr = "open" if active_page in ["grouped", "hierarchy"] else ""
            html += f'''
            <details class="sidebar-group" {open_attr}>
                <summary><span class="side-emoji">{icon}</span><span>{title}</span></summary>
                <div class="sidebar-links">
            '''
            for child_href, child_title, active in children:
                active_class = "active-link" if active else ""
                html += f'<a href="{child_href}" class="{active_class}">{child_title}</a>'
            html += '</div></details>'
        else:
            active_class = "sidebar-standalone active-link" if active_page == key else "sidebar-standalone"
            html += f'<a href="{href}" class="{active_class}"><span class="side-emoji">{icon}</span><span>{title}</span></a>'

    html += '</aside>'
    return html



def page_shell(title, content, active_page="grouped", extra_scripts="", top_actions=""):
    sidebar = sidebar_html(active_page)
    return f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{escape(title)}</title>
        <style>
            :root {{
                --bg: #07101f;
                --panel: #0d1729;
                --panel-2: #111f35;
                --panel-3: #16243c;
                --text: #ebf2ff;
                --muted: #8ca3c7;
                --border: #1f3150;
                --accent1: #38bdf8;
                --accent2: #2563eb;
                --accent3: #22c55e;
                --shadow: 0 12px 30px rgba(0,0,0,0.28);
                --table-head: #081120;
                --table-head-text: #dce8ff;
                --row-even: rgba(255,255,255,0.015);
                --good: rgba(34,197,94,0.13);
                --warn: rgba(245,158,11,0.12);
                --bad: rgba(239,68,68,0.10);
                --chip: #12233c;
                --soft: rgba(56,189,248,0.08);
                --resize-line: rgba(56,189,248,0.75);
            }}
            body.light {{
                --bg: #edf4ff;
                --panel: #ffffff;
                --panel-2: #f7fbff;
                --panel-3: #eef5ff;
                --text: #0f172a;
                --muted: #64748b;
                --border: #d7e3f2;
                --accent1: #2563eb;
                --accent2: #06b6d4;
                --accent3: #22c55e;
                --shadow: 0 10px 28px rgba(15, 23, 42, 0.08);
                --table-head: #f6faff;
                --table-head-text: #0f172a;
                --row-even: #f8fbff;
                --good: #dcfce7;
                --warn: #fef3c7;
                --bad: #fee2e2;
                --chip: #eef5ff;
                --soft: rgba(37,99,235,0.06);
                --resize-line: rgba(37,99,235,0.65);
            }}
            * {{ box-sizing: border-box; }}
            html {{ scroll-behavior: smooth; }}
            body {{
                margin: 0;
                background: radial-gradient(circle at top right, rgba(56,189,248,0.10), transparent 23%), var(--bg);
                color: var(--text);
                font-family: "Avenir Next", "Nunito", "Trebuchet MS", "Segoe UI", Arial, sans-serif;
            }}
            .app {{ display: flex; min-height: 100vh; }}
            .sidebar {{
                width: 280px;
                background: linear-gradient(180deg, rgba(8,16,32,0.90), rgba(10,20,38,0.98));
                border-right: 1px solid var(--border);
                padding: 18px 14px;
                position: sticky;
                top: 0;
                height: 100vh;
                overflow-y: auto;
            }}
            body.light .sidebar {{ background: linear-gradient(180deg, #ffffff, #f3f8ff); }}
            .sidebar-brand {{
                display: flex;
                align-items: center;
                gap: 10px;
                font-size: 18px;
                font-weight: 900;
                margin-bottom: 18px;
                color: var(--text);
            }}
            .brand-mark {{
                width: 14px;
                height: 14px;
                border-radius: 999px;
                background: linear-gradient(135deg, var(--accent1), var(--accent2), var(--accent3));
                box-shadow: 0 0 0 4px rgba(56,189,248,0.14);
                display: inline-block;
                flex-shrink: 0;
            }}
            .sidebar-group, .sidebar-standalone {{
                display: block;
                background: var(--panel);
                border: 1px solid var(--border);
                border-radius: 16px;
                margin-bottom: 12px;
                text-decoration: none;
                color: var(--text);
                box-shadow: var(--shadow);
            }}
            .sidebar-group summary, .sidebar-standalone {{
                padding: 12px 14px;
                cursor: pointer;
                font-weight: 900;
                font-size: 15px;
                list-style: none;
                display: flex;
                align-items: center;
                gap: 10px;
            }}
            .sidebar-group summary::-webkit-details-marker {{ display: none; }}
            .side-emoji {{ width: 22px; text-align: center; font-size: 18px; flex-shrink: 0; }}
            .sidebar-links {{ display: flex; flex-direction: column; gap: 8px; padding: 0 10px 10px; }}
            .sidebar-links a {{
                text-decoration: none;
                color: var(--text);
                padding: 10px 12px;
                border-radius: 12px;
                font-weight: 800;
            }}
            .sidebar-links a:hover, .sidebar-standalone:hover {{ background: var(--soft); }}
            .active-link {{
                background: linear-gradient(90deg, rgba(56,189,248,0.25), rgba(37,99,235,0.25));
                outline: 1px solid rgba(56,189,248,0.3);
            }}
            .main {{ flex: 1; padding: 22px; overflow-x: hidden; }}
            .topbar {{ display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; flex-wrap: wrap; margin-bottom: 18px; }}
            .page-title {{ font-size: 26px; font-weight: 900; letter-spacing: 0.2px; }}
            .subtitle {{ color: var(--muted); font-size: 13px; margin-top: 6px; }}
            .top-actions {{ display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }}
            .btn, .small-btn, .theme-toggle, .filters button, .filters a, .upload-btn, .ghost-btn {{
                border: 1px solid var(--border);
                background: linear-gradient(90deg, var(--accent2), var(--accent1));
                color: #fff;
                padding: 10px 14px;
                border-radius: 12px;
                cursor: pointer;
                text-decoration: none;
                font-weight: 900;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                gap: 8px;
                transition: 0.18s ease;
            }}
            .ghost-btn {{ background: var(--panel-2); color: var(--text); }}
            .small-btn {{ padding: 8px 12px; font-size: 13px; }}
            .btn:hover, .small-btn:hover, .theme-toggle:hover, .filters button:hover, .filters a:hover, .upload-btn:hover, .ghost-btn:hover {{ transform: translateY(-1px); }}
            .panel {{
                background: linear-gradient(180deg, var(--panel), var(--panel-2));
                border: 1px solid var(--border);
                border-radius: 20px;
                box-shadow: var(--shadow);
                padding: 16px;
                margin-bottom: 16px;
            }}
            .compact-panel {{ padding: 14px 16px; }}
            .panel-title {{ font-size: 15px; font-weight: 900; margin-bottom: 12px; }}
            .panel-subtitle {{ color: var(--muted); font-size: 13px; }}
            .toolbar-grid {{ display: grid; grid-template-columns: 1.35fr 1fr; gap: 16px; align-items: start; margin-bottom: 16px; }}
            .upload-form, .filters form {{ display: flex; gap: 10px; flex-wrap: wrap; align-items: end; }}
            .upload-form label, .filters label {{ display: flex; flex-direction: column; gap: 6px; font-size: 12px; font-weight: 800; color: var(--text); }}
            .upload-form input, .filters input, .filters select {{
                min-width: 150px;
                border-radius: 12px;
                border: 1px solid var(--border);
                background: var(--panel-3);
                color: var(--text);
                padding: 11px 12px;
                outline: none;
            }}
            .upload-inline {{ display: flex; flex-wrap: wrap; gap: 10px; align-items: end; }}
            .hint {{ color: var(--muted); font-size: 12px; margin-top: 8px; }}
            .stats-grid {{ display: grid; grid-template-columns: repeat(7, minmax(120px, 1fr)); gap: 12px; }}
            .stat-card {{
                background: linear-gradient(180deg, var(--panel), var(--panel-3));
                border: 1px solid var(--border);
                border-radius: 16px;
                padding: 14px;
            }}
            .stat-card .name {{ font-size: 12px; color: var(--muted); margin-bottom: 8px; font-weight: 800; text-transform: uppercase; letter-spacing: 0.4px; }}
            .stat-card .value {{ font-size: 28px; font-weight: 900; line-height: 1.05; }}
            .controls-line {{ display: flex; justify-content: space-between; gap: 10px; align-items: center; flex-wrap: wrap; margin-bottom: 10px; }}
            .table-wrap {{
                overflow: auto;
                border-radius: 18px;
                border: 1px solid var(--border);
                box-shadow: var(--shadow);
                background: var(--panel);
            }}
            table {{ border-collapse: separate; border-spacing: 0; width: 100%; min-width: 1450px; background: var(--panel); color: var(--text); }}
            th, td {{
                border-right: 1px solid var(--border);
                border-bottom: 1px solid var(--border);
                padding: 10px 12px;
                text-align: left;
                font-size: 14px;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                position: relative;
            }}
            th {{
                background: var(--table-head);
                color: var(--table-head-text);
                position: sticky;
                top: 0;
                z-index: 5;
                user-select: none;
            }}
            tbody tr:nth-child(even) {{ background: var(--row-even); }}
            tbody tr.good-row {{ background: var(--good); }}
            tbody tr.warn-row {{ background: var(--warn); }}
            tbody tr.bad-row {{ background: var(--bad); }}
            body.hide-row-colors tbody tr.good-row,
            body.hide-row-colors tbody tr.warn-row,
            body.hide-row-colors tbody tr.bad-row {{ background: transparent !important; }}
            th a {{ color: inherit; text-decoration: none; }}
            .th-inner {{ display: flex; align-items: center; justify-content: space-between; gap: 8px; padding-right: 10px; }}
            .drag-handle {{ cursor: grab; opacity: 0.75; font-size: 12px; }}
            .dragging {{ opacity: 0.45; }}
            .drag-target-left::before, .drag-target-right::after {{
                content: "";
                position: absolute;
                top: 0;
                bottom: 0;
                width: 3px;
                background: var(--resize-line);
                z-index: 8;
            }}
            .drag-target-left::before {{ left: 0; }}
            .drag-target-right::after {{ right: 0; }}
            .resizer {{
                position: absolute;
                top: 0;
                right: 0;
                width: 8px;
                height: 100%;
                cursor: col-resize;
                user-select: none;
                z-index: 9;
            }}
            .column-menu-wrap {{ position: relative; }}
            .column-menu {{
                position: absolute;
                right: 0;
                top: calc(100% + 8px);
                width: 340px;
                max-width: calc(100vw - 40px);
                background: var(--panel);
                border: 1px solid var(--border);
                border-radius: 18px;
                box-shadow: var(--shadow);
                padding: 14px;
                display: none;
                z-index: 30;
            }}
            .column-menu.open {{ display: block; }}
            .column-actions {{ display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 10px; }}
            .column-grid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; max-height: 300px; overflow: auto; padding-right: 4px; }}
            .column-chip {{
                display: flex;
                align-items: center;
                gap: 8px;
                padding: 9px 10px;
                border: 1px solid var(--border);
                border-radius: 12px;
                background: var(--chip);
                font-size: 13px;
                font-weight: 800;
            }}
            .tree-root details {{ margin-bottom: 10px; border: 1px solid var(--border); border-radius: 16px; overflow: hidden; background: var(--panel); }}
            .tree-root summary {{ cursor: pointer; list-style: none; display: flex; justify-content: space-between; gap: 12px; align-items: center; flex-wrap: wrap; padding: 14px 16px; font-weight: 900; }}
            .tree-root summary::-webkit-details-marker {{ display: none; }}
            .tree-meta {{ color: var(--muted); font-size: 13px; }}
            .tree-line {{ display: grid; grid-template-columns: 2.5fr repeat(6, 1fr); gap: 10px; padding: 10px 16px; border-top: 1px solid var(--border); font-size: 14px; align-items: center; }}
            .tree-level-1 > summary {{ background: rgba(56,189,248,0.09); }}
            .tree-level-2 > summary {{ background: rgba(37,99,235,0.09); }}
            .tree-level-3 > summary {{ background: rgba(34,197,94,0.09); }}
            .tree-level-4 > summary {{ background: rgba(147,51,234,0.09); }}
            .tree-level-5 > summary {{ background: rgba(245,158,11,0.09); }}
            .empty-dev {{ min-height: 58vh; display: flex; align-items: center; justify-content: center; text-align: center; }}
            .empty-dev-card {{ max-width: 540px; padding: 28px; border-radius: 24px; border: 1px solid var(--border); background: linear-gradient(180deg, var(--panel), var(--panel-2)); box-shadow: var(--shadow); }}
            .empty-dev-card .big {{ font-size: 22px; font-weight: 900; margin-bottom: 10px; }}
            .muted {{ color: var(--muted); }}
            @media (max-width: 1200px) {{
                .stats-grid {{ grid-template-columns: repeat(3, minmax(130px, 1fr)); }}
                .toolbar-grid {{ grid-template-columns: 1fr; }}
            }}
            @media (max-width: 900px) {{
                .app {{ display: block; }}
                .sidebar {{ width: 100%; height: auto; position: relative; }}
                .main {{ padding: 14px; }}
                .page-title {{ font-size: 22px; }}
                .stats-grid {{ grid-template-columns: repeat(2, minmax(130px, 1fr)); }}
                .column-grid {{ grid-template-columns: 1fr; }}
            }}
        </style>
    </head>
    <body>
        <div class="app">
            {sidebar}
            <main class="main">
                <div class="topbar">
                    <div>
                        <div class="page-title">{escape(title)}</div>
                        <div class="subtitle">TEAMbead CRM — Facebook аналитика, структура и полный контроль</div>
                    </div>
                    <div class="top-actions">
                        {top_actions}
                        <button class="ghost-btn small-btn" onclick="toggleRowColors()">🎨 Цвета строк</button>
                        <button class="ghost-btn small-btn" onclick="toggleTheme()">🌙 / ☀️ Тема</button>
                    </div>
                </div>
                {content}
            </main>
        </div>
        <script>
            function toggleTheme() {{
                document.body.classList.toggle('light');
                localStorage.setItem('teambead-theme', document.body.classList.contains('light') ? 'light' : 'dark');
            }}
            (function initTheme() {{
                const saved = localStorage.getItem('teambead-theme');
                if (saved === 'light') document.body.classList.add('light');
            }})();
            function toggleRowColors() {{
                document.body.classList.toggle('hide-row-colors');
                localStorage.setItem('teambead-row-colors', document.body.classList.contains('hide-row-colors') ? 'off' : 'on');
            }}
            (function initRowColors() {{
                const saved = localStorage.getItem('teambead-row-colors');
                if (saved === 'off') document.body.classList.add('hide-row-colors');
            }})();
            document.addEventListener('click', function(e) {{
                const wrap = document.querySelector('.column-menu-wrap');
                const menu = document.getElementById('columnMenu');
                if (!wrap || !menu) return;
                if (!wrap.contains(e.target)) menu.classList.remove('open');
            }});
        </script>
        {extra_scripts}
    </body>
    </html>
    """



def get_row_class(row):
    if (row.get("ftd") or 0) > 0:
        return "good-row"
    if (row.get("reg") or 0) > 0:
        return "warn-row"
    return "bad-row"



def sort_link(label, field, current_sort, current_order, **params):
    next_order = "asc" if current_sort != field or current_order == "desc" else "desc"
    qs = build_query_string(sort_by=field, order=next_order, **params)
    arrow = ""
    if current_sort == field:
        arrow = " ↑" if current_order == "asc" else " ↓"
    return f'<a href="?{qs}">{escape(label)}{arrow}</a>'



def render_stats_cards(totals):
    cards = [
        ("Spend", format_money(totals["spend"])),
        ("Leads", format_int_or_float(totals["leads"])),
        ("Reg", format_int_or_float(totals["reg"])),
        ("FTD", format_int_or_float(totals["ftd"])),
        ("CPA", format_money(totals["cpa_real"])),
        ("L2FTD", format_percent(totals["l2ftd"])),
        ("R2D", format_percent(totals["r2d"])),
    ]
    html = '<div class="panel compact-panel"><div class="stats-grid">'
    for title, value in cards:
        html += f'<div class="stat-card"><div class="name">{title}</div><div class="value">{value}</div></div>'
    html += '</div></div>'
    return html



def render_tree_nodes(nodes, level=1):
    html = ""
    level_class = f"tree-level-{min(level, 5)}"
    for node in nodes:
        m = node["metrics"]
        meta = f'<span class="tree-meta">Spend: {format_money(m["spend"])} · Leads: {format_int_or_float(m["leads"])} · Reg: {format_int_or_float(m["reg"])} · FTD: {format_int_or_float(m["ftd"])} · CPA: {format_money(m["cpa_real"])} · L2FTD: {format_percent(m["l2ftd"])} · R2D: {format_percent(m["r2d"])} </span>'
        if node["children"]:
            children_html = render_tree_nodes(node["children"], level + 1)
            html += f'''
            <details class="{level_class}" open>
                <summary><span>{escape(node["name"])} <span class="muted">({escape(node["key"])})</span></span>{meta}</summary>
                {children_html}
            </details>
            '''
        else:
            html += f'''
            <div class="tree-line {level_class}">
                <div><strong>{escape(node["name"])}</strong> <span class="muted">({escape(node["key"])})</span></div>
                <div>{format_money(m["spend"])}</div>
                <div>{format_int_or_float(m["leads"])}</div>
                <div>{format_int_or_float(m["reg"])}</div>
                <div>{format_int_or_float(m["ftd"])}</div>
                <div>{format_money(m["cpa_real"])}</div>
                <div>{format_percent(m["l2ftd"])}</div>
                <div>{format_percent(m["r2d"])} </div>
            </div>
            '''
    return html



def render_dev_page(title, emoji, active_page):
    content = f'''
    <div class="empty-dev">
        <div class="empty-dev-card">
            <div class="big">{emoji} {escape(title)}</div>
            <div class="muted">Эта страница пока в разработке. Блок уже добавлен в меню, дальше сможем наполнять его отдельно.</div>
        </div>
    </div>
    '''
    return page_shell(title, content, active_page=active_page)


# =========================================
# BLOCK 8 — UPLOAD
# =========================================
@app.post("/upload")
async def upload_file(buyer: str = Form(...), file: UploadFile = File(...)):
    original_name = file.filename or ""
    ext = os.path.splitext(original_name)[1].lower() or ".csv"
    filename = f"temp_{uuid.uuid4()}{ext}"

    try:
        with open(filename, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        if ext in [".xlsx", ".xls"]:
            df = pd.read_excel(filename)
        else:
            try:
                df = pd.read_csv(filename)
            except Exception:
                df = pd.read_csv(filename, sep=";")

        rows_to_insert = parse_uploaded_dataframe(df, buyer.strip())

        db = SessionLocal()
        try:
            db.query(FBRow).filter(FBRow.uploader == buyer.strip()).delete()
            db.commit()
            for item in rows_to_insert:
                db.add(item)
            db.commit()
        finally:
            db.close()

        return RedirectResponse(url="/grouped", status_code=303)
    finally:
        if os.path.exists(filename):
            os.remove(filename)


# =========================================
# BLOCK 9 — EXPORT
# =========================================
@app.get("/export/grouped")
def export_grouped_csv(
    buyer: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    offer: str = Query(default=""),
    search: str = Query(default=""),
    sort_by: str = Query(default="spend"),
    order: str = Query(default="desc"),
):
    rows = aggregate_grouped_rows(get_filtered_data(buyer, manager, geo, offer, search))
    reverse = order.lower() != "asc"
    rows.sort(key=lambda x: x.get(sort_by, 0) if x.get(sort_by) is not None else 0, reverse=reverse)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Buyer", "Ad Name", "Launch Date", "Platform", "Manager", "Geo", "Offer", "Creative",
        "Rows", "Clicks", "Leads", "Reg", "FTD", "Spend", "CPC", "CPL", "CPA",
        "L2FTD", "R2D", "Date Start", "Date End"
    ])
    for row in rows:
        writer.writerow([
            row["buyer"], row["ad_name"], row["launch_date"], row["platform"], row["manager"], row["geo"], row["offer"], row["creative"],
            format_int_or_float(row["rows_combined"]), format_int_or_float(row["clicks"]), format_int_or_float(row["leads"]),
            format_int_or_float(row["reg"]), format_int_or_float(row["ftd"]), format_money(row["spend"]),
            format_money(row["cpc_real"]), format_money(row["cpl_real"]), format_money(row["cpa_real"]),
            format_percent(row["l2ftd"]), format_percent(row["r2d"]), row["date_start"], row["date_end"],
        ])
    output.seek(0)
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv; charset=utf-8", headers={"Content-Disposition": "attachment; filename=teambead_export.csv"})


@app.get("/export/hierarchy")
def export_hierarchy_csv(
    buyer: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    offer: str = Query(default=""),
    search: str = Query(default=""),
):
    rows = aggregate_grouped_rows(get_filtered_data(buyer, manager, geo, offer, search))
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Geo", "Platform", "Manager", "Offer", "Creative", "Ad Name", "Leads", "Reg", "FTD", "Spend", "CPA", "L2FTD", "R2D"])
    for row in rows:
        writer.writerow([
            row["geo"], row["platform"], row["manager"], row["offer"], row["creative"], row["ad_name"],
            format_int_or_float(row["leads"]), format_int_or_float(row["reg"]), format_int_or_float(row["ftd"]),
            format_money(row["spend"]), format_money(row["cpa_real"]), format_percent(row["l2ftd"]), format_percent(row["r2d"]),
        ])
    output.seek(0)
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv; charset=utf-8", headers={"Content-Disposition": "attachment; filename=teambead_statistic.csv"})


# =========================================
# BLOCK 10 — GROUPED PAGE
# =========================================
@app.get("/grouped", response_class=HTMLResponse)
def show_grouped_table(
    buyer: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    offer: str = Query(default=""),
    search: str = Query(default=""),
    sort_by: str = Query(default="spend"),
    order: str = Query(default="desc"),
):
    data = get_filtered_data(buyer, manager, geo, offer, search)
    rows = aggregate_grouped_rows(data)
    all_buyers, all_managers, all_geos, all_offers = get_filter_options()

    allowed_sort_fields = {
        "buyer", "ad_name", "launch_date", "platform", "manager", "geo", "offer", "creative",
        "rows_combined", "clicks", "leads", "reg", "ftd", "spend", "cpa_real", "l2ftd", "r2d",
        "date_start", "date_end"
    }
    if sort_by not in allowed_sort_fields:
        sort_by = "spend"
    reverse = order.lower() != "asc"
    rows.sort(key=lambda x: x.get(sort_by, 0) if x.get(sort_by) is not None else 0, reverse=reverse)

    totals = aggregate_totals(rows)
    buyer_options = make_options(all_buyers, buyer)
    manager_options = make_options(all_managers, manager)
    geo_options = make_options(all_geos, geo)
    offer_options = make_options(all_offers, offer)
    export_qs = build_query_string(buyer=buyer, manager=manager, geo=geo, offer=offer, search=search, sort_by=sort_by, order=order)
    export_link = f"/export/grouped?{export_qs}" if export_qs else "/export/grouped"

    table_headers = [
        ("buyer", "Buyer"),
        ("ad_name", "Ad Name"),
        ("launch_date", "Launch"),
        ("platform", "Platform"),
        ("manager", "Manager"),
        ("geo", "Geo"),
        ("offer", "Offer"),
        ("creative", "Creative"),
        ("rows_combined", "Rows"),
        ("clicks", "Clicks"),
        ("leads", "Leads"),
        ("reg", "Reg"),
        ("ftd", "FTD"),
        ("spend", "Spend"),
        ("cpa_real", "CPA"),
        ("l2ftd", "L2FTD"),
        ("r2d", "R2D"),
        ("date_start", "Date Start"),
        ("date_end", "Date End"),
    ]

    head_html = ""
    for field, label in table_headers:
        head_html += f'''
        <th data-col="{field}" draggable="true">
            <div class="th-inner">
                <span class="drag-handle">⋮⋮</span>
                <span>{sort_link(label, field, sort_by, order, buyer=buyer, manager=manager, geo=geo, offer=offer, search=search)}</span>
            </div>
            <span class="resizer"></span>
        </th>
        '''

    rows_html = ""
    for row in rows:
        rows_html += f'''
        <tr class="{get_row_class(row)}">
            <td data-col="buyer">{escape(row["buyer"])}</td>
            <td data-col="ad_name">{escape(row["ad_name"])}</td>
            <td data-col="launch_date">{escape(row["launch_date"])}</td>
            <td data-col="platform">{escape(row["platform"])}</td>
            <td data-col="manager">{escape(row["manager"])}</td>
            <td data-col="geo">{escape(row["geo"])}</td>
            <td data-col="offer">{escape(row["offer"])}</td>
            <td data-col="creative">{escape(row["creative"])}</td>
            <td data-col="rows_combined">{format_int_or_float(row["rows_combined"])}</td>
            <td data-col="clicks">{format_int_or_float(row["clicks"])}</td>
            <td data-col="leads">{format_int_or_float(row["leads"])}</td>
            <td data-col="reg">{format_int_or_float(row["reg"])}</td>
            <td data-col="ftd">{format_int_or_float(row["ftd"])}</td>
            <td data-col="spend">{format_money(row["spend"])}</td>
            <td data-col="cpa_real">{format_money(row["cpa_real"])}</td>
            <td data-col="l2ftd">{format_percent(row["l2ftd"])}</td>
            <td data-col="r2d">{format_percent(row["r2d"])}</td>
            <td data-col="date_start">{escape(row["date_start"])}</td>
            <td data-col="date_end">{escape(row["date_end"])}</td>
        </tr>
        '''

    column_chips = ""
    for field, label in table_headers:
        column_chips += f'<label class="column-chip"><input type="checkbox" class="column-toggle" value="{field}" checked> {escape(label)}</label>'

    content = f'''
    <div class="toolbar-grid">
        <div class="panel compact-panel">
            <div class="panel-title">Загрузка данных</div>
            <form method="post" action="/upload" enctype="multipart/form-data" class="upload-form">
                <label>Buyer
                    <input type="text" name="buyer" required placeholder="Например: TeamBead1">
                </label>
                <label>CSV / XLSX
                    <input type="file" name="file" accept=".csv,.xlsx,.xls" required>
                </label>
                <button type="submit" class="upload-btn">Загрузить</button>
            </form>
            <div class="hint">Если грузишь того же buyer повторно — старые строки заменяются новыми.</div>
        </div>

        <div class="panel compact-panel">
            <div class="panel-title">Фильтры</div>
            <div class="filters">
                <form method="get" action="/grouped">
                    <label>Buyer<select name="buyer">{buyer_options}</select></label>
                    <label>Manager<select name="manager">{manager_options}</select></label>
                    <label>Geo<select name="geo">{geo_options}</select></label>
                    <label>Offer<select name="offer">{offer_options}</select></label>
                    <label>Search<input type="text" name="search" value="{escape(search)}" placeholder="Поиск по строкам"></label>
                    <input type="hidden" name="sort_by" value="{escape(sort_by)}">
                    <input type="hidden" name="order" value="{escape(order)}">
                    <button type="submit">Фильтровать</button>
                    <a href="/grouped" class="ghost-btn">Сбросить</a>
                </form>
            </div>
        </div>
    </div>

    {render_stats_cards(totals)}

    <div class="panel compact-panel">
        <div class="controls-line">
            <div>
                <div class="panel-title" style="margin-bottom:4px;">Таблица Export</div>
                <div class="panel-subtitle">Колонки можно двигать мышкой за заголовок и менять ширину за правый край.</div>
            </div>
            <div class="column-menu-wrap">
                <button type="button" class="ghost-btn small-btn" onclick="toggleColumnMenu()">⚙️ Колонки</button>
                <div class="column-menu" id="columnMenu">
                    <div class="column-actions">
                        <button type="button" class="ghost-btn small-btn" onclick="showAllColumns()">Показать все</button>
                        <button type="button" class="ghost-btn small-btn" onclick="resetColumnsAll()">Сбросить всё</button>
                    </div>
                    <div class="column-grid">{column_chips}</div>
                </div>
            </div>
        </div>

        <div class="table-wrap">
            <table id="groupedTable">
                <thead><tr>{head_html}</tr></thead>
                <tbody>{rows_html if rows_html else '<tr><td colspan="19">Нет данных</td></tr>'}</tbody>
            </table>
        </div>
    </div>
    '''

    extra_scripts = """
    <script>
        const HIDDEN_KEY = 'teambead-hidden-columns-v2';
        const ORDER_KEY = 'teambead-column-order-v2';
        const WIDTH_KEY = 'teambead-column-widths-v2';

        function getTable() { return document.getElementById('groupedTable'); }
        function getHeaderRow() { return getTable()?.querySelector('thead tr'); }
        function getCurrentOrder() {
            return Array.from(getHeaderRow().querySelectorAll('th[data-col]')).map(th => th.dataset.col);
        }
        function getRows() {
            return Array.from(getTable().querySelectorAll('tr'));
        }
        function reorderCells(order) {
            getRows().forEach(row => {
                const cellsMap = {};
                Array.from(row.children).forEach(cell => {
                    const key = cell.dataset.col;
                    if (key) cellsMap[key] = cell;
                });
                order.forEach(key => {
                    if (cellsMap[key]) row.appendChild(cellsMap[key]);
                });
            });
        }
        function applyOrder() {
            const saved = JSON.parse(localStorage.getItem(ORDER_KEY) || '[]');
            const current = getCurrentOrder();
            if (!saved.length) return;
            const merged = saved.filter(x => current.includes(x)).concat(current.filter(x => !saved.includes(x)));
            reorderCells(merged);
        }
        function applyVisibility() {
            const hidden = JSON.parse(localStorage.getItem(HIDDEN_KEY) || '[]');
            document.querySelectorAll('.column-toggle').forEach(cb => {
                cb.checked = !hidden.includes(cb.value);
            });
            document.querySelectorAll('[data-col]').forEach(el => {
                el.style.display = hidden.includes(el.dataset.col) ? 'none' : '';
            });
        }
        function saveVisibility() {
            const hidden = [];
            document.querySelectorAll('.column-toggle').forEach(cb => {
                if (!cb.checked) hidden.push(cb.value);
            });
            localStorage.setItem(HIDDEN_KEY, JSON.stringify(hidden));
            applyVisibility();
        }
        function showAllColumns() {
            localStorage.setItem(HIDDEN_KEY, JSON.stringify([]));
            applyVisibility();
        }
        function resetColumnsAll() {
            localStorage.removeItem(HIDDEN_KEY);
            localStorage.removeItem(ORDER_KEY);
            localStorage.removeItem(WIDTH_KEY);
            window.location.reload();
        }
        function toggleColumnMenu() {
            document.getElementById('columnMenu').classList.toggle('open');
        }
        function applyWidths() {
            const widths = JSON.parse(localStorage.getItem(WIDTH_KEY) || '{}');
            Object.entries(widths).forEach(([key, width]) => {
                document.querySelectorAll('[data-col="' + key + '"]').forEach(el => {
                    el.style.width = width + 'px';
                    el.style.minWidth = width + 'px';
                    el.style.maxWidth = width + 'px';
                });
            });
        }
        function saveWidth(key, width) {
            const widths = JSON.parse(localStorage.getItem(WIDTH_KEY) || '{}');
            widths[key] = Math.max(80, Math.round(width));
            localStorage.setItem(WIDTH_KEY, JSON.stringify(widths));
        }
        function initResizers() {
            document.querySelectorAll('#groupedTable th[data-col]').forEach(th => {
                const resizer = th.querySelector('.resizer');
                if (!resizer) return;
                let startX = 0;
                let startWidth = 0;
                let resizing = false;
                const key = th.dataset.col;
                resizer.addEventListener('mousedown', function(e) {
                    e.preventDefault();
                    e.stopPropagation();
                    resizing = true;
                    startX = e.clientX;
                    startWidth = th.getBoundingClientRect().width;
                    document.body.style.cursor = 'col-resize';
                });
                document.addEventListener('mousemove', function(e) {
                    if (!resizing) return;
                    const newWidth = Math.max(80, startWidth + (e.clientX - startX));
                    document.querySelectorAll('[data-col="' + key + '"]').forEach(el => {
                        el.style.width = newWidth + 'px';
                        el.style.minWidth = newWidth + 'px';
                        el.style.maxWidth = newWidth + 'px';
                    });
                });
                document.addEventListener('mouseup', function() {
                    if (!resizing) return;
                    resizing = false;
                    document.body.style.cursor = '';
                    saveWidth(key, th.getBoundingClientRect().width);
                });
            });
        }
        function initDragAndDrop() {
            let dragged = null;
            document.querySelectorAll('#groupedTable th[data-col]').forEach(th => {
                th.addEventListener('dragstart', function(e) {
                    if (e.target.classList.contains('resizer')) { e.preventDefault(); return; }
                    dragged = th;
                    th.classList.add('dragging');
                });
                th.addEventListener('dragend', function() {
                    document.querySelectorAll('#groupedTable th[data-col]').forEach(x => x.classList.remove('dragging', 'drag-target-left', 'drag-target-right'));
                    dragged = null;
                });
                th.addEventListener('dragover', function(e) {
                    e.preventDefault();
                    if (!dragged || dragged === th) return;
                    const rect = th.getBoundingClientRect();
                    const before = (e.clientX - rect.left) < rect.width / 2;
                    th.classList.toggle('drag-target-left', before);
                    th.classList.toggle('drag-target-right', !before);
                });
                th.addEventListener('dragleave', function() {
                    th.classList.remove('drag-target-left', 'drag-target-right');
                });
                th.addEventListener('drop', function(e) {
                    e.preventDefault();
                    if (!dragged || dragged === th) return;
                    const rect = th.getBoundingClientRect();
                    const before = (e.clientX - rect.left) < rect.width / 2;
                    if (before) th.parentNode.insertBefore(dragged, th);
                    else th.parentNode.insertBefore(dragged, th.nextSibling);
                    const order = getCurrentOrder();
                    reorderCells(order);
                    localStorage.setItem(ORDER_KEY, JSON.stringify(order));
                    document.querySelectorAll('#groupedTable th[data-col]').forEach(x => x.classList.remove('drag-target-left', 'drag-target-right'));
                });
            });
        }
        document.querySelectorAll('.column-toggle').forEach(cb => cb.addEventListener('change', saveVisibility));
        applyOrder();
        applyVisibility();
        applyWidths();
        initResizers();
        initDragAndDrop();
    </script>
    """

    top_actions = f'<a class="small-btn" href="{export_link}">⬇ CSV</a>'
    return page_shell("FB — Export", content, "grouped", extra_scripts, top_actions=top_actions)


# =========================================
# BLOCK 11 — STATISTIC PAGE
# =========================================
@app.get("/hierarchy", response_class=HTMLResponse)
def show_hierarchy(
    buyer: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    offer: str = Query(default=""),
    search: str = Query(default=""),
):
    data = get_filtered_data(buyer, manager, geo, offer, search)
    rows = aggregate_grouped_rows(data)
    all_buyers, all_managers, all_geos, all_offers = get_filter_options()

    buyer_options = make_options(all_buyers, buyer)
    manager_options = make_options(all_managers, manager)
    geo_options = make_options(all_geos, geo)
    offer_options = make_options(all_offers, offer)

    hierarchy_keys = ["geo", "platform", "manager", "offer", "creative", "ad_name"]
    tree = aggregate_for_hierarchy(rows, hierarchy_keys)
    tree_html = render_tree_nodes(tree) if tree else '<div class="panel">Нет данных</div>'
    totals = aggregate_totals(rows)

    export_qs = build_query_string(buyer=buyer, manager=manager, geo=geo, offer=offer, search=search)
    export_link = f"/export/hierarchy?{export_qs}" if export_qs else "/export/hierarchy"

    content = f'''
    <div class="panel compact-panel filters">
        <div class="panel-title">Фильтры</div>
        <form method="get" action="/hierarchy">
            <label>Buyer<select name="buyer">{buyer_options}</select></label>
            <label>Manager<select name="manager">{manager_options}</select></label>
            <label>Geo<select name="geo">{geo_options}</select></label>
            <label>Offer<select name="offer">{offer_options}</select></label>
            <label>Search<input type="text" name="search" value="{escape(search)}"></label>
            <button type="submit">Фильтровать</button>
            <a href="/hierarchy" class="ghost-btn">Сбросить</a>
        </form>
    </div>

    {render_stats_cards(totals)}

    <div class="panel compact-panel">
        <div class="panel-title">Statistic</div>
        <div class="panel-subtitle">Порядок уровней: Geo → Platform → Manager → Offer → Creative → Ad Name</div>
    </div>

    <div class="tree-root">{tree_html}</div>
    '''

    top_actions = f'<a class="small-btn" href="{export_link}">⬇ CSV</a>'
    return page_shell("FB — Statistic", content, "hierarchy", top_actions=top_actions)


# =========================================
# BLOCK 12 — PLACEHOLDERS
# =========================================
@app.get("/finance", response_class=HTMLResponse)
def finance_page():
    return render_dev_page("Finance", "💸", "finance")


@app.get("/caps", response_class=HTMLResponse)
def caps_page():
    return render_dev_page("Caps", "🧢", "caps")


@app.get("/chatterfy", response_class=HTMLResponse)
def chatterfy_page():
    return render_dev_page("Chatterfy", "💬", "chatterfy")


@app.get("/hold-wager", response_class=HTMLResponse)
def hold_wager_page():
    return render_dev_page("Hold/Wager", "🎯", "holdwager")
