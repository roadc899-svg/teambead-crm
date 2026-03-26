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
    uploader = Column(String)
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



def parse_uploaded_dataframe(df, uploader):
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
                uploader=uploader,
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



def get_filtered_data(uploader="", manager="", geo="", offer="", search=""):
    rows = get_all_rows()
    filtered = []
    search_lower = (search or "").lower().strip()

    for row in rows:
        if uploader and (row.uploader or "") != uploader:
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
                "uploader": row.uploader or "",
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
    grouped_active = "active-link" if active_page == "grouped" else ""
    hierarchy_active = "active-link" if active_page == "hierarchy" else ""
    return f"""
    <aside class="sidebar">
        <div class="sidebar-brand"><span class="brand-mark">◉</span><span>TEAMbead CRM</span></div>
        <details class="sidebar-group" open>
            <summary>FB</summary>
            <div class="sidebar-links">
                <a href="/grouped" class="{grouped_active}">Выгрузка</a>
                <a href="/hierarchy" class="{hierarchy_active}">Статистика</a>
            </div>
        </details>
    </aside>
    """



def page_shell(title, content, active_page="grouped", extra_scripts=""):
    sidebar = sidebar_html(active_page)
    return f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{escape(title)}</title>
        <style>
            :root {{
                --bg: #edf4ff;
                --panel: #ffffff;
                --panel-2: #f7fbff;
                --text: #0f172a;
                --muted: #64748b;
                --border: #d7e3f2;
                --shadow: 0 10px 28px rgba(15, 23, 42, 0.08);
                --accent1: #2563eb;
                --accent2: #06b6d4;
                --accent3: #22c55e;
                --table-head: #1d4ed8;
                --table-head-text: #ffffff;
                --row-even: #f8fbff;
                --good: #dcfce7;
                --warn: #fef3c7;
                --bad: #fee2e2;
                --lvl1: #dbeafe;
                --lvl2: #ede9fe;
                --lvl3: #fef3c7;
                --lvl4: #dcfce7;
                --lvl5: #ffe4e6;
                --lvl6: #f8fafc;
            }}
            body.dark {{
                --bg: #0b1220;
                --panel: #111827;
                --panel-2: #172033;
                --text: #f8fafc;
                --muted: #94a3b8;
                --border: #243244;
                --shadow: 0 8px 24px rgba(0,0,0,0.35);
                --accent1: #60a5fa;
                --accent2: #22d3ee;
                --accent3: #34d399;
                --table-head: #020617;
                --table-head-text: #ffffff;
                --row-even: #0f172a;
                --good: #14532d;
                --warn: #78350f;
                --bad: #7f1d1d;
                --lvl1: #172554;
                --lvl2: #3b0764;
                --lvl3: #713f12;
                --lvl4: #14532d;
                --lvl5: #4c0519;
                --lvl6: #1e293b;
            }}
            * {{ box-sizing: border-box; }}
            body {{
                margin: 0;
                background: linear-gradient(180deg, var(--bg), #ffffff00 400px), var(--bg);
                color: var(--text);
                font-family: "Trebuchet MS", "Avenir Next", "Segoe UI", Arial, sans-serif;
            }}
            .app {{ display: flex; min-height: 100vh; }}
            .sidebar {{
                width: 260px;
                background: linear-gradient(180deg, var(--panel), var(--panel-2));
                border-right: 1px solid var(--border);
                padding: 20px 16px;
                box-shadow: var(--shadow);
                position: sticky;
                top: 0;
                height: 100vh;
            }}
            .sidebar-brand {{
                font-size: 26px;
                font-weight: 900;
                margin-bottom: 20px;
                background: linear-gradient(90deg, var(--accent1), var(--accent2), var(--accent3));
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                background-clip: text;
                display: flex;
                align-items: center;
                gap: 10px;
            }}
            .brand-mark {{
                display: inline-flex;
                align-items: center;
                justify-content: center;
                width: 18px;
                height: 18px;
                border-radius: 999px;
                background: linear-gradient(135deg, var(--accent1), var(--accent2), var(--accent3));
                color: transparent;
                box-shadow: 0 0 0 4px rgba(37, 99, 235, 0.12);
                flex-shrink: 0;
            }}
            .sidebar-group {{ background: var(--panel); border: 1px solid var(--border); border-radius: 14px; padding: 10px 12px; }}
            .sidebar-group summary {{ cursor: pointer; font-weight: bold; font-size: 15px; color: var(--text); list-style: none; }}
            .sidebar-group summary::-webkit-details-marker {{ display: none; }}
            .sidebar-links {{ margin-top: 10px; display: flex; flex-direction: column; gap: 8px; }}
            .sidebar-links a {{ display: block; text-decoration: none; color: var(--text); padding: 10px 12px; border-radius: 10px; font-weight: 700; }}
            .sidebar-links a:hover {{ background: var(--panel-2); }}
            .active-link {{ background: linear-gradient(90deg, var(--accent1), var(--accent2)); color: white !important; }}
            .main {{ flex: 1; padding: 24px; overflow-x: auto; }}
            .topbar {{ display: flex; justify-content: space-between; align-items: center; gap: 16px; margin-bottom: 20px; flex-wrap: wrap; }}
            .page-title {{ font-size: 36px; font-weight: 900; background: linear-gradient(90deg, var(--accent1), var(--accent2), var(--accent3)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text; }}
            .subtitle {{ color: var(--muted); font-size: 14px; margin-top: 6px; }}
            .top-actions {{ display: flex; gap: 10px; flex-wrap: wrap; }}
            .theme-toggle, .small-btn, .filters button, .filters a, .upload-btn {{
                border: 1px solid var(--border);
                background: linear-gradient(90deg, var(--accent1), var(--accent2));
                color: white;
                padding: 10px 14px;
                border-radius: 12px;
                cursor: pointer;
                text-decoration: none;
                font-weight: 800;
                display: inline-flex;
                align-items: center;
                justify-content: center;
                gap: 6px;
            }}
            .panel, .column-panel {{
                background: var(--panel);
                border: 1px solid var(--border);
                border-radius: 18px;
                box-shadow: var(--shadow);
                padding: 16px;
                margin-bottom: 18px;
            }}
            .column-panel-title {{ font-size: 16px; font-weight: 900; margin-bottom: 12px; }}
            .hint {{ color: var(--muted); font-size: 13px; margin-top: 8px; }}
            .filters form, .upload-form {{ display: flex; gap: 12px; flex-wrap: wrap; align-items: end; }}
            .filters label, .upload-form label {{ display: flex; flex-direction: column; font-size: 13px; font-weight: 800; color: var(--text); }}
            .filters input, .filters select, .upload-form input {{
                margin-top: 6px;
                padding: 10px 12px;
                min-width: 170px;
                border-radius: 12px;
                border: 1px solid var(--border);
                background: var(--panel-2);
                color: var(--text);
            }}
            .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; }}
            .card {{ background: linear-gradient(180deg, var(--panel), var(--panel-2)); border: 1px solid var(--border); padding: 15px; border-radius: 14px; }}
            .card-title {{ font-size: 13px; color: var(--muted); margin-bottom: 8px; }}
            .card-value {{ font-size: 24px; font-weight: 900; color: var(--text); }}
            .controls-row {{ display: flex; gap: 12px; flex-wrap: wrap; align-items: center; justify-content: space-between; margin-bottom: 12px; }}
            .column-list {{ display: flex; flex-wrap: wrap; gap: 10px 18px; }}
            .column-list label {{ display: inline-flex; align-items: center; gap: 8px; font-size: 14px; color: var(--text); }}
            .table-wrap {{ overflow: auto; border-radius: 18px; border: 1px solid var(--border); box-shadow: var(--shadow); background: var(--panel); }}
            table {{ border-collapse: collapse; width: 100%; min-width: 1900px; table-layout: fixed; background: var(--panel); color: var(--text); }}
            th, td {{ border-right: 1px solid var(--border); border-bottom: 1px solid var(--border); padding: 10px 12px; text-align: left; font-size: 14px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
            th {{ background: var(--table-head); color: var(--table-head-text); position: sticky; top: 0; z-index: 2; }}
            th a {{ color: inherit; text-decoration: none; }}
            tbody tr:nth-child(even) {{ background: var(--row-even); }}
            tr.good-row {{ background: var(--good); }}
            tr.warn-row {{ background: var(--warn); }}
            tr.bad-row {{ background: var(--bad); }}
            body.hide-row-colors tr.good-row, body.hide-row-colors tr.warn-row, body.hide-row-colors tr.bad-row {{ background: transparent !important; }}
            .tree-root details {{ margin-bottom: 10px; border: 1px solid var(--border); border-radius: 14px; overflow: hidden; background: var(--panel); }}
            .tree-root summary {{ cursor: pointer; font-weight: 800; color: var(--text); list-style: none; display: flex; justify-content: space-between; gap: 12px; flex-wrap: wrap; padding: 12px 14px; }}
            .tree-root summary::-webkit-details-marker {{ display: none; }}
            .tree-line {{ display: grid; grid-template-columns: 2.8fr 1fr 1fr 1fr 1fr 1fr; gap: 10px; padding: 10px 14px; border-top: 1px solid var(--border); align-items: center; font-size: 14px; background: var(--panel); }}
            .tree-level-1 > summary {{ background: var(--lvl1); font-size: 18px; }}
            .tree-level-2 > summary {{ background: var(--lvl2); font-size: 16px; }}
            .tree-level-3 > summary {{ background: var(--lvl3); font-size: 15px; }}
            .tree-level-4 > summary {{ background: var(--lvl4); font-size: 14px; }}
            .tree-level-5 > summary {{ background: var(--lvl5); font-size: 14px; }}
            .tree-level-6 > .tree-line {{ background: var(--lvl6); }}
            .tree-name {{ font-weight: 800; }}
            @media (max-width: 900px) {{
                .app {{ display: block; }}
                .sidebar {{ position: relative; width: 100%; height: auto; }}
                .main {{ padding: 14px; }}
                .page-title {{ font-size: 28px; }}
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
                        <button class="theme-toggle" onclick="toggleRowColors()">🎨 Цвета строк</button>
                        <button class="theme-toggle" onclick="toggleTheme()">🌙 / ☀️ Тема</button>
                    </div>
                </div>
                {content}
            </main>
        </div>
        <script>
            function toggleTheme() {{
                const body = document.body;
                body.classList.toggle('dark');
                localStorage.setItem('teambead-theme', body.classList.contains('dark') ? 'dark' : 'light');
            }}
            (function initTheme() {{
                const savedTheme = localStorage.getItem('teambead-theme');
                if (savedTheme === 'dark') document.body.classList.add('dark');
            }})();
            function toggleRowColors() {{
                const body = document.body;
                body.classList.toggle('hide-row-colors');
                localStorage.setItem('teambead-row-colors', body.classList.contains('hide-row-colors') ? 'off' : 'on');
            }}
            (function initRowColors() {{
                const saved = localStorage.getItem('teambead-row-colors');
                if (saved === 'off') document.body.classList.add('hide-row-colors');
            }})();
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
    return f'<a href="/grouped?{qs}">{escape(label)}{arrow}</a>'



def render_stats_cards(totals):
    cards = [
        ("Spend", format_money(totals["spend"])),
        ("Clicks", format_int_or_float(totals["clicks"])),
        ("Leads", format_int_or_float(totals["leads"])),
        ("REG", format_int_or_float(totals["reg"])),
        ("FTD", format_int_or_float(totals["ftd"])),
        ("CPC", format_money(totals["cpc_real"])),
        ("CPL", format_money(totals["cpl_real"])),
        ("CPA", format_money(totals["cpa_real"])),
        ("CR REG", format_percent(totals["cr_reg"])),
        ("CR FTD", format_percent(totals["cr_ftd"])),
    ]
    html = '<div class="panel"><div class="stats">'
    for title, value in cards:
        html += f'<div class="card"><div class="card-title">{title}</div><div class="card-value">{value}</div></div>'
    html += "</div></div>"
    return html



def render_tree_nodes(nodes, level=1):
    html = ""
    level_class = f"tree-level-{min(level, 6)}"
    for node in nodes:
        m = node["metrics"]
        summary_right = (
            f'<span>Spend: {format_money(m["spend"])} | Clicks: {format_int_or_float(m["clicks"])} | '
            f'Leads: {format_int_or_float(m["leads"])} | REG: {format_int_or_float(m["reg"])} | FTD: {format_int_or_float(m["ftd"])} </span>'
        )
        if node["children"]:
            children_html = render_tree_nodes(node["children"], level + 1)
            html += f'''
            <details class="{level_class}" open>
                <summary><span class="tree-name">{escape(node["name"])} ({escape(node["key"])})</span>{summary_right}</summary>
                {children_html}
            </details>
            '''
        else:
            html += f'''
            <div class="tree-line {level_class}">
                <div class="tree-name">{escape(node["name"])} ({escape(node["key"])})</div>
                <div>{format_money(m["spend"])}</div>
                <div>{format_int_or_float(m["clicks"])}</div>
                <div>{format_int_or_float(m["leads"])}</div>
                <div>{format_int_or_float(m["reg"])}</div>
                <div>{format_int_or_float(m["ftd"])}</div>
            </div>
            '''
    return html


# =========================================
# BLOCK 8 — UPLOAD
# =========================================
@app.post("/upload")
async def upload_file(uploader: str = Form(...), file: UploadFile = File(...)):
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

        rows_to_insert = parse_uploaded_dataframe(df, uploader.strip())

        db = SessionLocal()
        try:
            db.query(FBRow).filter(FBRow.uploader == uploader.strip()).delete()
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
    uploader: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    offer: str = Query(default=""),
    search: str = Query(default=""),
    sort_by: str = Query(default="spend"),
    order: str = Query(default="desc"),
):
    rows = aggregate_grouped_rows(get_filtered_data(uploader, manager, geo, offer, search))
    reverse = order.lower() != "asc"
    rows.sort(key=lambda x: x.get(sort_by, 0) if x.get(sort_by) is not None else 0, reverse=reverse)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Uploader", "Ad Name", "Platform", "Manager", "Geo", "Offer", "Creative",
        "Rows", "Clicks", "Leads", "REG", "FTD", "Spend", "CPC", "CPL", "CPA",
        "CR REG", "CR FTD", "Date Start", "Date End"
    ])
    for row in rows:
        writer.writerow([
            row["uploader"], row["ad_name"], row["platform"], row["manager"], row["geo"], row["offer"], row["creative"],
            format_int_or_float(row["rows_combined"]), format_int_or_float(row["clicks"]), format_int_or_float(row["leads"]),
            format_int_or_float(row["reg"]), format_int_or_float(row["ftd"]), format_money(row["spend"]),
            format_money(row["cpc_real"]), format_money(row["cpl_real"]), format_money(row["cpa_real"]),
            format_percent(row["cr_reg"]), format_percent(row["cr_ftd"]), row["date_start"], row["date_end"],
        ])
    output.seek(0)
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv; charset=utf-8", headers={"Content-Disposition": "attachment; filename=teambead_grouped.csv"})


@app.get("/export/hierarchy")
def export_hierarchy_csv(
    uploader: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    offer: str = Query(default=""),
    search: str = Query(default=""),
):
    rows = aggregate_grouped_rows(get_filtered_data(uploader, manager, geo, offer, search))
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Geo", "Platform", "Manager", "Offer", "Creative", "Ad Name", "Clicks", "Leads", "REG", "FTD", "Spend"])
    for row in rows:
        writer.writerow([
            row["geo"], row["platform"], row["manager"], row["offer"], row["creative"], row["ad_name"],
            format_int_or_float(row["clicks"]), format_int_or_float(row["leads"]), format_int_or_float(row["reg"]),
            format_int_or_float(row["ftd"]), format_money(row["spend"]),
        ])
    output.seek(0)
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv; charset=utf-8", headers={"Content-Disposition": "attachment; filename=teambead_hierarchy.csv"})


# =========================================
# BLOCK 10 — GROUPED PAGE
# =========================================
@app.get("/grouped", response_class=HTMLResponse)
def show_grouped_table(
    uploader: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    offer: str = Query(default=""),
    search: str = Query(default=""),
    sort_by: str = Query(default="spend"),
    order: str = Query(default="desc"),
):
    data = get_filtered_data(uploader, manager, geo, offer, search)
    rows = aggregate_grouped_rows(data)
    all_uploaders, all_managers, all_geos, all_offers = get_filter_options()

    allowed_sort_fields = {
        "uploader", "ad_name", "platform", "manager", "geo", "offer", "creative",
        "rows_combined", "clicks", "leads", "reg", "ftd", "spend", "cpc_real", "cpl_real", "cpa_real", "cr_reg", "cr_ftd",
        "date_start", "date_end"
    }
    if sort_by not in allowed_sort_fields:
        sort_by = "spend"
    reverse = order.lower() != "asc"
    rows.sort(key=lambda x: x.get(sort_by, 0) if x.get(sort_by) is not None else 0, reverse=reverse)

    totals = aggregate_totals(rows)
    uploader_options = make_options(all_uploaders, uploader)
    manager_options = make_options(all_managers, manager)
    geo_options = make_options(all_geos, geo)
    offer_options = make_options(all_offers, offer)
    export_qs = build_query_string(uploader=uploader, manager=manager, geo=geo, offer=offer, search=search, sort_by=sort_by, order=order)
    export_link = f"/export/grouped?{export_qs}" if export_qs else "/export/grouped"

    rows_html = ""
    for row in rows:
        rows_html += f'''
        <tr class="{get_row_class(row)}">
            <td data-col="uploader">{escape(row["uploader"])}</td>
            <td data-col="ad_name">{escape(row["ad_name"])}</td>
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
            <td data-col="cpc_real">{format_money(row["cpc_real"])}</td>
            <td data-col="cpl_real">{format_money(row["cpl_real"])}</td>
            <td data-col="cpa_real">{format_money(row["cpa_real"])}</td>
            <td data-col="cr_reg">{format_percent(row["cr_reg"])}</td>
            <td data-col="cr_ftd">{format_percent(row["cr_ftd"])}</td>
            <td data-col="date_start">{escape(row["date_start"])}</td>
            <td data-col="date_end">{escape(row["date_end"])}</td>
        </tr>
        '''

    table_headers = [
        ("uploader", "Uploader"), ("ad_name", "Ad Name"), ("platform", "Platform"), ("manager", "Manager"),
        ("geo", "Geo"), ("offer", "Offer"), ("creative", "Creative"), ("rows_combined", "Rows"),
        ("clicks", "Clicks"), ("leads", "Leads"), ("reg", "REG"), ("ftd", "FTD"), ("spend", "Spend"),
        ("cpc_real", "CPC"), ("cpl_real", "CPL"), ("cpa_real", "CPA"), ("cr_reg", "CR REG"),
        ("cr_ftd", "CR FTD"), ("date_start", "Date Start"), ("date_end", "Date End")
    ]

    head_html = ""
    for field, label in table_headers:
        head_html += f'<th data-col="{field}">{sort_link(label, field, sort_by, order, uploader=uploader, manager=manager, geo=geo, offer=offer, search=search)}</th>'

    column_toggles = ""
    for field, label in table_headers:
        column_toggles += f'<label><input type="checkbox" class="column-toggle" value="{field}" checked> {escape(label)}</label>'

    content = f'''
    <div class="panel">
        <div class="column-panel-title">Загрузка данных</div>
        <form method="post" action="/upload" enctype="multipart/form-data" class="upload-form">
            <label>Uploader
                <input type="text" name="uploader" required placeholder="Например: TeamBead1">
            </label>
            <label>Файл CSV / XLSX
                <input type="file" name="file" accept=".csv,.xlsx,.xls" required>
            </label>
            <button type="submit" class="upload-btn">Загрузить</button>
        </form>
        <div class="hint">Если загружаешь тот же uploader повторно — старые строки заменятся новыми.</div>
    </div>

    <div class="panel">
        <div class="controls-row">
            <div class="column-panel-title">Экспорт</div>
            <a class="small-btn" href="{export_link}">⬇ Выгрузить CSV</a>
        </div>
    </div>

    <div class="panel filters">
        <form method="get" action="/grouped">
            <label>Uploader<select name="uploader">{uploader_options}</select></label>
            <label>Manager<select name="manager">{manager_options}</select></label>
            <label>Geo<select name="geo">{geo_options}</select></label>
            <label>Offer<select name="offer">{offer_options}</select></label>
            <label>Search<input type="text" name="search" value="{escape(search)}"></label>
            <input type="hidden" name="sort_by" value="{escape(sort_by)}">
            <input type="hidden" name="order" value="{escape(order)}">
            <button type="submit">Фильтровать</button>
            <a href="/grouped">Сбросить</a>
        </form>
    </div>

    {render_stats_cards(totals)}

    <div class="column-panel">
        <div class="controls-row">
            <div class="column-panel-title">Колонки</div>
            <button type="button" class="small-btn" onclick="resetColumns()">Сбросить колонки</button>
        </div>
        <div class="column-list">{column_toggles}</div>
        <div class="hint">Настройки колонок сохраняются в браузере.</div>
    </div>

    <div class="table-wrap">
        <table id="groupedTable">
            <thead><tr>{head_html}</tr></thead>
            <tbody>{rows_html if rows_html else '<tr><td colspan="20">Нет данных</td></tr>'}</tbody>
        </table>
    </div>
    '''

    extra_scripts = '''
    <script>
        const STORAGE_KEY = 'teambead-grouped-hidden-columns';
        function applyColumns() {
            const hidden = JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]');
            document.querySelectorAll('.column-toggle').forEach(cb => {
                cb.checked = !hidden.includes(cb.value);
            });
            document.querySelectorAll('[data-col]').forEach(el => {
                el.style.display = hidden.includes(el.dataset.col) ? 'none' : '';
            });
        }
        function saveColumns() {
            const hidden = [];
            document.querySelectorAll('.column-toggle').forEach(cb => {
                if (!cb.checked) hidden.push(cb.value);
            });
            localStorage.setItem(STORAGE_KEY, JSON.stringify(hidden));
            applyColumns();
        }
        function resetColumns() {
            localStorage.removeItem(STORAGE_KEY);
            applyColumns();
        }
        document.querySelectorAll('.column-toggle').forEach(cb => cb.addEventListener('change', saveColumns));
        applyColumns();
    </script>
    '''

    return page_shell("FB — Выгрузка", content, "grouped", extra_scripts)


# =========================================
# BLOCK 11 — HIERARCHY PAGE
# =========================================
@app.get("/hierarchy", response_class=HTMLResponse)
def show_hierarchy(
    uploader: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    offer: str = Query(default=""),
    search: str = Query(default=""),
):
    data = get_filtered_data(uploader, manager, geo, offer, search)
    rows = aggregate_grouped_rows(data)
    all_uploaders, all_managers, all_geos, all_offers = get_filter_options()

    uploader_options = make_options(all_uploaders, uploader)
    manager_options = make_options(all_managers, manager)
    geo_options = make_options(all_geos, geo)
    offer_options = make_options(all_offers, offer)

    hierarchy_keys = ["geo", "platform", "manager", "offer", "creative", "ad_name"]
    tree = aggregate_for_hierarchy(rows, hierarchy_keys)
    tree_html = render_tree_nodes(tree) if tree else '<div class="panel">Нет данных</div>'
    totals = aggregate_totals(rows)

    export_qs = build_query_string(uploader=uploader, manager=manager, geo=geo, offer=offer, search=search)
    export_link = f"/export/hierarchy?{export_qs}" if export_qs else "/export/hierarchy"

    content = f'''
    <div class="panel">
        <div class="controls-row">
            <div class="column-panel-title">Экспорт</div>
            <a class="small-btn" href="{export_link}">⬇ Выгрузить CSV</a>
        </div>
    </div>

    <div class="panel filters">
        <form method="get" action="/hierarchy">
            <label>Uploader<select name="uploader">{uploader_options}</select></label>
            <label>Manager<select name="manager">{manager_options}</select></label>
            <label>Geo<select name="geo">{geo_options}</select></label>
            <label>Offer<select name="offer">{offer_options}</select></label>
            <label>Search<input type="text" name="search" value="{escape(search)}"></label>
            <button type="submit">Фильтровать</button>
            <a href="/hierarchy">Сбросить</a>
        </form>
    </div>

    {render_stats_cards(totals)}

    <div class="panel">
        <div class="column-panel-title">Статистика по уровням</div>
        <div class="hint">Порядок уровней: Geo → Platform → Manager → Offer → Creative → Ad Name</div>
    </div>

    <div class="tree-root">{tree_html}</div>
    '''
    return page_shell("FB — Статистика", content, "hierarchy")
