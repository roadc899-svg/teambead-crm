from fastapi import FastAPI, UploadFile, File, Query, Form
from fastapi.responses import HTMLResponse, StreamingResponse
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker, declarative_base
import pandas as pd
import shutil
import uuid
import os
from urllib.parse import quote_plus
import io
import csv

DATABASE_URL = "sqlite:///./test.db"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


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

app = FastAPI()


@app.get("/")
def home():
    return RedirectResponse(url="/grouped", status_code=302)


def safe_number(value):
    if value is None:
        return 0.0
    try:
        if pd.isna(value):
            return 0.0
        if isinstance(value, str):
            value = value.replace(" ", "").replace(",", ".")
        return float(value)
    except:
        return 0.0


def format_int_or_float(value):
    if value is None:
        return "0"
    try:
        value = float(value)
        if value.is_integer():
            return str(int(value))
        return f"{value:.2f}".rstrip("0").rstrip(".")
    except:
        return str(value)


def format_money(value):
    if value is None:
        value = 0
    try:
        return f"${float(value):,.2f}"
    except:
        return "$0.00"


def format_percent(value):
    if value is None:
        value = 0
    try:
        return f"{float(value):.2f}%"
    except:
        return "0.00%"


def parse_ad_name(ad_name):
    if not ad_name:
        return {
            "launch_date": None,
            "platform": None,
            "manager": None,
            "geo": None,
            "offer": None,
            "creative": None,
        }

    parts = str(ad_name).split("/")

    return {
        "launch_date": parts[0] if len(parts) > 0 else None,
        "platform": parts[1] if len(parts) > 1 else None,
        "manager": parts[2] if len(parts) > 2 else None,
        "geo": parts[3] if len(parts) > 3 else None,
        "offer": parts[4] if len(parts) > 4 else None,
        "creative": parts[5] if len(parts) > 5 else None,
    }


def calc_metrics(clicks, reg, ftd, spend):
    clicks = clicks or 0
    reg = reg or 0
    ftd = ftd or 0
    spend = spend or 0

    cpc_real = spend / clicks if clicks > 0 else 0
    cpl_real = spend / reg if reg > 0 else 0
    cpa_real = spend / ftd if ftd > 0 else 0
    cr_reg = (reg / clicks) * 100 if clicks > 0 else 0
    cr_ftd = (ftd / clicks) * 100 if clicks > 0 else 0

    return {
        "cpc_real": cpc_real,
        "cpl_real": cpl_real,
        "cpa_real": cpa_real,
        "cr_reg": cr_reg,
        "cr_ftd": cr_ftd,
    }


def get_all_rows():
    db = SessionLocal()
    data = db.query(FBRow).all()
    db.close()
    return data


def get_filtered_data(uploader="", manager="", geo="", offer="", search=""):
    data = get_all_rows()
    filtered = []

    for row in data:
        row_uploader = row.uploader or ""
        row_manager = row.manager or ""
        row_geo = row.geo or ""
        row_offer = row.offer or ""
        row_ad_name = row.ad_name or ""

        if uploader and row_uploader != uploader:
            continue
        if manager and row_manager != manager:
            continue
        if geo and row_geo != geo:
            continue
        if offer and row_offer != offer:
            continue
        if search and search.lower() not in row_ad_name.lower():
            continue

        filtered.append(row)

    return filtered


def get_filter_options():
    data = get_all_rows()

    all_uploaders = sorted(set(row.uploader for row in data if row.uploader))
    all_managers = sorted(set(row.manager for row in data if row.manager))
    all_geos = sorted(set(row.geo for row in data if row.geo))
    all_offers = sorted(set(row.offer for row in data if row.offer))

    return all_uploaders, all_managers, all_geos, all_offers


def make_options(options, selected_value):
    html = '<option value="">Все</option>'
    for option in options:
        selected = "selected" if option == selected_value else ""
        html += f'<option value="{option}" {selected}>{option}</option>'
    return html


def aggregate_grouped_rows(rows):
    grouped = {}

    for row in rows:
        key = f"{row.uploader or ''}|||{row.ad_name or ''}"

        if key not in grouped:
            grouped[key] = {
                "uploader": row.uploader,
                "ad_name": row.ad_name,
                "launch_date": row.launch_date,
                "platform": row.platform,
                "manager": row.manager,
                "geo": row.geo,
                "offer": row.offer,
                "creative": row.creative,
                "leads": 0.0,
                "reg": 0.0,
                "ftd": 0.0,
                "clicks": 0.0,
                "spend": 0.0,
                "rows_combined": 0,
                "date_start": row.date_start,
                "date_end": row.date_end,
            }

        grouped[key]["leads"] += row.leads or 0
        grouped[key]["reg"] += row.reg or 0
        grouped[key]["ftd"] += row.ftd or 0
        grouped[key]["clicks"] += row.clicks or 0
        grouped[key]["spend"] += row.spend or 0
        grouped[key]["rows_combined"] += 1

    result = list(grouped.values())

    for item in result:
        item.update(calc_metrics(item["clicks"], item["reg"], item["ftd"], item["spend"]))

    return result


def aggregate_totals(rows):
    total_clicks = sum(r["clicks"] for r in rows)
    total_leads = sum(r["leads"] for r in rows)
    total_reg = sum(r["reg"] for r in rows)
    total_ftd = sum(r["ftd"] for r in rows)
    total_spend = sum(r["spend"] for r in rows)

    metrics = calc_metrics(total_clicks, total_reg, total_ftd, total_spend)

    return {
        "clicks": total_clicks,
        "leads": total_leads,
        "reg": total_reg,
        "ftd": total_ftd,
        "spend": total_spend,
        **metrics
    }


def sidebar_html(active_page):
    grouped_active = "active-link" if active_page == "grouped" else ""
    hierarchy_active = "active-link" if active_page == "hierarchy" else ""

    return f"""
    <aside class="sidebar">
        <div class="sidebar-brand">TEAMbead CRM</div>

        <details class="sidebar-group" open>
            <summary>FB</summary>
            <div class="sidebar-links">
                <a href="/grouped" class="{grouped_active}">Сводка</a>
                <a href="/hierarchy" class="{hierarchy_active}">Иерархия</a>
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
        <title>{title}</title>
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
                --accent1: #a855f7;
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

            * {{
                box-sizing: border-box;
            }}

            body {{
                margin: 0;
                background: linear-gradient(180deg, var(--bg), #ffffff00 400px), var(--bg);
                color: var(--text);
                font-family: Arial, sans-serif;
            }}

            .app {{
                display: flex;
                min-height: 100vh;
            }}

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
            }}

            .sidebar-group {{
                background: var(--panel);
                border: 1px solid var(--border);
                border-radius: 14px;
                padding: 10px 12px;
            }}

            .sidebar-group summary {{
                cursor: pointer;
                font-weight: bold;
                font-size: 15px;
                color: var(--text);
                list-style: none;
            }}

            .sidebar-group summary::-webkit-details-marker {{
                display: none;
            }}

            .sidebar-links {{
                margin-top: 10px;
                display: flex;
                flex-direction: column;
                gap: 8px;
            }}

            .sidebar-links a {{
                display: block;
                text-decoration: none;
                color: var(--text);
                padding: 10px 12px;
                border-radius: 10px;
                background: transparent;
                border: 1px solid transparent;
                font-weight: 600;
            }}

            .sidebar-links a:hover {{
                background: var(--panel-2);
                border-color: var(--border);
            }}

            .active-link {{
                background: linear-gradient(90deg, var(--accent1), var(--accent2));
                color: white !important;
                font-weight: bold;
            }}

            .main {{
                flex: 1;
                padding: 24px;
                overflow-x: auto;
            }}

            .topbar {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 16px;
                margin-bottom: 20px;
                flex-wrap: wrap;
            }}

            .page-title {{
                font-size: 36px;
                font-weight: 900;
                background: linear-gradient(90deg, var(--accent1), var(--accent2), var(--accent3));
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                background-clip: text;
            }}

            .subtitle {{
                color: var(--muted);
                font-size: 14px;
                margin-top: 6px;
            }}

            .top-actions {{
                display: flex;
                gap: 10px;
                flex-wrap: wrap;
            }}

            .theme-toggle {{
                border: 1px solid var(--border);
                background: var(--panel);
                color: var(--text);
                padding: 10px 14px;
                border-radius: 12px;
                cursor: pointer;
                box-shadow: var(--shadow);
                font-weight: bold;
            }}

            .panel {{
                background: var(--panel);
                border: 1px solid var(--border);
                border-radius: 18px;
                box-shadow: var(--shadow);
                padding: 16px;
                margin-bottom: 18px;
            }}

            .filters form {{
                display: flex;
                gap: 12px;
                flex-wrap: wrap;
                align-items: end;
            }}

            .filters label {{
                display: flex;
                flex-direction: column;
                font-size: 13px;
                font-weight: bold;
                color: var(--text);
            }}

            .filters input, .filters select {{
                margin-top: 6px;
                padding: 10px 12px;
                min-width: 170px;
                border-radius: 12px;
                border: 1px solid var(--border);
                background: var(--panel-2);
                color: var(--text);
            }}

            .filters button, .filters a, .small-btn {{
                padding: 11px 16px;
                border: none;
                background: linear-gradient(90deg, var(--accent1), var(--accent2));
                color: white;
                text-decoration: none;
                cursor: pointer;
                border-radius: 12px;
                font-size: 14px;
                font-weight: bold;
                display: inline-flex;
                align-items: center;
                justify-content: center;
            }}

            .small-btn {{
                padding: 8px 12px;
                font-size: 13px;
            }}

            .stats {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                gap: 12px;
            }}

            .card {{
                background: linear-gradient(180deg, var(--panel), var(--panel-2));
                border: 1px solid var(--border);
                padding: 15px;
                border-radius: 14px;
            }}

            .card-title {{
                font-size: 13px;
                color: var(--muted);
                margin-bottom: 8px;
            }}

            .card-value {{
                font-size: 24px;
                font-weight: bold;
                color: var(--text);
            }}

            .table-wrap {{
                overflow: auto;
                border-radius: 18px;
                border: 1px solid var(--border);
                box-shadow: var(--shadow);
                background: var(--panel);
            }}

            table {{
                border-collapse: collapse;
                width: 100%;
                min-width: 1900px;
                table-layout: fixed;
                background: var(--panel);
                color: var(--text);
            }}

            th, td {{
                border-right: 1px solid var(--border);
                border-bottom: 1px solid var(--border);
                padding: 10px 12px;
                text-align: left;
                font-size: 14px;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
            }}

            th {{
                background: var(--table-head);
                color: var(--table-head-text);
                position: sticky;
                top: 0;
                z-index: 2;
                user-select: none;
            }}

            th a {{
                color: inherit;
                text-decoration: none;
            }}

            tbody tr:nth-child(even) {{
                background: var(--row-even);
            }}

            tr.good-row {{
                background: var(--good);
            }}

            tr.warn-row {{
                background: var(--warn);
            }}

            tr.bad-row {{
                background: var(--bad);
            }}

            body.hide-row-colors tr.good-row,
            body.hide-row-colors tr.warn-row,
            body.hide-row-colors tr.bad-row {{
                background: transparent !important;
            }}

            .controls-row {{
                display: flex;
                gap: 12px;
                flex-wrap: wrap;
                align-items: center;
                justify-content: space-between;
                margin-bottom: 12px;
            }}

            .column-panel {{
                background: var(--panel);
                border: 1px solid var(--border);
                border-radius: 18px;
                box-shadow: var(--shadow);
                padding: 16px;
                margin-bottom: 18px;
            }}

            .column-panel-title {{
                font-size: 16px;
                font-weight: bold;
                margin-bottom: 12px;
            }}

            .column-list {{
                display: flex;
                flex-wrap: wrap;
                gap: 10px 18px;
            }}

            .column-list label {{
                display: inline-flex;
                align-items: center;
                gap: 8px;
                font-size: 14px;
                color: var(--text);
            }}

            .hint {{
                color: var(--muted);
                font-size: 13px;
                margin-top: 10px;
            }}

            th.resizable {{
                position: sticky;
            }}

            .resize-handle {{
                position: absolute;
                top: 0;
                right: 0;
                width: 8px;
                height: 100%;
                cursor: col-resize;
            }}

            .drag-handle {{
                margin-right: 8px;
                cursor: grab;
                opacity: 0.75;
                font-weight: bold;
            }}

            .dragging {{
                opacity: 0.5;
            }}

            .tree-root details {{
                margin-bottom: 10px;
                border: 1px solid var(--border);
                border-radius: 14px;
                overflow: hidden;
                background: var(--panel);
            }}

            .tree-root summary {{
                cursor: pointer;
                font-weight: bold;
                color: var(--text);
                list-style: none;
                display: flex;
                justify-content: space-between;
                gap: 12px;
                flex-wrap: wrap;
                padding: 12px 14px;
            }}

            .tree-root summary::-webkit-details-marker {{
                display: none;
            }}

            .tree-line {{
                display: grid;
                grid-template-columns: 2.6fr 1fr 1fr 1fr 1fr 1fr 1fr 1fr;
                gap: 10px;
                padding: 10px 14px;
                border-top: 1px solid var(--border);
                align-items: center;
                font-size: 14px;
                background: var(--panel);
            }}

            .tree-header {{
                font-weight: bold;
                color: var(--muted);
                background: transparent;
                border: none;
                padding-left: 0;
                margin-bottom: 8px;
            }}

            .tree-level-1 > summary {{ background: var(--lvl1); font-size: 18px; }}
            .tree-level-2 > summary {{ background: var(--lvl2); font-size: 16px; }}
            .tree-level-3 > summary {{ background: var(--lvl3); font-size: 15px; }}
            .tree-level-4 > summary {{ background: var(--lvl4); font-size: 14px; }}
            .tree-level-5 > summary {{ background: var(--lvl5); font-size: 14px; }}
            .tree-level-6 > .tree-line {{ background: var(--lvl6); }}

            .tree-name {{
                font-weight: bold;
            }}
        </style>
    </head>
    <body>
        <div class="app">
            {sidebar}
            <main class="main">
                <div class="topbar">
                    <div>
                        <div class="page-title">{title}</div>
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
                if (savedTheme === 'dark') {{
                    document.body.classList.add('dark');
                }}
            }})();

            function toggleRowColors() {{
                const body = document.body;
                body.classList.toggle('hide-row-colors');
                localStorage.setItem(
                    'teambead-row-colors',
                    body.classList.contains('hide-row-colors') ? 'off' : 'on'
                );
            }}

            (function initRowColors() {{
                const saved = localStorage.getItem('teambead-row-colors');
                if (saved === 'off') {{
                    document.body.classList.add('hide-row-colors');
                }}
            }})();
        </script>
        {extra_scripts}
    </body>
    </html>
    """


@app.post("/upload")
async def upload_file(
    uploader: str = Form(...),
    file: UploadFile = File(...)
):
    filename = f"temp_{uuid.uuid4()}.csv"

    with open(filename, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    df = pd.read_csv(filename)

    db = SessionLocal()

    db.query(FBRow).filter(FBRow.uploader == uploader).delete()
    db.commit()

    for _, row in df.iterrows():
        parsed = parse_ad_name(row.get("Название объявления"))

        item = FBRow(
            uploader=uploader,
            ad_name=row.get("Название объявления"),
            launch_date=parsed["launch_date"],
            platform=parsed["platform"],
            manager=parsed["manager"],
            geo=parsed["geo"],
            offer=parsed["offer"],
            creative=parsed["creative"],
            leads=safe_number(row.get("Лиды")),
            reg=safe_number(row.get("Завершенные регистрации")),
            ftd=safe_number(row.get("Покупки")),
            clicks=safe_number(row.get("Клики по ссылке")),
            spend=safe_number(row.get("Потраченная сумма (USD)")),
            cpc=safe_number(row.get("CPC (цена за клик по ссылке)")),
            ctr=safe_number(row.get("CTR (все)")),
            date_start=str(row.get("Дата начала отчетности") or ""),
            date_end=str(row.get("Дата окончания отчетности") or ""),
        )

        db.add(item)

    db.commit()
    db.close()

    os.remove(filename)

    return {
        "status": "ok",
        "uploader": uploader,
        "rows_loaded": len(df)
    }


@app.get("/data")
def get_data():
    data = get_all_rows()

    result = []
    for row in data:
        result.append({
            "id": row.id,
            "uploader": row.uploader,
            "ad_name": row.ad_name,
            "launch_date": row.launch_date,
            "platform": row.platform,
            "manager": row.manager,
            "geo": row.geo,
            "offer": row.offer,
            "creative": row.creative,
            "leads": row.leads,
            "reg": row.reg,
            "ftd": row.ftd,
            "clicks": row.clicks,
            "spend": row.spend,
            "date_start": row.date_start,
            "date_end": row.date_end,
        })
    return result


@app.get("/export/grouped")
def export_grouped_csv(
    uploader: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    offer: str = Query(default=""),
    search: str = Query(default="")
):
    data = get_filtered_data(uploader, manager, geo, offer, search)
    rows = aggregate_grouped_rows(data)

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "Uploader",
        "Ad Name",
        "Platform",
        "Manager",
        "Geo",
        "Offer",
        "Creative",
        "Rows",
        "Clicks",
        "Leads",
        "REG",
        "FTD",
        "Spend",
        "Real CPC",
        "Real CPL",
        "Real CPA",
        "CR REG",
        "CR FTD",
        "Date Start",
        "Date End",
    ])

    for row in rows:
        writer.writerow([
            row["uploader"] or "",
            row["ad_name"] or "",
            row["platform"] or "",
            row["manager"] or "",
            row["geo"] or "",
            row["offer"] or "",
            row["creative"] or "",
            format_int_or_float(row["rows_combined"]),
            format_int_or_float(row["clicks"]),
            format_int_or_float(row["leads"]),
            format_int_or_float(row["reg"]),
            format_int_or_float(row["ftd"]),
            row["spend"],
            row["cpc_real"],
            row["cpl_real"],
            row["cpa_real"],
            row["cr_reg"],
            row["cr_ftd"],
            row["date_start"] or "",
            row["date_end"] or "",
        ])

    output.seek(0)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=teambead_grouped.csv"}
    )


@app.get("/export/hierarchy")
def export_hierarchy_csv(
    uploader: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    offer: str = Query(default=""),
    search: str = Query(default="")
):
    raw_data = get_filtered_data(uploader, manager, geo, offer, search)
    rows = aggregate_grouped_rows(raw_data)

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "Level",
        "Geo",
        "Platform",
        "Manager",
        "Offer",
        "Creative",
        "Ad Name",
        "Clicks",
        "Leads",
        "REG",
        "FTD",
        "Spend"
    ])

    for row in rows:
        writer.writerow([
            "Ad",
            row["geo"] or "",
            row["platform"] or "",
            row["manager"] or "",
            row["offer"] or "",
            row["creative"] or "",
            row["ad_name"] or "",
            row["clicks"],
            row["leads"],
            row["reg"],
            row["ftd"],
            row["spend"],
        ])

    output.seek(0)

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=teambead_hierarchy.csv"}
    )


@app.get("/grouped", response_class=HTMLResponse)
def show_grouped_table(
    uploader: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    offer: str = Query(default=""),
    search: str = Query(default=""),
    sort_by: str = Query(default="spend"),
    order: str = Query(default="desc")
):
    data = get_filtered_data(uploader, manager, geo, offer, search)
    all_uploaders, all_managers, all_geos, all_offers = get_filter_options()

    rows = aggregate_grouped_rows(data)

    reverse = order.lower() != "asc"
    allowed_sort_fields = {
        "uploader", "ad_name", "manager", "platform", "geo", "offer",
        "rows_combined", "clicks", "leads", "reg", "ftd",
        "spend", "cpc_real", "cpl_real", "cpa_real",
        "cr_reg", "cr_ftd"
    }

    if sort_by not in allowed_sort_fields:
        sort_by = "spend"

    rows.sort(
        key=lambda x: x.get(sort_by, 0) if x.get(sort_by) is not None else 0,
        reverse=reverse
    )

    totals = aggregate_totals(rows)

    def sort_link(field_name):
        next_order = "asc"
        if sort_by == field_name and order == "asc":
            next_order = "desc"

        return (
            f"/grouped?"
            f"uploader={quote_plus(uploader)}&"
            f"manager={quote_plus(manager)}&"
            f"geo={quote_plus(geo)}&"
            f"offer={quote_plus(offer)}&"
            f"search={quote_plus(search)}&"
            f"sort_by={field_name}&"
            f"order={next_order}"
        )

    export_grouped_link = (
        f"/export/grouped?"
        f"uploader={quote_plus(uploader)}&"
        f"manager={quote_plus(manager)}&"
        f"geo={quote_plus(geo)}&"
        f"offer={quote_plus(offer)}&"
        f"search={quote_plus(search)}"
    )

    uploader_options = make_options(all_uploaders, uploader)
    manager_options = make_options(all_managers, manager)
    geo_options = make_options(all_geos, geo)
    offer_options = make_options(all_offers, offer)

    rows_html = ""
    for row in rows:
        row_class = ""
        if row["ftd"] > 0:
            row_class = "good-row"
        elif row["reg"] > 0 and row["ftd"] == 0:
            row_class = "warn-row"
        elif row["spend"] > 0 and row["reg"] == 0:
            row_class = "bad-row"

        rows_html += f"""
        <tr class="{row_class}">
            <td class="col-uploader">{row['uploader'] or ''}</td>
            <td class="col-ad_name">{row['ad_name'] or ''}</td>
            <td class="col-platform">{row['platform'] or ''}</td>
            <td class="col-manager">{row['manager'] or ''}</td>
            <td class="col-geo">{row['geo'] or ''}</td>
            <td class="col-offer">{row['offer'] or ''}</td>
            <td class="col-creative">{row['creative'] or ''}</td>
            <td class="col-rows">{format_int_or_float(row['rows_combined'])}</td>
            <td class="col-clicks">{format_int_or_float(row['clicks'])}</td>
            <td class="col-leads">{format_int_or_float(row['leads'])}</td>
            <td class="col-reg">{format_int_or_float(row['reg'])}</td>
            <td class="col-ftd">{format_int_or_float(row['ftd'])}</td>
            <td class="col-spend">{format_money(row['spend'])}</td>
            <td class="col-cpc">{format_money(row['cpc_real'])}</td>
            <td class="col-cpl">{format_money(row['cpl_real'])}</td>
            <td class="col-cpa">{format_money(row['cpa_real'])}</td>
            <td class="col-cr_reg">{format_percent(row['cr_reg'])}</td>
            <td class="col-cr_ftd">{format_percent(row['cr_ftd'])}</td>
            <td class="col-date_start">{row['date_start'] or ''}</td>
            <td class="col-date_end">{row['date_end'] or ''}</td>
        </tr>
        """

    content = f"""
    <div class="panel">
        <div class="controls-row">
            <div class="column-panel-title">Экспорт</div>
            <div style="display:flex; gap:10px; flex-wrap:wrap;">
                <a class="small-btn" href="{export_grouped_link}">⬇ Выгрузить CSV</a>
            </div>
        </div>
    </div>

    <div class="panel filters">
        <form method="get" action="/grouped">
            <label>
                Uploader
                <select name="uploader">{uploader_options}</select>
            </label>

            <label>
                Manager
                <select name="manager">{manager_options}</select>
            </label>

            <label>
                Geo
                <select name="geo">{geo_options}</select>
            </label>

            <label>
                Offer
                <select name="offer">{offer_options}</select>
            </label>

            <label>
                Search
                <input type="text" name="search" value="{search}">
            </label>

            <input type="hidden" name="sort_by" value="{sort_by}">
            <input type="hidden" name="order" value="{order}">

            <button type="submit">Фильтровать</button>
            <a href="/grouped">Сбросить</a>
        </form>
    </div>

    <div class="column-panel">
        <div class="controls-row">
            <div class="column-panel-title">Колонки</div>
            <button type="button" class="small-btn" onclick="resetColumnSettings()">Сбросить колонки</button>
        </div>
        <div class="column-list" id="columnToggles">
            <label><input type="checkbox" data-col="col-uploader" checked> Uploader</label>
            <label><input type="checkbox" data-col="col-ad_name" checked> Ad Name</label>
            <label><input type="checkbox" data-col="col-platform" checked> Platform</label>
            <label><input type="checkbox" data-col="col-manager" checked> Manager</label>
            <label><input type="checkbox" data-col="col-geo" checked> Geo</label>
            <label><input type="checkbox" data-col="col-offer" checked> Offer</label>
            <label><input type="checkbox" data-col="col-creative" checked> Creative</label>
            <label><input type="checkbox" data-col="col-rows" checked> Rows</label>
            <label><input type="checkbox" data-col="col-clicks" checked> Clicks</label>
            <label><input type="checkbox" data-col="col-leads" checked> Leads</label>
            <label><input type="checkbox" data-col="col-reg" checked> REG</label>
            <label><input type="checkbox" data-col="col-ftd" checked> FTD</label>
            <label><input type="checkbox" data-col="col-spend" checked> Spend</label>
            <label><input type="checkbox" data-col="col-cpc" checked> Real CPC</label>
            <label><input type="checkbox" data-col="col-cpl" checked> Real CPL</label>
            <label><input type="checkbox" data-col="col-cpa" checked> Real CPA</label>
            <label><input type="checkbox" data-col="col-cr_reg" checked> CR REG</label>
            <label><input type="checkbox" data-col="col-cr_ftd" checked> CR FTD</label>
            <label><input type="checkbox" data-col="col-date_start" checked> Date Start</label>
            <label><input type="checkbox" data-col="col-date_end" checked> Date End</label>
        </div>
        <div class="hint">Можно скрывать колонки, менять их ширину и перетаскивать порядок прямо в таблице.</div>
    </div>

    <div class="panel">
        <div class="stats">
            <div class="card"><div class="card-title">Total Spend</div><div class="card-value">{format_money(totals['spend'])}</div></div>
            <div class="card"><div class="card-title">Total Clicks</div><div class="card-value">{format_int_or_float(totals['clicks'])}</div></div>
            <div class="card"><div class="card-title">Total Leads</div><div class="card-value">{format_int_or_float(totals['leads'])}</div></div>
            <div class="card"><div class="card-title">Total REG</div><div class="card-value">{format_int_or_float(totals['reg'])}</div></div>
            <div class="card"><div class="card-title">Total FTD</div><div class="card-value">{format_int_or_float(totals['ftd'])}</div></div>
            <div class="card"><div class="card-title">Real CPC</div><div class="card-value">{format_money(totals['cpc_real'])}</div></div>
            <div class="card"><div class="card-title">Real CPL</div><div class="card-value">{format_money(totals['cpl_real'])}</div></div>
            <div class="card"><div class="card-title">Real CPA</div><div class="card-value">{format_money(totals['cpa_real'])}</div></div>
            <div class="card"><div class="card-title">CR REG</div><div class="card-value">{format_percent(totals['cr_reg'])}</div></div>
            <div class="card"><div class="card-title">CR FTD</div><div class="card-value">{format_percent(totals['cr_ftd'])}</div></div>
        </div>
    </div>

    <div class="table-wrap">
        <table id="crmTable">
            <thead>
                <tr>
                    <th draggable="true" class="resizable col-uploader"><span class="drag-handle">⋮⋮</span><a href="{sort_link('uploader')}">Uploader</a><div class="resize-handle"></div></th>
                    <th draggable="true" class="resizable col-ad_name"><span class="drag-handle">⋮⋮</span><a href="{sort_link('ad_name')}">Ad Name</a><div class="resize-handle"></div></th>
                    <th draggable="true" class="resizable col-platform"><span class="drag-handle">⋮⋮</span><a href="{sort_link('platform')}">Platform</a><div class="resize-handle"></div></th>
                    <th draggable="true" class="resizable col-manager"><span class="drag-handle">⋮⋮</span><a href="{sort_link('manager')}">Manager</a><div class="resize-handle"></div></th>
                    <th draggable="true" class="resizable col-geo"><span class="drag-handle">⋮⋮</span><a href="{sort_link('geo')}">Geo</a><div class="resize-handle"></div></th>
                    <th draggable="true" class="resizable col-offer"><span class="drag-handle">⋮⋮</span><a href="{sort_link('offer')}">Offer</a><div class="resize-handle"></div></th>
                    <th draggable="true" class="resizable col-creative"><span class="drag-handle">⋮⋮</span>Creative<div class="resize-handle"></div></th>
                    <th draggable="true" class="resizable col-rows"><span class="drag-handle">⋮⋮</span><a href="{sort_link('rows_combined')}">Rows</a><div class="resize-handle"></div></th>
                    <th draggable="true" class="resizable col-clicks"><span class="drag-handle">⋮⋮</span><a href="{sort_link('clicks')}">Clicks</a><div class="resize-handle"></div></th>
                    <th draggable="true" class="resizable col-leads"><span class="drag-handle">⋮⋮</span><a href="{sort_link('leads')}">Leads</a><div class="resize-handle"></div></th>
                    <th draggable="true" class="resizable col-reg"><span class="drag-handle">⋮⋮</span><a href="{sort_link('reg')}">REG</a><div class="resize-handle"></div></th>
                    <th draggable="true" class="resizable col-ftd"><span class="drag-handle">⋮⋮</span><a href="{sort_link('ftd')}">FTD</a><div class="resize-handle"></div></th>
                    <th draggable="true" class="resizable col-spend"><span class="drag-handle">⋮⋮</span><a href="{sort_link('spend')}">Spend</a><div class="resize-handle"></div></th>
                    <th draggable="true" class="resizable col-cpc"><span class="drag-handle">⋮⋮</span><a href="{sort_link('cpc_real')}">Real CPC</a><div class="resize-handle"></div></th>
                    <th draggable="true" class="resizable col-cpl"><span class="drag-handle">⋮⋮</span><a href="{sort_link('cpl_real')}">Real CPL</a><div class="resize-handle"></div></th>
                    <th draggable="true" class="resizable col-cpa"><span class="drag-handle">⋮⋮</span><a href="{sort_link('cpa_real')}">Real CPA</a><div class="resize-handle"></div></th>
                    <th draggable="true" class="resizable col-cr_reg"><span class="drag-handle">⋮⋮</span><a href="{sort_link('cr_reg')}">CR REG</a><div class="resize-handle"></div></th>
                    <th draggable="true" class="resizable col-cr_ftd"><span class="drag-handle">⋮⋮</span><a href="{sort_link('cr_ftd')}">CR FTD</a><div class="resize-handle"></div></th>
                    <th draggable="true" class="resizable col-date_start"><span class="drag-handle">⋮⋮</span>Date Start<div class="resize-handle"></div></th>
                    <th draggable="true" class="resizable col-date_end"><span class="drag-handle">⋮⋮</span>Date End<div class="resize-handle"></div></th>
                </tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
    </div>
    """

    extra_scripts = """
    <script>
        const toggleStorageKey = 'teambead-columns-v2';
        const widthStorageKey = 'teambead-column-widths-v2';
        const orderStorageKey = 'teambead-column-order-v2';

        function resetColumnSettings() {
            localStorage.removeItem(toggleStorageKey);
            localStorage.removeItem(widthStorageKey);
            localStorage.removeItem(orderStorageKey);
            location.reload();
        }

        function applyColumnVisibility() {
            const saved = JSON.parse(localStorage.getItem(toggleStorageKey) || '{}');
            document.querySelectorAll('#columnToggles input[type="checkbox"]').forEach(cb => {
                const colClass = cb.dataset.col;
                if (saved[colClass] !== undefined) {
                    cb.checked = saved[colClass];
                }
                document.querySelectorAll('.' + colClass).forEach(el => {
                    el.style.display = cb.checked ? '' : 'none';
                });
            });
        }

        document.querySelectorAll('#columnToggles input[type="checkbox"]').forEach(cb => {
            cb.addEventListener('change', () => {
                const saved = JSON.parse(localStorage.getItem(toggleStorageKey) || '{}');
                saved[cb.dataset.col] = cb.checked;
                localStorage.setItem(toggleStorageKey, JSON.stringify(saved));
                applyColumnVisibility();
            });
        });

        function loadSavedWidths() {
            const saved = JSON.parse(localStorage.getItem(widthStorageKey) || '{}');
            Object.keys(saved).forEach(colClass => {
                document.querySelectorAll('.' + colClass).forEach(el => {
                    el.style.width = saved[colClass] + 'px';
                    el.style.minWidth = saved[colClass] + 'px';
                    el.style.maxWidth = saved[colClass] + 'px';
                });
            });
        }

        function saveWidth(colClass, width) {
            const saved = JSON.parse(localStorage.getItem(widthStorageKey) || '{}');
            saved[colClass] = width;
            localStorage.setItem(widthStorageKey, JSON.stringify(saved));
        }

        function setupResize() {
            document.querySelectorAll('th.resizable').forEach(th => {
                const handle = th.querySelector('.resize-handle');
                const colClass = Array.from(th.classList).find(cls => cls.startsWith('col-'));
                if (!handle || !colClass) return;

                let startX = 0;
                let startWidth = 0;
                let resizing = false;

                handle.addEventListener('mousedown', (e) => {
                    resizing = true;
                    startX = e.pageX;
                    startWidth = th.offsetWidth;
                    document.body.style.cursor = 'col-resize';
                    e.preventDefault();
                    e.stopPropagation();
                });

                document.addEventListener('mousemove', (e) => {
                    if (!resizing) return;
                    const newWidth = Math.max(90, startWidth + (e.pageX - startX));
                    document.querySelectorAll('.' + colClass).forEach(el => {
                        el.style.width = newWidth + 'px';
                        el.style.minWidth = newWidth + 'px';
                        el.style.maxWidth = newWidth + 'px';
                    });
                });

                document.addEventListener('mouseup', (e) => {
                    if (!resizing) return;
                    resizing = false;
                    document.body.style.cursor = '';
                    const finalWidth = Math.max(90, startWidth + (e.pageX - startX));
                    saveWidth(colClass, finalWidth);
                });
            });
        }

        function saveColumnOrder(order) {
            localStorage.setItem(orderStorageKey, JSON.stringify(order));
        }

        function getCurrentOrder() {
            const headerRow = document.querySelector('#crmTable thead tr');
            return Array.from(headerRow.children).map(th => Array.from(th.classList).find(cls => cls.startsWith('col-')));
        }

        function applySavedOrder() {
            const saved = JSON.parse(localStorage.getItem(orderStorageKey) || 'null');
            if (!saved || !saved.length) return;

            const table = document.getElementById('crmTable');
            const headerRow = table.tHead.rows[0];
            const bodyRows = table.tBodies[0].rows;

            saved.forEach(colClass => {
                const th = headerRow.querySelector('.' + colClass);
                if (th) headerRow.appendChild(th);
            });

            Array.from(bodyRows).forEach(row => {
                saved.forEach(colClass => {
                    const td = row.querySelector('.' + colClass);
                    if (td) row.appendChild(td);
                });
            });
        }

        function setupDragAndDrop() {
            const table = document.getElementById('crmTable');
            const headerRow = table.tHead.rows[0];
            let draggedTh = null;

            headerRow.querySelectorAll('th').forEach(th => {
                th.addEventListener('dragstart', () => {
                    draggedTh = th;
                    th.classList.add('dragging');
                });

                th.addEventListener('dragend', () => {
                    th.classList.remove('dragging');
                    draggedTh = null;
                    saveColumnOrder(getCurrentOrder());
                });

                th.addEventListener('dragover', (e) => {
                    e.preventDefault();
                });

                th.addEventListener('drop', (e) => {
                    e.preventDefault();
                    if (!draggedTh || draggedTh === th) return;

                    const headers = Array.from(headerRow.children);
                    const fromIndex = headers.indexOf(draggedTh);
                    const toIndex = headers.indexOf(th);

                    if (fromIndex < toIndex) {
                        th.after(draggedTh);
                    } else {
                        th.before(draggedTh);
                    }

                    Array.from(table.tBodies[0].rows).forEach(row => {
                        const draggedClass = Array.from(draggedTh.classList).find(c => c.startsWith('col-'));
                        const targetClass = Array.from(th.classList).find(c => c.startsWith('col-'));

                        const draggedTd = row.querySelector('.' + draggedClass);
                        const targetTd = row.querySelector('.' + targetClass);

                        if (!draggedTd || !targetTd) return;

                        if (fromIndex < toIndex) {
                            targetTd.after(draggedTd);
                        } else {
                            targetTd.before(draggedTd);
                        }
                    });

                    saveColumnOrder(getCurrentOrder());
                });
            });
        }

        applySavedOrder();
        applyColumnVisibility();
        loadSavedWidths();
        setupResize();
        setupDragAndDrop();
    </script>
    """

    return page_shell("FB — Сводка", content, "grouped", extra_scripts)


@app.get("/hierarchy", response_class=HTMLResponse)
def show_hierarchy(
    uploader: str = Query(default=""),
    manager: str = Query(default=""),
    geo: str = Query(default=""),
    offer: str = Query(default=""),
    search: str = Query(default="")
):
    raw_data = get_filtered_data(uploader, manager, geo, offer, search)
    all_uploaders, all_managers, all_geos, all_offers = get_filter_options()

    rows = aggregate_grouped_rows(raw_data)

    tree = {}

    for row in rows:
        geo_key = row["geo"] or "Без GEO"
        platform_key = row["platform"] or "Без Platform"
        manager_key = row["manager"] or "Без Manager"
        offer_key = row["offer"] or "Без Offer"
        creative_key = row["creative"] or "Без Creative"
        ad_key = row["ad_name"] or "Ad"

        tree.setdefault(geo_key, {"metrics": {"clicks": 0, "leads": 0, "reg": 0, "ftd": 0, "spend": 0}, "children": {}})
        geo_node = tree[geo_key]
        geo_node["metrics"]["clicks"] += row["clicks"]
        geo_node["metrics"]["leads"] += row["leads"]
        geo_node["metrics"]["reg"] += row["reg"]
        geo_node["metrics"]["ftd"] += row["ftd"]
        geo_node["metrics"]["spend"] += row["spend"]

        geo_node["children"].setdefault(platform_key, {"metrics": {"clicks": 0, "leads": 0, "reg": 0, "ftd": 0, "spend": 0}, "children": {}})
        platform_node = geo_node["children"][platform_key]
        platform_node["metrics"]["clicks"] += row["clicks"]
        platform_node["metrics"]["leads"] += row["leads"]
        platform_node["metrics"]["reg"] += row["reg"]
        platform_node["metrics"]["ftd"] += row["ftd"]
        platform_node["metrics"]["spend"] += row["spend"]

        platform_node["children"].setdefault(manager_key, {"metrics": {"clicks": 0, "leads": 0, "reg": 0, "ftd": 0, "spend": 0}, "children": {}})
        manager_node = platform_node["children"][manager_key]
        manager_node["metrics"]["clicks"] += row["clicks"]
        manager_node["metrics"]["leads"] += row["leads"]
        manager_node["metrics"]["reg"] += row["reg"]
        manager_node["metrics"]["ftd"] += row["ftd"]
        manager_node["metrics"]["spend"] += row["spend"]

        manager_node["children"].setdefault(offer_key, {"metrics": {"clicks": 0, "leads": 0, "reg": 0, "ftd": 0, "spend": 0}, "children": {}})
        offer_node = manager_node["children"][offer_key]
        offer_node["metrics"]["clicks"] += row["clicks"]
        offer_node["metrics"]["leads"] += row["leads"]
        offer_node["metrics"]["reg"] += row["reg"]
        offer_node["metrics"]["ftd"] += row["ftd"]
        offer_node["metrics"]["spend"] += row["spend"]

        offer_node["children"].setdefault(creative_key, {"metrics": {"clicks": 0, "leads": 0, "reg": 0, "ftd": 0, "spend": 0}, "children": {}})
        creative_node = offer_node["children"][creative_key]
        creative_node["metrics"]["clicks"] += row["clicks"]
        creative_node["metrics"]["leads"] += row["leads"]
        creative_node["metrics"]["reg"] += row["reg"]
        creative_node["metrics"]["ftd"] += row["ftd"]
        creative_node["metrics"]["spend"] += row["spend"]

        creative_node["children"][ad_key] = row

    export_hierarchy_link = (
        f"/export/hierarchy?"
        f"uploader={quote_plus(uploader)}&"
        f"manager={quote_plus(manager)}&"
        f"geo={quote_plus(geo)}&"
        f"offer={quote_plus(offer)}&"
        f"search={quote_plus(search)}"
    )

    uploader_options = make_options(all_uploaders, uploader)
    manager_options = make_options(all_managers, manager)
    geo_options = make_options(all_geos, geo)
    offer_options = make_options(all_offers, offer)

    def metrics_line(name, clicks, leads, reg, ftd, spend):
        m = calc_metrics(clicks, reg, ftd, spend)
        return f"""
        <div class="tree-line">
            <div class="tree-name">{name}</div>
            <div>Clicks: {format_int_or_float(clicks)}</div>
            <div>Leads: {format_int_or_float(leads)}</div>
            <div>REG: {format_int_or_float(reg)}</div>
            <div>FTD: {format_int_or_float(ftd)}</div>
            <div>Spend: {format_money(spend)}</div>
            <div>CPL: {format_money(m['cpl_real'])}</div>
            <div>CPA: {format_money(m['cpa_real'])}</div>
        </div>
        """

    tree_html = """
    <div class="tree-root">
        <div class="tree-line tree-header">
            <div>Уровень</div>
            <div>Clicks</div>
            <div>Leads</div>
            <div>REG</div>
            <div>FTD</div>
            <div>Spend</div>
            <div>CPL</div>
            <div>CPA</div>
        </div>
    """

    for geo_name, geo_node in tree.items():
        gm = geo_node["metrics"]
        tree_html += f"""
        <details class="tree-level-1 hierarchy-node" data-node-id="geo::{geo_name}" open>
            <summary>
                <span>GEO: {geo_name}</span>
                <span>Spend {format_money(gm['spend'])} | REG {format_int_or_float(gm['reg'])} | FTD {format_int_or_float(gm['ftd'])}</span>
            </summary>
            {metrics_line(geo_name, gm['clicks'], gm['leads'], gm['reg'], gm['ftd'], gm['spend'])}
        """

        for platform_name, platform_node in geo_node["children"].items():
            pm = platform_node["metrics"]
            tree_html += f"""
            <details class="tree-level-2 hierarchy-node" data-node-id="geo::{geo_name}|platform::{platform_name}" open>
                <summary>
                    <span>Platform: {platform_name}</span>
                    <span>Spend {format_money(pm['spend'])} | REG {format_int_or_float(pm['reg'])} | FTD {format_int_or_float(pm['ftd'])}</span>
                </summary>
                {metrics_line(platform_name, pm['clicks'], pm['leads'], pm['reg'], pm['ftd'], pm['spend'])}
            """

            for manager_name, manager_node in platform_node["children"].items():
                mm = manager_node["metrics"]
                tree_html += f"""
                <details class="tree-level-3 hierarchy-node" data-node-id="geo::{geo_name}|platform::{platform_name}|manager::{manager_name}" open>
                    <summary>
                        <span>Manager: {manager_name}</span>
                        <span>Spend {format_money(mm['spend'])} | REG {format_int_or_float(mm['reg'])} | FTD {format_int_or_float(mm['ftd'])}</span>
                    </summary>
                    {metrics_line(manager_name, mm['clicks'], mm['leads'], mm['reg'], mm['ftd'], mm['spend'])}
                """

                for offer_name, offer_node in manager_node["children"].items():
                    om = offer_node["metrics"]
                    tree_html += f"""
                    <details class="tree-level-4 hierarchy-node" data-node-id="geo::{geo_name}|platform::{platform_name}|manager::{manager_name}|offer::{offer_name}" open>
                        <summary>
                            <span>Offer: {offer_name}</span>
                            <span>Spend {format_money(om['spend'])} | REG {format_int_or_float(om['reg'])} | FTD {format_int_or_float(om['ftd'])}</span>
                        </summary>
                        {metrics_line(offer_name, om['clicks'], om['leads'], om['reg'], om['ftd'], om['spend'])}
                    """

                    for creative_name, creative_node in offer_node["children"].items():
                        cm = creative_node["metrics"]
                        tree_html += f"""
                        <details class="tree-level-5 hierarchy-node" data-node-id="geo::{geo_name}|platform::{platform_name}|manager::{manager_name}|offer::{offer_name}|creative::{creative_name}">
                            <summary>
                                <span>Creative: {creative_name}</span>
                                <span>Spend {format_money(cm['spend'])} | REG {format_int_or_float(cm['reg'])} | FTD {format_int_or_float(cm['ftd'])}</span>
                            </summary>
                            {metrics_line(creative_name, cm['clicks'], cm['leads'], cm['reg'], cm['ftd'], cm['spend'])}
                        """

                        for ad_name, ad_row in creative_node["children"].items():
                            tree_html += f"""
                            <div class="tree-level-6">
                                {metrics_line(
                                    ad_name,
                                    ad_row['clicks'],
                                    ad_row['leads'],
                                    ad_row['reg'],
                                    ad_row['ftd'],
                                    ad_row['spend']
                                )}
                            </div>
                            """

                        tree_html += "</details>"

                    tree_html += "</details>"
                tree_html += "</details>"
            tree_html += "</details>"
        tree_html += "</details>"

    tree_html += "</div>"

    content = f"""
    <div class="panel">
        <div class="controls-row">
            <div class="column-panel-title">Экспорт</div>
            <div style="display:flex; gap:10px; flex-wrap:wrap;">
                <a class="small-btn" href="{export_hierarchy_link}">⬇ Выгрузить CSV</a>
            </div>
        </div>
    </div>

    <div class="panel filters">
        <form method="get" action="/hierarchy">
            <label>
                Uploader
                <select name="uploader">{uploader_options}</select>
            </label>

            <label>
                Manager
                <select name="manager">{manager_options}</select>
            </label>

            <label>
                Geo
                <select name="geo">{geo_options}</select>
            </label>

            <label>
                Offer
                <select name="offer">{offer_options}</select>
            </label>

            <label>
                Search
                <input type="text" name="search" value="{search}">
            </label>

            <button type="submit">Фильтровать</button>
            <a href="/hierarchy">Сбросить</a>
        </form>
        <div class="hint">Логика: GEO → Platform → Manager → Offer → Creative → объявления. Считает из тех же сгруппированных строк, что и Сводка.</div>
    </div>

    {tree_html}
    """

    extra_scripts = """
    <script>
        const hierarchyStorageKey = 'teambead-hierarchy-state-v1';

        function loadHierarchyState() {
            const saved = JSON.parse(localStorage.getItem(hierarchyStorageKey) || '{}');
            document.querySelectorAll('.hierarchy-node').forEach(node => {
                const id = node.dataset.nodeId;
                if (!id) return;

                if (saved[id] === false) {
                    node.removeAttribute('open');
                } else if (saved[id] === true) {
                    node.setAttribute('open', 'open');
                }
            });
        }

        function bindHierarchyState() {
            document.querySelectorAll('.hierarchy-node').forEach(node => {
                node.addEventListener('toggle', () => {
                    const saved = JSON.parse(localStorage.getItem(hierarchyStorageKey) || '{}');
                    saved[node.dataset.nodeId] = node.open;
                    localStorage.setItem(hierarchyStorageKey, JSON.stringify(saved));
                });
            });
        }

        loadHierarchyState();
        bindHierarchyState();
    </script>
    """

    return page_shell("FB — Иерархия", content, "hierarchy", extra_scripts)
