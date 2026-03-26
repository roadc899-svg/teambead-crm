from fastapi import FastAPI, UploadFile, File, Query, Form
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker, declarative_base
import pandas as pd
import shutil
import uuid
import os
import io
import csv
from html import escape

DATABASE_URL = "sqlite:///./test.db"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
)
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

app = FastAPI(title="TEAMbead CRM")


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
            value = value.replace(" ", "").replace(",", ".").replace("$", "")
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
            "launch_date": None,
            "platform": None,
            "manager": None,
            "geo": None,
            "offer": None,
            "creative": None,
        }

    parts = str(ad_name).split("/")

    return {
        "launch_date": parts[0].strip() if len(parts) > 0 else None,
        "platform": parts[1].strip() if len(parts) > 1 else None,
        "manager": parts[2].strip() if len(parts) > 2 else None,
        "geo": parts[3].strip() if len(parts) > 3 else None,
        "offer": parts[4].strip() if len(parts) > 4 else None,
        "creative": parts[5].strip() if len(parts) > 5 else None,
    }


def calc_metrics(clicks, reg, ftd, spend):
    clicks = float(clicks or 0)
    reg = float(reg or 0)
    ftd = float(ftd or 0)
    spend = float(spend or 0)
    leads = 0.0

    cpc_real = spend / clicks if clicks > 0 else 0
    cpl_real = spend / leads if leads > 0 else 0
    cpa_real = spend / ftd if ftd > 0 else 0
    cr_reg = (reg / clicks * 100) if clicks > 0 else 0
    cr_ftd = (ftd / reg * 100) if reg > 0 else 0

    return {
        "cpc_real": cpc_real,
        "cpl_real": cpl_real,
        "cpa_real": cpa_real,
        "cr_reg": cr_reg,
        "cr_ftd": cr_ftd,
    }


def get_all_rows():
    db = SessionLocal()
    try:
        return db.query(FBRow).all()
    finally:
        db.close()


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
        html += f'<option value="{escape(option)}" {selected}>{escape(option)}</option>'
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
        metrics = calc_metrics(item["clicks"], item["reg"], item["ftd"], item["spend"])
        item.update(metrics)
        item["cpl_real"] = item["spend"] / item["leads"] if item["leads"] > 0 else 0

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
        "cpl_real": (total_spend / total_leads) if total_leads > 0 else 0,
        **metrics,
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
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
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

            * {{
                box-sizing: border-box;
            }}

            body {{
                margin: 0;
                font-family: Arial, sans-serif;
                background: var(--bg);
                color: var(--text);
            }}

            .app {{
                display: flex;
                min-height: 100vh;
            }}

            .sidebar {{
                width: 240px;
                background: linear-gradient(180deg, #0f172a 0%, #1e293b 100%);
                color: #fff;
                padding: 20px 14px;
                box-shadow: var(--shadow);
            }}

            .sidebar-brand {{
                font-size: 24px;
                font-weight: 800;
                margin-bottom: 20px;
                background: linear-gradient(90deg, #60a5fa, #22d3ee, #4ade80);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
            }}

            .sidebar-group summary {{
                cursor: pointer;
                font-weight: 700;
                padding: 10px 12px;
                border-radius: 12px;
                background: rgba(255,255,255,0.06);
                margin-bottom: 10px;
            }}

            .sidebar-links {{
                display: flex;
                flex-direction: column;
                gap: 8px;
                margin-top: 10px;
            }}

            .sidebar-links a {{
                color: #cbd5e1;
                text-decoration: none;
                padding: 10px 12px;
                border-radius: 10px;
                transition: 0.2s;
            }}

            .sidebar-links a:hover,
            .sidebar-links a.active-link {{
                background: rgba(96, 165, 250, 0.18);
                color: #fff;
            }}

            .main {{
                flex: 1;
                padding: 24px;
            }}

            .card {{
                background: var(--panel);
                border: 1px solid var(--border);
                border-radius: 18px;
                box-shadow: var(--shadow);
                padding: 18px;
                margin-bottom: 18px;
            }}

            h1 {{
                margin-top: 0;
                margin-bottom: 8px;
                font-size: 28px;
            }}

            .muted {{
                color: var(--muted);
            }}

            .filters {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                gap: 12px;
                margin-top: 16px;
            }}

            .filters input,
            .filters select,
            .filters button {{
                width: 100%;
                padding: 10px 12px;
                border-radius: 12px;
                border: 1px solid var(--border);
                background: var(--panel-2);
                font-size: 14px;
            }}

            .filters button {{
                background: linear-gradient(90deg, var(--accent1), var(--accent2));
                color: white;
                border: none;
                cursor: pointer;
                font-weight: 700;
            }}

            .upload-form {{
                display: flex;
                flex-wrap: wrap;
                gap: 12px;
                align-items: end;
            }}

            .upload-form .field {{
                flex: 1;
                min-width: 180px;
            }}

            .upload-form label {{
                display: block;
                font-size: 13px;
                color: var(--muted);
                margin-bottom: 6px;
            }}

            .upload-form input,
            .upload-form button {{
                width: 100%;
                padding: 10px 12px;
                border-radius: 12px;
                border: 1px solid var(--border);
            }}

            .upload-form button {{
                background: linear-gradient(90deg, var(--accent3), #16a34a);
                color: white;
                border: none;
                cursor: pointer;
                font-weight: 700;
            }}

            .table-wrap {{
                overflow: auto;
                border-radius: 16px;
                border: 1px solid var(--border);
            }}

            table {{
                width: 100%;
                border-collapse: collapse;
                background: var(--panel);
            }}

            th, td {{
                padding: 12px 10px;
                border-bottom: 1px solid var(--border);
                text-align: left;
                white-space: nowrap;
                vertical-align: top;
            }}

            th {{
                background: var(--table-head);
                color: var(--table-head-text);
                position: sticky;
                top: 0;
                z-index: 1;
            }}

            tbody tr:nth-child(even) {{
                background: var(--row-even);
            }}

            .totals {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
                gap: 12px;
                margin-top: 14px;
            }}

            .metric {{
                background: var(--panel-2);
                border: 1px solid var(--border);
                border-radius: 14px;
                padding: 12px;
            }}

            .metric .label {{
                font-size: 12px;
                color: var(--muted);
                margin-bottom: 6px;
            }}

            .metric .value {{
                font-size: 20px;
                font-weight: 700;
            }}

            .tree details {{
                margin-bottom: 10px;
                border: 1px solid var(--border);
                border-radius: 12px;
                overflow: hidden;
                background: white;
            }}

            .tree summary {{
                cursor: pointer;
                list-style: none;
                padding: 12px 14px;
                font-weight: 700;
            }}

            .tree summary::-webkit-details-marker {{
                display: none;
            }}

            .lvl1 > summary {{ background: var(--lvl1); }}
            .lvl2 > summary {{ background: var(--lvl2); }}
            .lvl3 > summary {{ background: var(--lvl3); }}
            .lvl4 > summary {{ background: var(--lvl4); }}
            .lvl5 > summary {{ background: var(--lvl5); }}
            .lvl6 > summary {{ background: var(--lvl6); }}

            .node-content {{
                padding: 10px 14px 14px;
            }}

            .node-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
                gap: 8px;
                margin-top: 10px;
            }}

            .pill {{
                display: inline-block;
                padding: 4px 8px;
                border-radius: 999px;
                background: var(--panel-2);
                border: 1px solid var(--border);
                font-size: 12px;
                color: var(--muted);
            }}

            @media (max-width: 900px) {{
                .app {{
                    flex-direction: column;
                }}
                .sidebar {{
                    width: 100%;
                }}
                .main {{
                    padding: 14px;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="app">
            {sidebar}
            <main class="main">
                {content}
            </main>
        </div>
        {extra_scripts}
    </body>
    </html>
    """


def build_query_string(uploader="", manager="", geo="", offer="", search="", sort_by="", order=""):
    params = []
    if uploader:
        params.append(f"uploader={uploader}")
    if manager:
        params.append(f"manager={manager}")
    if geo:
        params.append(f"geo={geo}")
    if offer:
        params.append(f"offer={offer}")
    if search:
        params.append(f"search={search}")
    if sort_by:
        params.append(f"sort_by={sort_by}")
    if order:
        params.append(f"order={order}")
    return "&".join(params)


@app.post("/upload")
async def upload_file(
    uploader: str = Form(...),
    file: UploadFile = File(...)
):
    original_name = file.filename or ""
    ext = os.path.splitext(original_name)[1].lower()
    temp_name = f"temp_{uuid.uuid4()}{ext or '.csv'}"

    try:
        with open(temp_name, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        if ext in [".xlsx", ".xls"]:
            df = pd.read_excel(temp_name)
        else:
            try:
                df = pd.read_csv(temp_name)
            except Exception:
                df = pd.read_csv(temp_name, sep=";")

        db = SessionLocal()
        try:
            db.query(FBRow).filter(FBRow.uploader == uploader).delete()
            db.commit()

            for _, row in df.iterrows():
                parsed = parse_ad_name(row.get("Название объявления"))

                item = FBRow(
                    uploader=uploader,
                    ad_name=str(row.get("Название объявления") or ""),
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
        finally:
            db.close()

        return RedirectResponse(url="/grouped", status_code=303)

    finally:
        if os.path.exists(temp_name):
            os.remove(temp_name)


@app.get("/export/grouped")
def export_grouped_csv(
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
        "Uploader", "Ad Name", "Platform", "Manager", "Geo", "Offer", "Creative",
        "Rows Combined", "Clicks", "Leads", "REG", "FTD", "Spend",
        "CPC", "CPL", "CPA", "CR REG", "CR FTD", "Date Start", "Date End"
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
            format_money(row["spend"]),
            format_money(row["cpc_real"]),
            format_money(row["cpl_real"]),
            format_money(row["cpa_real"]),
            format_percent(row["cr_reg"]),
            format_percent(row["cr_ftd"]),
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
            format_int_or_float(row["clicks"]),
            format_int_or_float(row["leads"]),
            format_int_or_float(row["reg"]),
            format_int_or_float(row["ftd"]),
            format_money(row["spend"]),
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
        "spend", "cpc_real", "cpl_real", "cpa_real", "cr_reg", "cr_ftd"
    }

    if sort_by not in allowed_sort_fields:
        sort_by = "spend"

    rows.sort(
        key=lambda x: x.get(sort_by, 0) if x.get(sort_by) is not None else 0,
        reverse=reverse
    )

    totals = aggregate_totals(rows)
    qs = build_query_string(uploader, manager, geo, offer, search, sort_by, order)
    export_url = f"/export/grouped?{qs}" if qs else "/export/grouped"

    rows_html = ""
    for row in rows:
        rows_html += f"""
        <tr>
            <td>{escape(row["uploader"] or "")}</td>
            <td>{escape(row["ad_name"] or "")}</td>
            <td>{escape(row["platform"] or "")}</td>
            <td>{escape(row["manager"] or "")}</td>
            <td>{escape(row["geo"] or "")}</td>
            <td>{escape(row["offer"] or "")}</td>
            <td>{escape(row["creative"] or "")}</td>
            <td>{format_int_or_float(row["rows_combined"])}</td>
            <td>{format_int_or_float(row["clicks"])}</td>
            <td>{format_int_or_float(row["leads"])}</td>
            <td>{format_int_or_float(row["reg"])}</td>
            <td>{format_int_or_float(row["ftd"])}</td>
            <td>{format_money(row["spend"])}</td>
            <td>{format_money(row["cpc_real"])}</td>
            <td>{format_money(row["cpl_real"])}</td>
            <td>{format_money(row["cpa_real"])}</td>
            <td>{format_percent(row["cr_reg"])}</td>
            <td>{format_percent(row["cr_ftd"])}</td>
            <td>{escape(row["date_start"] or "")}</td>
            <td>{escape(row["date_end"] or "")}</td>
        </tr>
        """

    content = f"""
    <div class="card">
        <h1>FB — Сводка</h1>
        <div class="muted">Загрузка, фильтрация и общая сводка по объявлениям</div>
    </div>

    <div class="card">
        <form class="upload-form" action="/upload" method="post" enctype="multipart/form-data">
            <div class="field">
                <label>Кто загружает</label>
                <input type="text" name="uploader" placeholder="Например: Road / Dima / Buyer 1" required>
            </div>
            <div class="field">
                <label>Файл CSV или Excel</label>
                <input type="file" name="file" required>
            </div>
            <div class="field" style="max-width:220px;">
                <label>&nbsp;</label>
                <button type="submit">Загрузить</button>
            </div>
        </form>
    </div>

    <div class="card">
        <form method="get" action="/grouped">
            <div class="filters">
                <select name="uploader">{make_options(all_uploaders, uploader)}</select>
                <select name="manager">{make_options(all_managers, manager)}</select>
                <select name="geo">{make_options(all_geos, geo)}</select>
                <select name="offer">{make_options(all_offers, offer)}</select>
                <input type="text" name="search" value="{escape(search)}" placeholder="Поиск по названию объявления">
                <select name="sort_by">
                    {make_options(sorted(list(allowed_sort_fields)), sort_by)}
                </select>
                <select name="order">
                    <option value="desc" {"selected" if order=="desc" else ""}>DESC</option>
                    <option value="asc" {"selected" if order=="asc" else ""}>ASC</option>
                </select>
                <button type="submit">Применить</button>
            </div>
        </form>
    </div>

    <div class="card">
        <a class="pill" href="{export_url}">Экспорт CSV</a>

        <div class="totals">
            <div class="metric"><div class="label">Клики</div><div class="value">{format_int_or_float(totals["clicks"])}</div></div>
            <div class="metric"><div class="label">Лиды</div><div class="value">{format_int_or_float(totals["leads"])}</div></div>
            <div class="metric"><div class="label">REG</div><div class="value">{format_int_or_float(totals["reg"])}</div></div>
            <div class="metric"><div class="label">FTD</div><div class="value">{format_int_or_float(totals["ftd"])}</div></div>
            <div class="metric"><div class="label">Spend</div><div class="value">{format_money(totals["spend"])}</div></div>
            <div class="metric"><div class="label">CPC</div><div class="value">{format_money(totals["cpc_real"])}</div></div>
            <div class="metric"><div class="label">CPL</div><div class="value">{format_money(totals["cpl_real"])}</div></div>
            <div class="metric"><div class="label">CPA</div><div class="value">{format_money(totals["cpa_real"])}</div></div>
            <div class="metric"><div class="label">CR REG</div><div class="value">{format_percent(totals["cr_reg"])}</div></div>
            <div class="metric"><div class="label">CR FTD</div><div class="value">{format_percent(totals["cr_ftd"])}</div></div>
        </div>
    </div>

    <div class="card">
        <div class="table-wrap">
            <table>
                <thead>
                    <tr>
                        <th>Uploader</th>
                        <th>Ad Name</th>
                        <th>Platform</th>
                        <th>Manager</th>
                        <th>Geo</th>
                        <th>Offer</th>
                        <th>Creative</th>
                        <th>Rows</th>
                        <th>Clicks</th>
                        <th>Leads</th>
                        <th>REG</th>
                        <th>FTD</th>
                        <th>Spend</th>
                        <th>CPC</th>
                        <th>CPL</th>
                        <th>CPA</th>
                        <th>CR REG</th>
                        <th>CR FTD</th>
                        <th>Date Start</th>
                        <th>Date End</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html if rows_html else '<tr><td colspan="20">Нет данных</td></tr>'}
                </tbody>
            </table>
        </div>
    </div>
    """

    return page_shell("FB — Сводка", content, "grouped")


def render_node(title, metrics, level_class, children_html=""):
    return f"""
    <details class="{level_class}" open>
        <summary>{escape(title)}</summary>
        <div class="node-content">
            <div class="node-grid">
                <div class="metric"><div class="label">Клики</div><div class="value">{format_int_or_float(metrics["clicks"])}</div></div>
                <div class="metric"><div class="label">Лиды</div><div class="value">{format_int_or_float(metrics["leads"])}</div></div>
                <div class="metric"><div class="label">REG</div><div class="value">{format_int_or_float(metrics["reg"])}</div></div>
                <div class="metric"><div class="label">FTD</div><div class="value">{format_int_or_float(metrics["ftd"])}</div></div>
                <div class="metric"><div class="label">Spend</div><div class="value">{format_money(metrics["spend"])}</div></div>
            </div>
            {children_html}
        </div>
    </details>
    """


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

        creative_node["children"][ad_key] = {
            "metrics": {
                "clicks": row["clicks"],
                "leads": row["leads"],
                "reg": row["reg"],
                "ftd": row["ftd"],
                "spend": row["spend"],
            }
        }

    hierarchy_html = ""

    for geo_name, geo_node in tree.items():
        platform_html = ""
        for platform_name, platform_node in geo_node["children"].items():
            manager_html = ""
            for manager_name, manager_node in platform_node["children"].items():
                offer_html = ""
                for offer_name, offer_node in manager_node["children"].items():
                    creative_html = ""
                    for creative_name, creative_node in offer_node["children"].items():
                        ad_html = ""
                        for ad_name, ad_node in creative_node["children"].items():
                            ad_html += render_node(ad_name, ad_node["metrics"], "lvl6")
                        creative_html += render_node(creative_name, creative_node["metrics"], "lvl5", ad_html)
                    offer_html += render_node(offer_name, offer_node["metrics"], "lvl4", creative_html)
                manager_html += render_node(manager_name, manager_node["metrics"], "lvl3", offer_html)
            platform_html += render_node(platform_name, platform_node["metrics"], "lvl2", manager_html)
        hierarchy_html += render_node(geo_name, geo_node["metrics"], "lvl1", platform_html)

    qs = build_query_string(uploader, manager, geo, offer, search)
    export_url = f"/export/hierarchy?{qs}" if qs else "/export/hierarchy"

    content = f"""
    <div class="card">
        <h1>FB — Иерархия</h1>
        <div class="muted">GEO → Platform → Manager → Offer → Creative → Ad</div>
    </div>

    <div class="card">
        <form method="get" action="/hierarchy">
            <div class="filters">
                <select name="uploader">{make_options(all_uploaders, uploader)}</select>
                <select name="manager">{make_options(all_managers, manager)}</select>
                <select name="geo">{make_options(all_geos, geo)}</select>
                <select name="offer">{make_options(all_offers, offer)}</select>
                <input type="text" name="search" value="{escape(search)}" placeholder="Поиск по названию объявления">
                <button type="submit">Применить</button>
            </div>
        </form>
    </div>

    <div class="card">
        <a class="pill" href="{export_url}">Экспорт CSV</a>
    </div>

    <div class="tree">
        {hierarchy_html if hierarchy_html else '<div class="card">Нет данных</div>'}
    </div>
    """

    return page_shell("FB — Иерархия", content, "hierarchy")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
