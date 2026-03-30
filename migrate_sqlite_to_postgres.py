import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import app


SOURCE_URL = os.getenv("SOURCE_DATABASE_URL", "sqlite:///./test.db").strip()
TARGET_URL = os.getenv("DATABASE_URL", "").strip()


def normalize_url(url: str) -> str:
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


def make_engine(url: str):
    url = normalize_url(url)
    kwargs = {"pool_pre_ping": True}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    return create_engine(url, **kwargs)


def ensure_sqlite_column(conn, table_name: str, columns: list[str], column_name: str, ddl: str):
    if column_name not in columns:
        conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl}"))


def prepare_source_schema(source_engine, source_url: str):
    app.Base.metadata.create_all(bind=source_engine)
    if not normalize_url(source_url).startswith("sqlite"):
        return

    with source_engine.begin() as conn:
        cabinet_columns = [row[1] for row in conn.execute(text("PRAGMA table_info(cabinet_rows)")).fetchall()]
        ensure_sqlite_column(conn, "cabinet_rows", cabinet_columns, "advertiser", "VARCHAR DEFAULT ''")
        ensure_sqlite_column(conn, "cabinet_rows", cabinet_columns, "platform", "VARCHAR DEFAULT ''")
        ensure_sqlite_column(conn, "cabinet_rows", cabinet_columns, "name", "VARCHAR DEFAULT ''")
        ensure_sqlite_column(conn, "cabinet_rows", cabinet_columns, "geo_list", "VARCHAR DEFAULT ''")
        ensure_sqlite_column(conn, "cabinet_rows", cabinet_columns, "brands", "VARCHAR DEFAULT ''")
        ensure_sqlite_column(conn, "cabinet_rows", cabinet_columns, "team_name", "VARCHAR DEFAULT ''")
        ensure_sqlite_column(conn, "cabinet_rows", cabinet_columns, "manager_name", "VARCHAR DEFAULT ''")
        ensure_sqlite_column(conn, "cabinet_rows", cabinet_columns, "manager_contact", "VARCHAR DEFAULT ''")
        ensure_sqlite_column(conn, "cabinet_rows", cabinet_columns, "wallet", "VARCHAR DEFAULT ''")
        ensure_sqlite_column(conn, "cabinet_rows", cabinet_columns, "comments", "VARCHAR DEFAULT ''")
        ensure_sqlite_column(conn, "cabinet_rows", cabinet_columns, "status", "VARCHAR DEFAULT 'Active'")
        if "cabinet_name" in cabinet_columns:
            conn.execute(text("UPDATE cabinet_rows SET name = cabinet_name WHERE COALESCE(name, '') = ''"))
        if "wallets" in cabinet_columns:
            conn.execute(text("UPDATE cabinet_rows SET wallet = wallets WHERE COALESCE(wallet, '') = ''"))
        if "is_active" in cabinet_columns:
            conn.execute(text("UPDATE cabinet_rows SET status = CASE WHEN is_active = 1 THEN 'Active' ELSE 'Archived' END WHERE COALESCE(status, '') = ''"))

        partner_columns = [row[1] for row in conn.execute(text("PRAGMA table_info(partner_rows)")).fetchall()]
        ensure_sqlite_column(conn, "partner_rows", partner_columns, "cabinet_name", "VARCHAR DEFAULT ''")
        ensure_sqlite_column(conn, "partner_rows", partner_columns, "report_date", "VARCHAR DEFAULT ''")
        ensure_sqlite_column(conn, "partner_rows", partner_columns, "period_start", "VARCHAR DEFAULT ''")
        ensure_sqlite_column(conn, "partner_rows", partner_columns, "period_end", "VARCHAR DEFAULT ''")
        ensure_sqlite_column(conn, "partner_rows", partner_columns, "period_label", "VARCHAR DEFAULT ''")
        ensure_sqlite_column(conn, "partner_rows", partner_columns, "manual_hold", "INTEGER DEFAULT 0")
        ensure_sqlite_column(conn, "partner_rows", partner_columns, "manual_blocked", "INTEGER DEFAULT 0")

        chatterfy_columns = [row[1] for row in conn.execute(text("PRAGMA table_info(chatterfy_rows)")).fetchall()]
        ensure_sqlite_column(conn, "chatterfy_rows", chatterfy_columns, "report_date", "VARCHAR DEFAULT ''")
        ensure_sqlite_column(conn, "chatterfy_rows", chatterfy_columns, "period_start", "VARCHAR DEFAULT ''")
        ensure_sqlite_column(conn, "chatterfy_rows", chatterfy_columns, "period_end", "VARCHAR DEFAULT ''")
        ensure_sqlite_column(conn, "chatterfy_rows", chatterfy_columns, "period_label", "VARCHAR DEFAULT ''")

        expense_columns = [row[1] for row in conn.execute(text("PRAGMA table_info(finance_expense_rows)")).fetchall()]
        ensure_sqlite_column(conn, "finance_expense_rows", expense_columns, "wallet_name", "VARCHAR DEFAULT ''")
        ensure_sqlite_column(conn, "finance_expense_rows", expense_columns, "from_wallet", "VARCHAR DEFAULT ''")
        if "paid_by" in expense_columns:
            conn.execute(text("UPDATE finance_expense_rows SET from_wallet = paid_by WHERE COALESCE(from_wallet, '') = ''"))

        income_columns = [row[1] for row in conn.execute(text("PRAGMA table_info(finance_income_rows)")).fetchall()]
        ensure_sqlite_column(conn, "finance_income_rows", income_columns, "wallet_name", "VARCHAR DEFAULT ''")
        ensure_sqlite_column(conn, "finance_income_rows", income_columns, "from_wallet", "VARCHAR DEFAULT ''")
        ensure_sqlite_column(conn, "finance_income_rows", income_columns, "comment", "VARCHAR DEFAULT ''")
        if "wallet" in income_columns:
            conn.execute(text("UPDATE finance_income_rows SET wallet_name = wallet WHERE COALESCE(wallet_name, '') = ''"))


def clone_row(model, row):
    values = {}
    for column in model.__table__.columns:
        values[column.name] = getattr(row, column.name)
    return model(**values)


def migrate_table(source_session, target_session, model):
    rows = source_session.query(model).all()
    target_session.query(model).delete()
    target_session.commit()
    for row in rows:
        target_session.add(clone_row(model, row))
    target_session.commit()
    return len(rows)


def main():
    if not TARGET_URL:
        raise SystemExit("DATABASE_URL is not set")

    source_engine = make_engine(SOURCE_URL)
    target_engine = make_engine(TARGET_URL)

    prepare_source_schema(source_engine, SOURCE_URL)
    app.Base.metadata.create_all(bind=target_engine)

    SourceSession = sessionmaker(bind=source_engine)
    TargetSession = sessionmaker(bind=target_engine)

    models = [
        app.User,
        app.UserSession,
        app.CabinetRow,
        app.CapRow,
        app.ChatterfyIdRow,
        app.ChatterfyRow,
        app.FBRow,
        app.FinanceWalletRow,
        app.FinanceExpenseRow,
        app.FinanceIncomeRow,
        app.FinanceTransferRow,
        app.PartnerRow,
        app.TaskRow,
    ]

    source_session = SourceSession()
    target_session = TargetSession()
    try:
        counts = {}
        for model in models:
            counts[model.__tablename__] = migrate_table(source_session, target_session, model)
        for table_name, count in counts.items():
            print(f"{table_name}: {count}")
    finally:
        source_session.close()
        target_session.close()


if __name__ == "__main__":
    main()
