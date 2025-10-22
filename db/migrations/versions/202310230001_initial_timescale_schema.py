"""Initial Timescale schema for market data."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "202310230001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")

    op.create_table(
        "candles_1m",
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("bucket_start", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("open", sa.Numeric(18, 8), nullable=False),
        sa.Column("high", sa.Numeric(18, 8), nullable=False),
        sa.Column("low", sa.Numeric(18, 8), nullable=False),
        sa.Column("close", sa.Numeric(18, 8), nullable=False),
        sa.Column("volume", sa.Numeric(24, 8), nullable=False),
        sa.Column("quote_volume", sa.Numeric(24, 8), nullable=True),
        sa.Column("trade_count", sa.Integer(), nullable=True),
        sa.Column("taker_buy_volume", sa.Numeric(24, 8), nullable=True),
        sa.Column("taker_buy_quote_volume", sa.Numeric(24, 8), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("symbol", "bucket_start", name="pk_candles_1m"),
    )
    op.create_index(
        "ix_candles_1m_bucket_start",
        "candles_1m",
        ["bucket_start"],
        postgresql_using="btree",
    )
    op.execute(
        sa.text(
            """
            SELECT create_hypertable(
                'candles_1m',
                'bucket_start',
                partitioning_column => 'symbol',
                number_partitions => 8,
                chunk_time_interval => INTERVAL '7 days',
                if_not_exists => TRUE,
                create_default_indexes => FALSE
            );
            """
        )
    )
    op.execute(
        sa.text(
            """
            SELECT add_retention_policy(
                'candles_1m',
                INTERVAL '180 days',
                if_not_exists => TRUE
            );
            """
        )
    )

    op.create_table(
        "trades",
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("trade_ts", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("trade_id", sa.BigInteger(), nullable=False),
        sa.Column("price", sa.Numeric(18, 8), nullable=False),
        sa.Column("quantity", sa.Numeric(18, 8), nullable=False),
        sa.Column("is_buyer_maker", sa.Boolean(), nullable=False),
        sa.Column("side", sa.String(length=4), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("symbol", "trade_ts", "trade_id", name="pk_trades"),
    )
    op.create_index(
        "ix_trades_trade_ts",
        "trades",
        ["trade_ts"],
        postgresql_using="btree",
    )
    op.execute(
        sa.text(
            """
            SELECT create_hypertable(
                'trades',
                'trade_ts',
                partitioning_column => 'symbol',
                number_partitions => 16,
                chunk_time_interval => INTERVAL '3 days',
                if_not_exists => TRUE,
                create_default_indexes => FALSE
            );
            """
        )
    )
    op.execute(
        sa.text(
            """
            SELECT add_retention_policy(
                'trades',
                INTERVAL '30 days',
                if_not_exists => TRUE
            );
            """
        )
    )

    op.create_table(
        "oi_snapshots",
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("snapshot_ts", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("open_interest", sa.Numeric(28, 8), nullable=False),
        sa.Column("open_interest_usd", sa.Numeric(28, 8), nullable=True),
        sa.Column("basis_points", sa.Numeric(12, 6), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("symbol", "snapshot_ts", name="pk_oi_snapshots"),
    )
    op.create_index(
        "ix_oi_snapshots_snapshot_ts",
        "oi_snapshots",
        ["snapshot_ts"],
        postgresql_using="btree",
    )
    op.execute(
        sa.text(
            """
            SELECT create_hypertable(
                'oi_snapshots',
                'snapshot_ts',
                partitioning_column => 'symbol',
                number_partitions => 8,
                chunk_time_interval => INTERVAL '7 days',
                if_not_exists => TRUE,
                create_default_indexes => FALSE
            );
            """
        )
    )
    op.execute(
        sa.text(
            """
            SELECT add_retention_policy(
                'oi_snapshots',
                INTERVAL '120 days',
                if_not_exists => TRUE
            );
            """
        )
    )

    op.create_table(
        "funding",
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("funding_ts", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("funding_rate", sa.Numeric(12, 10), nullable=False),
        sa.Column("funding_rate_annualized", sa.Numeric(12, 10), nullable=True),
        sa.Column("funding_payment", sa.Numeric(18, 8), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("symbol", "funding_ts", name="pk_funding"),
    )
    op.create_index(
        "ix_funding_funding_ts",
        "funding",
        ["funding_ts"],
        postgresql_using="btree",
    )
    op.execute(
        sa.text(
            """
            SELECT create_hypertable(
                'funding',
                'funding_ts',
                partitioning_column => 'symbol',
                number_partitions => 8,
                chunk_time_interval => INTERVAL '30 days',
                if_not_exists => TRUE,
                create_default_indexes => FALSE
            );
            """
        )
    )
    op.execute(
        sa.text(
            """
            SELECT add_retention_policy(
                'funding',
                INTERVAL '365 days',
                if_not_exists => TRUE
            );
            """
        )
    )


def downgrade() -> None:
    for table in ("funding", "oi_snapshots", "trades", "candles_1m"):
        op.execute(
            sa.text(
                f"""
                DO $$
                BEGIN
                    PERFORM remove_retention_policy('{table}');
                EXCEPTION
                    WHEN others THEN
                        NULL;
                END $$;
                """
            )
        )

    op.drop_index("ix_funding_funding_ts", table_name="funding")
    op.drop_table("funding")

    op.drop_index("ix_oi_snapshots_snapshot_ts", table_name="oi_snapshots")
    op.drop_table("oi_snapshots")

    op.drop_index("ix_trades_trade_ts", table_name="trades")
    op.drop_table("trades")

    op.drop_index("ix_candles_1m_bucket_start", table_name="candles_1m")
    op.drop_table("candles_1m")
