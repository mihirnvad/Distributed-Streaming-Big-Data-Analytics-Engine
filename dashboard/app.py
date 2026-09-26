"""Real-time fraud operations dashboard (Streamlit + Plotly on the Postgres serving layer).

streamlit run dashboard/app.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import psycopg
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # allow `streamlit run dashboard/app.py`

from common.settings import load_settings
from dashboard import queries as q

st.set_page_config(page_title="Fraud Ops - Real-Time", page_icon=":shield:", layout="wide")

# --------------------------------------------------------------------------- design tokens
# Categorical slots, sequential ramp and status palette from the validated reference
# palette; dark mode uses the dark-surface steps rather than an automatic inversion.
PALETTE = {
    "light": {"series": ["#2a78d6", "#eb6834"], "grid": "#e4e3df", "muted": "#52514e"},
    "dark": {"series": ["#3987e5", "#d95926"], "grid": "#383835", "muted": "#c3c2b7"},
}
SEQUENTIAL_BLUE = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
RISK_STYLE = {  # status colours always travel with an icon + label
    "CRITICAL": ("#d03b3b", "🔴"),
    "HIGH": ("#ec835a", "🟠"),
    "MEDIUM": ("#fab219", "🟡"),
    "LOW": ("#0ca30c", "🟢"),
}


def _mode() -> str:
    try:
        return "dark" if st.context.theme.type == "dark" else "light"
    except AttributeError:  # older Streamlit without st.context.theme
        return "light"


def _layout(fig: go.Figure, height: int = 300, **kwargs) -> go.Figure:
    tokens = PALETTE[_mode()]
    fig.update_layout(
        height=height,
        margin={"l": 8, "r": 8, "t": 8, "b": 8},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        hoverlabel={"namelength": -1},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0},
        **kwargs,
    )
    fig.update_xaxes(showgrid=False, linecolor=tokens["grid"], tickfont={"color": tokens["muted"]})
    fig.update_yaxes(gridcolor=tokens["grid"], zeroline=False, tickfont={"color": tokens["muted"]})
    return fig


# --------------------------------------------------------------------------- data access


@st.cache_resource
def _connection() -> psycopg.Connection:
    return psycopg.connect(load_settings().postgres.dsn, autocommit=True, application_name="fraud-dashboard")


def run(sql: str, tw: q.TimeWindow) -> pd.DataFrame:
    params = {"window": tw.window, "bucket": tw.bucket} if "%(" in sql else None
    for attempt in range(2):
        conn = _connection()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                cols = [d.name for d in cur.description] if cur.description else []
                return pd.DataFrame(cur.fetchall(), columns=cols)
        except psycopg.OperationalError:
            _connection.clear()  # stale connection (e.g. Postgres restarted): reconnect once
            if attempt:
                raise
    return pd.DataFrame()


def _num(df: pd.DataFrame, col: str, default: float = 0.0) -> float:
    if df.empty or col not in df or pd.isna(df.iloc[0][col]):
        return default
    return float(df.iloc[0][col])


def _fmt(df: pd.DataFrame, col: str, spec: str, unit: str = "") -> str:
    """Format a single-row metric, or an em dash when there is no data (never a misleading 0)."""
    value = _num(df, col, default=float("nan"))
    return "—" if pd.isna(value) else format(value, spec) + unit


# --------------------------------------------------------------------------- panels


def kpi_row(tw: q.TimeWindow) -> None:
    kpi = run(q.KPI_SUMMARY, tw)
    tput = run(q.THROUGHPUT, tw)
    e2e = run(q.END_TO_END_LATENCY, tw)
    mb = run(q.MICRO_BATCH_LATENCY, tw)

    c = st.columns(6)
    c[0].metric("Transactions", f"{_num(kpi, 'txn_count'):,.0f}", help=f"Scored in the {tw.label.lower()}")
    c[1].metric("Ingest throughput", f"{_num(tput, 'events_per_second'):,.0f} ev/s", help="Bronze, last 60 s")
    c[2].metric(
        "Alert rate",
        f"{100 * _num(kpi, 'alert_rate'):.2f}%",
        help=f"{_num(kpi, 'alert_count'):,.0f} alerts on ${_num(kpi, 'flagged_usd'):,.0f}",
    )
    c[3].metric(
        "Confirmed fraud",
        f"{_num(kpi, 'fraud_chargebacks'):,.0f}",
        help=f"Fraud-coded chargebacks reported: ${_num(kpi, 'fraud_chargeback_usd'):,.0f}",
    )
    c[4].metric(
        "p95 end-to-end latency",
        _fmt(e2e, "p95_s", ".1f", " s"),
        help=f"Kafka append -> queryable in Postgres, rows loaded in the last 5 min "
        f"(p50 {_fmt(e2e, 'p50_s', '.1f', ' s')})",
    )
    c[5].metric(
        "p95 micro-batch",
        _fmt(mb, "p95_s", ".2f", " s"),
        help=f"Gold scoring batch duration (p50 {_fmt(mb, 'p50_s', '.2f', ' s')} "
        f"over {_num(mb, 'batches'):,.0f} batches)",
    )


def volume_and_alert_charts(tw: q.TimeWindow) -> None:
    ts = run(q.TIMESERIES, tw)
    series = PALETTE[_mode()]["series"]
    left, right = st.columns(2)
    with left:
        st.markdown(f"**Transactions per {tw.bucket}**")
        fig = go.Figure(
            go.Scatter(
                x=ts.get("bucket"),
                y=ts.get("txn_count"),
                mode="lines+markers",  # markers keep isolated buckets visible
                line={"width": 2, "color": series[0]},
                marker={"size": 5},
                hovertemplate="%{x|%H:%M}<br>%{y:,} transactions<extra></extra>",
            )
        )
        st.plotly_chart(_layout(fig, hovermode="x"), width="stretch")
    with right:
        st.markdown(f"**Alert rate per {tw.bucket} (%)**")
        fig = go.Figure(
            go.Scatter(
                x=ts.get("bucket"),
                y=ts.get("alert_rate_pct"),
                mode="lines+markers",
                line={"width": 2, "color": series[1]},
                marker={"size": 5},
                customdata=ts.get("alert_count"),
                hovertemplate="%{x|%H:%M}<br>%{y:.2f}% flagged (%{customdata:,} alerts)<extra></extra>",
            )
        )
        st.plotly_chart(_layout(fig, hovermode="x"), width="stretch")
    with st.expander("Table view"):
        st.dataframe(ts, width="stretch", hide_index=True)


def alert_feed(tw: q.TimeWindow) -> None:
    st.markdown("**Live alert feed**")
    feed = run(q.ALERT_FEED, tw)
    if feed.empty:
        st.info("No alerts in this window yet.")
        return
    feed.insert(0, "risk", feed.pop("risk_level").map(lambda r: f"{RISK_STYLE.get(r, ('', '⚪'))[1]} {r}"))
    st.dataframe(
        feed,
        width="stretch",
        hide_index=True,
        height=380,
        column_config={
            "created_at": st.column_config.DatetimeColumn("Alerted", format="HH:mm:ss"),
            "event_ts": st.column_config.DatetimeColumn("Swiped", format="HH:mm:ss"),
            "fraud_score": st.column_config.ProgressColumn("Score", min_value=0.0, max_value=1.0, format="%.2f"),
            "amount_usd": st.column_config.NumberColumn("Amount", format="$%.2f"),
            "reasons": st.column_config.TextColumn("Rules fired", width="large"),
        },
    )


def category_heatmap(tw: q.TimeWindow) -> None:
    st.markdown("**Merchant-category risk: alert rate (%)**")
    heat = run(q.CATEGORY_RISK_HEATMAP, tw)
    if heat.empty:
        st.info("Waiting for data.")
        return
    pivot = heat.pivot_table(index="category_name", columns="bucket", values="alert_rate_pct", aggfunc="first")
    counts = heat.pivot_table(index="category_name", columns="bucket", values="txn_count", aggfunc="first")
    pivot = pivot.loc[pivot.mean(axis=1).sort_values().index]
    counts = counts.reindex(index=pivot.index, columns=pivot.columns)
    steps = len(SEQUENTIAL_BLUE) - 1
    fig = go.Figure(
        go.Heatmap(
            z=pivot.values,
            x=pivot.columns,
            y=pivot.index,
            customdata=counts.values,
            colorscale=[[i / steps, c] for i, c in enumerate(SEQUENTIAL_BLUE)],
            xgap=2,
            ygap=2,
            colorbar={"title": {"text": "% flagged"}, "thickness": 10},
            hovertemplate="%{y}<br>%{x|%H:%M}<br>%{z:.2f}% flagged of %{customdata:,} txns<extra></extra>",
        )
    )
    st.plotly_chart(_layout(fig, height=440), width="stretch")
    with st.expander("Table view"):
        st.dataframe(heat.sort_values(["bucket", "category_name"]), width="stretch", hide_index=True)


def rule_performance(tw: q.TimeWindow) -> None:
    st.markdown(f"**Rule quality vs. chargeback labels** (transactions older than {q.LABEL_MATURITY})")
    rules = run(q.RULE_PERFORMANCE, tw)
    conf = run(q.MODEL_CONFUSION, tw)
    if not conf.empty:
        tp, fp, fn, tn = (int(conf.iloc[0][k] or 0) for k in ("tp", "fp", "fn", "tn"))
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        fpr = fp / (fp + tn) if fp + tn else 0.0
        c = st.columns(3)
        c[0].metric("Alert precision", f"{precision:.1%}")
        c[1].metric("Fraud recall", f"{recall:.1%}")
        c[2].metric("False-positive rate", f"{fpr:.3%}")
    if rules.empty:
        st.info("Waiting for labelled transactions.")
        return
    rules = rules.sort_values("hits")
    series = PALETTE[_mode()]["series"]
    fig = go.Figure()
    for i, metric in enumerate(("precision", "recall")):
        fig.add_bar(
            y=rules["rule"],
            x=rules[metric].astype(float),
            name=metric.title(),
            orientation="h",
            marker={"color": series[i], "line": {"width": 0}},
            customdata=rules[["hits", "true_positives"]],
            hovertemplate="%{y}<br>" + metric + ": %{x:.1%}<br>%{customdata[0]:,} hits, "
            "%{customdata[1]:,} confirmed<extra></extra>",
        )
    fig.update_layout(barmode="group", bargap=0.35, bargroupgap=0.08)
    fig.update_xaxes(tickformat=".0%", range=[0, 1])
    st.plotly_chart(_layout(fig, height=340), width="stretch")
    with st.expander("Table view"):
        st.dataframe(rules.sort_values("hits", ascending=False), width="stretch", hide_index=True)


def fraud_map(tw: q.TimeWindow) -> None:
    st.markdown("**Where flagged transactions happen**")
    points = run(q.FLAGGED_LOCATIONS, tw)
    if points.empty:
        st.info("No flagged transactions yet.")
        return
    fig = go.Figure()
    for level in ("MEDIUM", "HIGH", "CRITICAL"):  # most severe drawn last, on top
        subset = points[points["risk_level"] == level]
        if subset.empty:
            continue
        color, icon = RISK_STYLE[level]
        fig.add_scattergeo(
            lat=subset["lat"],
            lon=subset["lon"],
            name=f"{icon} {level}",
            mode="markers",
            marker={"size": 8, "color": color, "opacity": 0.8, "line": {"width": 1, "color": "white"}},
            customdata=subset[["amount_usd", "reasons"]],
            hovertemplate="$%{customdata[0]:,.2f}<br>%{customdata[1]}<extra>" + level + "</extra>",
        )
    tokens = PALETTE[_mode()]
    fig.update_geos(
        projection_type="natural earth",
        showcountries=True,
        countrycolor=tokens["grid"],
        showland=True,
        landcolor="rgba(128,128,128,0.08)",
        showocean=False,
        showframe=False,
        bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(_layout(fig, height=380), width="stretch")


def merchant_spikes(tw: q.TimeWindow) -> None:
    st.markdown("**Merchants to watch** (latest 5-min sliding window vs. own trailing baseline)")
    spikes = run(q.MERCHANT_SPIKES, tw)
    if spikes.empty:
        st.info("Sliding-window aggregates will appear after the first windows close.")
        return
    st.dataframe(
        spikes,
        width="stretch",
        hide_index=True,
        column_config={
            "window_start": st.column_config.DatetimeColumn("Window", format="HH:mm"),
            "spike_ratio": st.column_config.NumberColumn("Spike x", format="%.2f"),
            "avg_fraud_score": st.column_config.NumberColumn("Avg score", format="%.3f"),
            "total_amount_usd": st.column_config.NumberColumn("Volume", format="$%.0f"),
        },
    )


def pipeline_health(tw: q.TimeWindow) -> None:
    st.markdown("**Pipeline health** (per streaming query)")
    health = run(q.PIPELINE_HEALTH, tw)
    if health.empty:
        st.info("No micro-batch metrics recorded yet.")
        return
    st.dataframe(
        health,
        width="stretch",
        hide_index=True,
        column_config={
            "avg_rows_per_s": st.column_config.NumberColumn("Rows/s", format="%d"),
            "p50_batch_ms": st.column_config.NumberColumn("p50 batch (ms)", format="%d"),
            "p95_batch_ms": st.column_config.NumberColumn("p95 batch (ms)", format="%d"),
            "kafka_lag": st.column_config.NumberColumn("Kafka lag (offsets)", format="%d"),
            "last_progress": st.column_config.DatetimeColumn("Last batch", format="HH:mm:ss"),
            "watermark": st.column_config.DatetimeColumn("Watermark", format="HH:mm:ss"),
        },
    )


# --------------------------------------------------------------------------- page


def render(tw: q.TimeWindow) -> None:
    try:
        kpi_row(tw)
    except psycopg.OperationalError as exc:
        st.error(f"Cannot reach the warehouse: {exc}")
        return
    volume_and_alert_charts(tw)
    left, right = st.columns([3, 2])
    with left:
        alert_feed(tw)
    with right:
        fraud_map(tw)
    left, right = st.columns([3, 2])
    with left:
        category_heatmap(tw)
    with right:
        rule_performance(tw)
    merchant_spikes(tw)
    pipeline_health(tw)
    st.caption(f"Refreshed {pd.Timestamp.now(tz='UTC'):%Y-%m-%d %H:%M:%S} UTC")


def main() -> None:
    st.title("Real-time fraud operations")
    controls = st.columns([2, 1, 1, 4])
    tw = controls[0].selectbox("Time range", q.TIME_WINDOWS, index=1, format_func=lambda t: t.label)
    auto = controls[1].toggle("Auto-refresh", value=True)
    every = controls[2].selectbox("Every", (5, 10, 30, 60), index=0, format_func=lambda s: f"{s} s")
    st.fragment(run_every=every if auto else None)(render)(tw)


main()
