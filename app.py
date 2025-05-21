"""
app_new.py

Author: Rupesh Kr
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from streamlit_autorefresh import st_autorefresh
from multi_index_oi_tracker_new import MultiIndexOITracker

st.set_page_config(page_title="Options Buyer OI & Volume Dashboard", layout="wide")
st.title("📊 Multi-Index Options OI & Volume Live Dashboard for Option Buyers")

refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 10, 120, 60)
st_autorefresh(interval=refresh_interval * 1000, key="oi_autorefresh")

tracker = MultiIndexOITracker.get_instance()
available_indices = [idx for idx in tracker.indices if not tracker.get_oi_history_df(idx).empty or tracker.get_current_oi(idx).get('ce_oi', 0) > 0]
if not available_indices:
    available_indices = tracker.indices[:1]

selected_index = st.sidebar.selectbox("Select Index", available_indices, index=0)
st.subheader(f"{selected_index} Analysis")

oi = tracker.get_current_oi(selected_index)

col1, col2, col3, col4 = st.columns(4)
with col1: st.metric("Current CE OI", f"{oi['ce_oi']:,}")
with col2: st.metric("Current PE OI", f"{oi['pe_oi']:,}")
with col3: st.metric("CE Volume", f"{oi['ce_volume']:,}")
with col4: st.metric("PE Volume", f"{oi['pe_volume']:,}")

df_history = tracker.get_oi_history_df(selected_index)

if not df_history.empty and len(df_history) > 1:
    latest_record = df_history.iloc[-1]
    previous_record = df_history.iloc[-16] if len(df_history) >= 16 else df_history.iloc[0]

    ce_oi_change = latest_record['CE_OI'] - previous_record['CE_OI']
    pe_oi_change = latest_record['PE_OI'] - previous_record['PE_OI']
    ce_vol_change = latest_record['CE_Volume'] - previous_record['CE_Volume']
    pe_vol_change = latest_record['PE_Volume'] - previous_record['PE_Volume']

    st.subheader("Recent OI Changes")
    change_col1, change_col2, change_col3, change_col4 = st.columns(4)
    with change_col1: st.metric("CE OI Change", f"{ce_oi_change:+,}")
    with change_col2: st.metric("PE OI Change", f"{pe_oi_change:+,}")
    with change_col3: st.metric("CE Volume Change", f"{ce_vol_change:+,}")
    with change_col4: st.metric("PE Volume Change", f"{pe_vol_change:+,}")

col_ratio1, col_ratio2 = st.columns(2)
with col_ratio1:
    if oi['pe_oi'] != 0:
        st.metric("CE/PE OI Ratio", f"{round(oi['ce_oi'] / oi['pe_oi'], 2)}")
    else:
        st.error("PE OI is 0")

with col_ratio2:
    if oi['pe_volume'] != 0:
        st.metric("CE/PE Volume Ratio", f"{round(oi['ce_volume'] / oi['pe_volume'], 2)}")
    else:
        st.error("PE Volume is 0")

if not df_history.empty and len(df_history) > 1:
    try:
        trend, signal = tracker.determine_market_trend_for_buyers(ce_oi_change, pe_oi_change, ce_vol_change, pe_vol_change)
        st.subheader(f"Market Trend: {trend}")

        if "BULLISH" in signal:
            st.success(f"Signal: {signal}")
        elif "BEARISH" in signal:
            st.error(f"Signal: {signal}")
        else:
            st.info(f"Signal: {signal}")

    except Exception as e:
        st.warning(f"Error: {e}")

st.subheader("Historical Data")
tab1, tab2, tab3 = st.tabs(["OI & Volume", "OI", "Volume"])

if not df_history.empty:
    with tab1:
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Scatter(x=df_history["Timestamp"], y=df_history["CE_OI"], name="CE OI", line=dict(color="green")), secondary_y=False)
        fig.add_trace(go.Scatter(x=df_history["Timestamp"], y=df_history["PE_OI"], name="PE OI", line=dict(color="red")), secondary_y=False)
        fig.add_trace(go.Scatter(x=df_history["Timestamp"], y=df_history["CE_Volume"], name="CE Volume", line=dict(color="lightgreen", dash="dash")), secondary_y=True)
        fig.add_trace(go.Scatter(x=df_history["Timestamp"], y=df_history["PE_Volume"], name="PE Volume", line=dict(color="lightcoral", dash="dash")), secondary_y=True)
        fig.update_layout(title=f"{selected_index} OI and Volume", height=500, hovermode="x unified")
        fig.update_yaxes(title_text="Open Interest", secondary_y=False)
        fig.update_yaxes(title_text="Volume", secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        fig_oi = go.Figure()
        fig_oi.add_trace(go.Scatter(x=df_history["Timestamp"], y=df_history["CE_OI"], name="CE OI", line=dict(color="green")))
        fig_oi.add_trace(go.Scatter(x=df_history["Timestamp"], y=df_history["PE_OI"], name="PE OI", line=dict(color="red")))
        fig_oi.update_layout(title=f"{selected_index} OI", height=400)
        st.plotly_chart(fig_oi, use_container_width=True)

    with tab3:
        fig_vol = go.Figure()
        fig_vol.add_trace(go.Scatter(x=df_history["Timestamp"], y=df_history["CE_Volume"], name="CE Volume", line=dict(color="green")))
        fig_vol.add_trace(go.Scatter(x=df_history["Timestamp"], y=df_history["PE_Volume"], name="PE Volume", line=dict(color="red")))
        fig_vol.update_layout(title=f"{selected_index} Volume", height=400)
        st.plotly_chart(fig_vol, use_container_width=True)

st.subheader("OI & Volume Trend Analysis")
analysis_df = tracker.get_analysis_table(selected_index)

if not analysis_df.empty and "Signal" in analysis_df.columns:
    analysis_df["Recommended Action"] = analysis_df["Signal"].apply(lambda x:
        "Buy CE" if "BULLISH" in x else
        "Buy PE" if "BEARISH" in x else "Wait"
    )
    st.dataframe(analysis_df, use_container_width=True)
else:
    st.warning(f"No analysis data for {selected_index}")

st.subheader("Cross-Index Comparison")
active_indices = [idx for idx in tracker.indices if not tracker.get_oi_history_df(idx).empty or tracker.get_current_oi(idx).get('ce_oi', 0) > 0]

if len(active_indices) > 1:
    comparison_data = []
    for idx in active_indices:
        current = tracker.get_current_oi(idx)
        signal, trend = "N/A", "N/A"
        analysis = tracker.get_analysis_table(idx)
        if not analysis.empty and "Time Window" in analysis.columns:
            row = analysis[analysis["Time Window"] == "Last 15 mins"]
            if not row.empty:
                signal = row.iloc[0]["Signal"]
                trend = row.iloc[0]["Trend"]
        comparison_data.append({
            "Index": idx,
            "CE OI": current.get('ce_oi', 0),
            "PE OI": current.get('pe_oi', 0),
            "CE/PE Ratio": round(current.get('ce_oi', 0) / max(current.get('pe_oi', 1), 1), 2),
            "CE Volume": current.get('ce_volume', 0),
            "PE Volume": current.get('pe_volume', 0),
            "Signal": signal,
            "Trend": trend
        })

    st.dataframe(pd.DataFrame(comparison_data), use_container_width=True)
    st.subheader("Market Trend Summary")
    cols = st.columns(len(comparison_data))
    for col, data in zip(cols, comparison_data):
        with col:
            st.subheader(data["Index"])
            sig = data["Signal"]
            if "BULLISH" in sig:
                st.success(f"Signal: {sig}")
            elif "BEARISH" in sig:
                st.error(f"Signal: {sig}")
            else:
                st.info(f"Signal: {sig}")
else:
    st.info("Not enough indices with data.")

st.sidebar.subheader("Export Data")
if st.sidebar.button("Export Data to CSV"):
    tracker.save_to_csv(selected_index)
    st.sidebar.success(f"Data for {selected_index} exported!")

st.caption("© 2025 Options Analytics Dashboard. Developed by Rupesh Kr.")
