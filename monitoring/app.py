import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session


# ============================================================
# CONFIG
# ============================================================

DATABASE = "STUDY"
SCHEMA = "KJH"

FULL_LOAD_TABLE = "MIGRATION_MONITORING"
CDC_TABLE = "CDC_MONITORING"

FULL_TABLE = f"{DATABASE}.{SCHEMA}.{FULL_LOAD_TABLE}"
CDC_FULL_TABLE = f"{DATABASE}.{SCHEMA}.{CDC_TABLE}"


# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Migration / CDC Monitoring",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# STYLE
# ============================================================

st.markdown(
    """
    <style>

    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
        max-width: 1500px;
    }

    .metric-card {
        background: linear-gradient(
            135deg,
            rgba(30, 41, 59, 0.95),
            rgba(15, 23, 42, 0.95)
        );
        border: 1px solid rgba(148,163,184,0.18);
        border-radius: 14px;
        padding: 18px 20px;
        min-height: 110px;
    }

    .metric-title {
        color: #94a3b8;
        font-size: 0.85rem;
        margin-bottom: 8px;
    }

    .metric-value {
        color: white;
        font-size: 2rem;
        font-weight: 700;
    }

    .success {
        color: #22c55e;
        font-weight: 700;
    }

    .failed {
        color: #ef4444;
        font-weight: 700;
    }

    .warning {
        color: #f59e0b;
        font-weight: 700;
    }

    .cdc {
        color: #38bdf8;
        font-weight: 700;
    }

    .full {
        color: #a78bfa;
        font-weight: 700;
    }

    .section-title {
        font-size: 1.35rem;
        font-weight: 700;
        margin-top: 1.7rem;
        margin-bottom: 0.8rem;
    }

    .info-box {
        padding: 14px 18px;
        border-radius: 10px;
        background: rgba(30,41,59,0.7);
        border: 1px solid rgba(148,163,184,0.15);
        margin-bottom: 15px;
    }

    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# SNOWFLAKE SESSION
# ============================================================

@st.cache_resource
def get_session():
    return get_active_session()


try:

    session = get_session()

except Exception as e:

    st.error(
        f"Snowflake 세션 연결 실패: {e}"
    )

    st.stop()


# ============================================================
# DATA LOAD
# ============================================================

@st.cache_data(ttl=10)
def load_full_load_data():

    query = f"""
        SELECT
            RUN_ID,
            PIPELINE_TYPE,
            TABLE_NAME,
            START_TIME,
            END_TIME,
            STATUS,
            PG_COUNT,
            SF_COUNT,
            COUNT_DIFF,
            EXTRACTED_COUNT,
            STAGED_COUNT,
            WATERMARK,
            ERROR_MESSAGE
        FROM {FULL_TABLE}
        ORDER BY START_TIME DESC
    """

    return session.sql(query).to_pandas()


@st.cache_data(ttl=10)
def load_cdc_data():

    query = f"""
        SELECT
            DAG_RUN_ID,
            TABLE_NAME,
            CHECKED_AT,
            STATUS,
            SOURCE_ROW_COUNT,
            TARGET_ROW_COUNT,
            ROW_COUNT_DIFF,
            CDC_ROW_COUNT,
            LAST_WATERMARK,
            LAST_CDC_RUN_AT
        FROM {CDC_FULL_TABLE}
        ORDER BY CHECKED_AT DESC
    """

    return session.sql(query).to_pandas()


# ============================================================
# LOAD
# ============================================================

try:

    full_df = load_full_load_data()

except Exception as e:

    st.error(
        f"FULL LOAD 조회 실패: {e}"
    )

    st.stop()


try:

    cdc_df = load_cdc_data()

except Exception as e:

    st.error(
        f"CDC 조회 실패: {e}"
    )

    st.stop()


# ============================================================
# NORMALIZE
# ============================================================

def normalize_dataframe(df):

    if df.empty:
        return df

    df = df.copy()

    df.columns = [
        c.upper()
        for c in df.columns
    ]

    return df


full_df = normalize_dataframe(
    full_df
)

cdc_df = normalize_dataframe(
    cdc_df
)


# ============================================================
# NORMALIZE FULL LOAD
# ============================================================

if not full_df.empty:

    for col in [
        "PG_COUNT",
        "SF_COUNT",
        "COUNT_DIFF",
        "EXTRACTED_COUNT",
        "STAGED_COUNT",
    ]:

        if col in full_df.columns:

            full_df[col] = pd.to_numeric(
                full_df[col],
                errors="coerce",
            ).fillna(0)


# ============================================================
# NORMALIZE CDC
# ============================================================

if not cdc_df.empty:

    for col in [
        "SOURCE_ROW_COUNT",
        "TARGET_ROW_COUNT",
        "ROW_COUNT_DIFF",
        "CDC_ROW_COUNT",
    ]:

        if col in cdc_df.columns:

            cdc_df[col] = pd.to_numeric(
                cdc_df[col],
                errors="coerce",
            ).fillna(0)


# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:

    st.markdown("# 📊 Monitoring")

    st.caption(
        "PostgreSQL → Snowflake\n"
        "Migration / CDC Monitoring"
    )

    st.divider()

    st.markdown("### 🔎 필터")

    pipeline_filter = st.selectbox(
        "Pipeline Type",
        [
            "ALL",
            "FULL_LOAD",
            "CDC",
        ],
    )

    status_values = set()

    if not full_df.empty:

        status_values.update(
            full_df["STATUS"]
            .dropna()
            .astype(str)
            .tolist()
        )

    if not cdc_df.empty:

        status_values.update(
            cdc_df["STATUS"]
            .dropna()
            .astype(str)
            .tolist()
        )

    status_options = [
        "ALL"
    ] + sorted(
        status_values
    )

    status_filter = st.selectbox(
        "Status",
        status_options,
    )

    table_values = set()

    if not full_df.empty:

        table_values.update(
            full_df["TABLE_NAME"]
            .dropna()
            .astype(str)
            .tolist()
        )

    if not cdc_df.empty:

        table_values.update(
            cdc_df["TABLE_NAME"]
            .dropna()
            .astype(str)
            .tolist()
        )

    table_options = [
        "ALL"
    ] + sorted(
        table_values
    )

    table_filter = st.selectbox(
        "Table",
        table_options,
    )

    st.divider()

    st.markdown("### ⚙️ 화면")

    auto_refresh = st.checkbox(
        "10초마다 새로고침",
        value=False,
    )

    if st.button(
        "🔄 데이터 새로고침",
        use_container_width=True,
    ):

        st.cache_data.clear()
        st.rerun()

    st.divider()

    st.caption(
        f"Database: `{DATABASE}`"
    )

    st.caption(
        f"Schema: `{SCHEMA}`"
    )


# ============================================================
# FILTER FULL LOAD
# ============================================================

show_full_load = pipeline_filter in [
    "ALL",
    "FULL_LOAD",
]

filtered_full_df = full_df.copy()

if show_full_load:

    if status_filter != "ALL":

        filtered_full_df = filtered_full_df[
            filtered_full_df["STATUS"]
            .astype(str)
            .str.upper()
            == status_filter.upper()
        ]

    if table_filter != "ALL":

        filtered_full_df = filtered_full_df[
            filtered_full_df["TABLE_NAME"]
            .astype(str)
            == table_filter
        ]

else:

    filtered_full_df = full_df.iloc[
        0:0
    ].copy()


# ============================================================
# FILTER CDC
# ============================================================

show_cdc = pipeline_filter in [
    "ALL",
    "CDC",
]

filtered_cdc_df = cdc_df.copy()

if show_cdc:

    if status_filter != "ALL":

        filtered_cdc_df = filtered_cdc_df[
            filtered_cdc_df["STATUS"]
            .astype(str)
            .str.upper()
            == status_filter.upper()
        ]

    if table_filter != "ALL":

        filtered_cdc_df = filtered_cdc_df[
            filtered_cdc_df["TABLE_NAME"]
            .astype(str)
            == table_filter
        ]

else:

    filtered_cdc_df = cdc_df.iloc[
        0:0
    ].copy()


# ============================================================
# HEADER
# ============================================================

st.title(
    "📊 Migration / CDC Monitoring"
)

st.markdown(
    f"""
    <div class="info-box">
        <b>CDC:</b> {CDC_FULL_TABLE}
        &nbsp;&nbsp;&nbsp;
        <b>FULL LOAD:</b> {FULL_TABLE}
    </div>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# HELPER
# ============================================================

def get_latest_by_table(
    df,
    time_column,
):

    if df.empty:
        return df.copy()

    temp = df.copy()

    temp["_SORT_TIME"] = pd.to_datetime(
        temp[time_column],
        errors="coerce",
    )

    temp = (
        temp
        .sort_values(
            "_SORT_TIME",
            ascending=False,
        )
        .drop_duplicates(
            subset=["TABLE_NAME"],
            keep="first",
        )
    )

    temp = temp.drop(
        columns=["_SORT_TIME"],
        errors="ignore",
    )

    return temp


def display_status(
    status,
    diff,
):

    status = str(
        status
    ).upper()

    try:

        diff = float(diff)

    except:

        diff = 0

    if status == "FAILED":

        return "🔴 FAILED"

    if diff != 0:

        return "🟠 MISMATCH"

    return "🟢 정상"


# ============================================================
# LATEST CDC STATUS
# ============================================================

latest_cdc = get_latest_by_table(
    filtered_cdc_df,
    "CHECKED_AT",
)


# ============================================================
# TODAY CDC CHANGE COUNT
#
# 기준:
# CHECKED_AT이 오늘인 모든 CDC 실행의
# CDC_ROW_COUNT를 합산
#
# 새로고침해도 DB의 기록을 기준으로
# 다시 계산되기 때문에 값이 사라지지 않음
# ============================================================

if not filtered_cdc_df.empty:

    cdc_checked_at = pd.to_datetime(
        filtered_cdc_df["CHECKED_AT"],
        errors="coerce",
    )

    today = pd.Timestamp.now().normalize()

    today_cdc_df = filtered_cdc_df[
        cdc_checked_at >= today
    ].copy()

    today_change_count = int(
        today_cdc_df[
            "CDC_ROW_COUNT"
        ]
        .fillna(0)
        .sum()
    )

else:

    today_cdc_df = pd.DataFrame()

    today_change_count = 0


# ============================================================
# CDC TOP METRICS
# ============================================================

if show_cdc:

    st.markdown(
        '<div class="section-title">🔄 CDC 상태</div>',
        unsafe_allow_html=True,
    )

    if latest_cdc.empty:

        st.info(
            "CDC 실행 기록이 없습니다."
        )

    else:

        cdc_table_count = len(
            latest_cdc
        )

        cdc_ok_count = int(
            (
                (
                    latest_cdc["STATUS"]
                    .astype(str)
                    .str.upper()
                    == "SUCCESS"
                )
                &
                (
                    latest_cdc[
                        "ROW_COUNT_DIFF"
                    ]
                    .fillna(0)
                    == 0
                )
            ).sum()
        )

        cdc_failed_count = int(
            (
                latest_cdc["STATUS"]
                .astype(str)
                .str.upper()
                == "FAILED"
            ).sum()
        )

        cdc_mismatch_count = int(
            (
                latest_cdc[
                    "ROW_COUNT_DIFF"
                ]
                .fillna(0)
                != 0
            ).sum()
        )

        m1, m2, m3, m4 = st.columns(4)

        with m1:

            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-title">
                        CDC 대상 Table
                    </div>
                    <div class="metric-value cdc">
                        {cdc_table_count}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with m2:

            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-title">
                        정상 Table
                    </div>
                    <div class="metric-value success">
                        {cdc_ok_count}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with m3:

            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-title">
                        Mismatch / Failed
                    </div>
                    <div class="metric-value failed">
                        {cdc_failed_count + cdc_mismatch_count}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with m4:

            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-title">
                        오늘 CDC 변경 건수
                    </div>
                    <div class="metric-value cdc">
                        {today_change_count:,}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )


# ============================================================
# CDC TABLE STATUS
# ============================================================

if show_cdc and not latest_cdc.empty:

    st.markdown(
        "### 📋 Table별 최신 CDC 상태"
    )

    cdc_view = latest_cdc[
        [
            "TABLE_NAME",
            "STATUS",
            "SOURCE_ROW_COUNT",
            "TARGET_ROW_COUNT",
            "ROW_COUNT_DIFF",
            "CDC_ROW_COUNT",
            "LAST_CDC_RUN_AT",
            "LAST_WATERMARK",
        ]
    ].copy()

    cdc_view["STATUS_DISPLAY"] = cdc_view.apply(
        lambda row: display_status(
            row["STATUS"],
            row["ROW_COUNT_DIFF"],
        ),
        axis=1,
    )

    cdc_view = cdc_view[
        [
            "TABLE_NAME",
            "STATUS_DISPLAY",
            "SOURCE_ROW_COUNT",
            "TARGET_ROW_COUNT",
            "ROW_COUNT_DIFF",
            "CDC_ROW_COUNT",
            "LAST_CDC_RUN_AT",
            "LAST_WATERMARK",
        ]
    ]

    cdc_view = cdc_view.rename(
        columns={
            "TABLE_NAME": "Table",
            "STATUS_DISPLAY": "상태",
            "SOURCE_ROW_COUNT": "Source",
            "TARGET_ROW_COUNT": "Target",
            "ROW_COUNT_DIFF": "차이",
            "CDC_ROW_COUNT": "이번 CDC",
            "LAST_CDC_RUN_AT": "마지막 CDC",
            "LAST_WATERMARK": "Watermark",
        }
    )

    st.dataframe(
        cdc_view,
        use_container_width=True,
        hide_index=True,
        column_config={

            "Table": st.column_config.TextColumn(
                "Table",
                width="medium",
            ),

            "상태": st.column_config.TextColumn(
                "상태",
                width="small",
            ),

            "Source": st.column_config.NumberColumn(
                "Source",
                format="%d",
            ),

            "Target": st.column_config.NumberColumn(
                "Target",
                format="%d",
            ),

            "차이": st.column_config.NumberColumn(
                "차이",
                format="%d",
            ),

            "이번 CDC": st.column_config.NumberColumn(
                "이번 CDC",
                format="%d",
            ),

            "마지막 CDC": st.column_config.TextColumn(
                "마지막 CDC",
                width="medium",
            ),

            "Watermark": st.column_config.TextColumn(
                "Watermark",
                width="medium",
            ),
        },
    )


# ============================================================
# CDC ATTENTION
# ============================================================

if show_cdc and not latest_cdc.empty:

    attention_df = latest_cdc[
        (
            latest_cdc["STATUS"]
            .astype(str)
            .str.upper()
            == "FAILED"
        )
        |
        (
            latest_cdc[
                "ROW_COUNT_DIFF"
            ]
            .fillna(0)
            != 0
        )
    ].copy()

    if not attention_df.empty:

        st.markdown(
            '<div class="section-title">🚨 확인이 필요한 Table</div>',
            unsafe_allow_html=True,
        )

        attention_view = attention_df[
            [
                "TABLE_NAME",
                "STATUS",
                "SOURCE_ROW_COUNT",
                "TARGET_ROW_COUNT",
                "ROW_COUNT_DIFF",
                "CDC_ROW_COUNT",
                "LAST_CDC_RUN_AT",
            ]
        ].copy()

        attention_view = attention_view.rename(
            columns={
                "TABLE_NAME": "Table",
                "STATUS": "Status",
                "SOURCE_ROW_COUNT": "Source",
                "TARGET_ROW_COUNT": "Target",
                "ROW_COUNT_DIFF": "차이",
                "CDC_ROW_COUNT": "이번 CDC",
                "LAST_CDC_RUN_AT": "마지막 CDC",
            }
        )

        st.dataframe(
            attention_view,
            use_container_width=True,
            hide_index=True,
        )


# ============================================================
# RECENT CDC ACTIVITY
# ============================================================

if show_cdc and not filtered_cdc_df.empty:

    st.markdown(
        '<div class="section-title">📈 최근 CDC 활동</div>',
        unsafe_allow_html=True,
    )

    recent_cdc = filtered_cdc_df.copy()

    recent_cdc["_TIME"] = pd.to_datetime(
        recent_cdc["CHECKED_AT"],
        errors="coerce",
    )

    recent_cdc = (
        recent_cdc
        .sort_values(
            "_TIME",
            ascending=False,
        )
        .head(20)
    )

    recent_cdc_view = recent_cdc[
        [
            "TABLE_NAME",
            "CHECKED_AT",
            "STATUS",
            "CDC_ROW_COUNT",
            "ROW_COUNT_DIFF",
        ]
    ].copy()

    recent_cdc_view["상태"] = recent_cdc_view.apply(
        lambda row: display_status(
            row["STATUS"],
            row["ROW_COUNT_DIFF"],
        ),
        axis=1,
    )

    recent_cdc_view = recent_cdc_view[
        [
            "TABLE_NAME",
            "CHECKED_AT",
            "상태",
            "CDC_ROW_COUNT",
            "ROW_COUNT_DIFF",
        ]
    ]

    recent_cdc_view = recent_cdc_view.rename(
        columns={
            "TABLE_NAME": "Table",
            "CHECKED_AT": "확인 시간",
            "CDC_ROW_COUNT": "이번 CDC",
            "ROW_COUNT_DIFF": "Source/Target 차이",
        }
    )

    st.dataframe(
        recent_cdc_view,
        use_container_width=True,
        hide_index=True,
        column_config={

            "이번 CDC": st.column_config.NumberColumn(
                "이번 CDC",
                format="%d",
            ),

            "Source/Target 차이": st.column_config.NumberColumn(
                "Source/Target 차이",
                format="%d",
            ),
        },
    )


# ============================================================
# FULL LOAD
# ============================================================

if show_full_load:

    st.markdown(
        '<div class="section-title">📦 Initial FULL LOAD</div>',
        unsafe_allow_html=True,
    )

    if filtered_full_df.empty:

        st.info(
            "FULL LOAD 실행 기록이 없습니다."
        )

    else:

        latest_full = get_latest_by_table(
            filtered_full_df,
            "START_TIME",
        )

        full_view = latest_full[
            [
                "TABLE_NAME",
                "STATUS",
                "PG_COUNT",
                "SF_COUNT",
                "COUNT_DIFF",
                "START_TIME",
                "END_TIME",
            ]
        ].copy()

        full_view["상태"] = full_view.apply(
            lambda row: display_status(
                row["STATUS"],
                row["COUNT_DIFF"],
            ),
            axis=1,
        )

        full_view = full_view[
            [
                "TABLE_NAME",
                "상태",
                "PG_COUNT",
                "SF_COUNT",
                "COUNT_DIFF",
                "START_TIME",
                "END_TIME",
            ]
        ]

        full_view = full_view.rename(
            columns={
                "TABLE_NAME": "Table",
                "PG_COUNT": "Source",
                "SF_COUNT": "Target",
                "COUNT_DIFF": "차이",
                "START_TIME": "시작",
                "END_TIME": "종료",
            }
        )

        st.dataframe(
            full_view,
            use_container_width=True,
            hide_index=True,
            column_config={

                "Source": st.column_config.NumberColumn(
                    "Source",
                    format="%d",
                ),

                "Target": st.column_config.NumberColumn(
                    "Target",
                    format="%d",
                ),

                "차이": st.column_config.NumberColumn(
                    "차이",
                    format="%d",
                ),
            },
        )

        # FULL LOAD summary

        full_success = int(
            (
                latest_full["STATUS"]
                .astype(str)
                .str.upper()
                == "SUCCESS"
            ).sum()
        )

        full_failed = int(
            (
                latest_full["STATUS"]
                .astype(str)
                .str.upper()
                == "FAILED"
            ).sum()
        )

        full_mismatch = int(
            (
                latest_full[
                    "COUNT_DIFF"
                ]
                .fillna(0)
                != 0
            ).sum()
        )

        f1, f2, f3 = st.columns(3)

        with f1:

            st.metric(
                "FULL LOAD Table",
                len(latest_full),
            )

        with f2:

            st.metric(
                "정상",
                full_success,
            )

        with f3:

            st.metric(
                "Mismatch / Failed",
                full_failed + full_mismatch,
            )


# ============================================================
# DATA QUALITY SUMMARY
# ============================================================

st.markdown(
    '<div class="section-title">🧪 Data Quality</div>',
    unsafe_allow_html=True,
)

latest_full_for_quality = (
    get_latest_by_table(
        filtered_full_df,
        "START_TIME",
    )
    if not filtered_full_df.empty
    else filtered_full_df
)

latest_cdc_for_quality = (
    get_latest_by_table(
        filtered_cdc_df,
        "CHECKED_AT",
    )
    if not filtered_cdc_df.empty
    else filtered_cdc_df
)

full_mismatch_count = (
    int(
        (
            latest_full_for_quality[
                "COUNT_DIFF"
            ]
            .fillna(0)
            != 0
        ).sum()
    )
    if not latest_full_for_quality.empty
    else 0
)

cdc_mismatch_count = (
    int(
        (
            latest_cdc_for_quality[
                "ROW_COUNT_DIFF"
            ]
            .fillna(0)
            != 0
        ).sum()
    )
    if not latest_cdc_for_quality.empty
    else 0
)

full_failed_count = (
    int(
        (
            latest_full_for_quality["STATUS"]
            .astype(str)
            .str.upper()
            == "FAILED"
        ).sum()
    )
    if not latest_full_for_quality.empty
    else 0
)

cdc_failed_count = (
    int(
        (
            latest_cdc_for_quality["STATUS"]
            .astype(str)
            .str.upper()
            == "FAILED"
        ).sum()
    )
    if not latest_cdc_for_quality.empty
    else 0
)

d1, d2, d3 = st.columns(3)

with d1:

    st.metric(
        "CDC Mismatch",
        cdc_mismatch_count,
    )

with d2:

    st.metric(
        "CDC Failed",
        cdc_failed_count,
    )

with d3:

    st.metric(
        "FULL LOAD 문제",
        full_failed_count + full_mismatch_count,
    )


if (
    cdc_mismatch_count
    + cdc_failed_count
    + full_failed_count
    + full_mismatch_count
    == 0
):

    st.success(
        "현재 필터 기준 데이터 정합성 및 CDC 상태에 문제가 없습니다."
    )

else:

    st.warning(
        "확인이 필요한 CDC 또는 FULL LOAD 항목이 있습니다."
    )


# ============================================================
# FULL LOAD DETAIL HISTORY
# ============================================================

if show_full_load and not filtered_full_df.empty:

    with st.expander(
        "📜 FULL LOAD 전체 실행 이력",
        expanded=False,
    ):

        full_history_columns = [
            "RUN_ID",
            "PIPELINE_TYPE",
            "TABLE_NAME",
            "START_TIME",
            "END_TIME",
            "STATUS",
            "PG_COUNT",
            "SF_COUNT",
            "COUNT_DIFF",
            "EXTRACTED_COUNT",
            "STAGED_COUNT",
            "WATERMARK",
            "ERROR_MESSAGE",
        ]

        available_columns = [
            col
            for col in full_history_columns
            if col in filtered_full_df.columns
        ]

        st.dataframe(
            filtered_full_df[
                available_columns
            ],
            use_container_width=True,
            hide_index=True,
        )


# ============================================================
# CDC DETAIL HISTORY
# ============================================================

if show_cdc and not filtered_cdc_df.empty:

    with st.expander(
        "📜 CDC 전체 실행 이력",
        expanded=False,
    ):

        cdc_history_columns = [
            "DAG_RUN_ID",
            "TABLE_NAME",
            "CHECKED_AT",
            "STATUS",
            "SOURCE_ROW_COUNT",
            "TARGET_ROW_COUNT",
            "ROW_COUNT_DIFF",
            "CDC_ROW_COUNT",
            "LAST_WATERMARK",
            "LAST_CDC_RUN_AT",
        ]

        available_columns = [
            col
            for col in cdc_history_columns
            if col in filtered_cdc_df.columns
        ]

        st.dataframe(
            filtered_cdc_df[
                available_columns
            ],
            use_container_width=True,
            hide_index=True,
        )


# ============================================================
# FOOTER
# ============================================================

st.divider()

st.caption(
    f"CDC • {CDC_FULL_TABLE}"
)

st.caption(
    f"FULL LOAD • {FULL_TABLE}"
)


# ============================================================
# AUTO REFRESH
# ============================================================

if auto_refresh:

    import time

    time.sleep(10)

    st.rerun()