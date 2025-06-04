# dash_seasonal_trend.py
"""
Seasonal Trend Dashboard
Visualizes monthly and weekday transaction trends
"""

import json
import dash
from dash import html, dcc
import pandas as pd
import plotly.express as px
import os

# ✅ Load JSON data from both directories
def load_data(folder):
    path = f"output/seasonal/{folder}"
    files = [f for f in os.listdir(path) if f.endswith(".json")]
    records = []
    for file in files:
        with open(os.path.join(path, file)) as f:
            for line in f:
                records.append(json.loads(line))
    return pd.DataFrame(records)

monthly_df = load_data("monthly").sort_values("month")
weekday_df = load_data("weekday")

# ✅ Ensure weekday order
weekday_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
weekday_df["weekday"] = pd.Categorical(weekday_df["weekday"], categories=weekday_order, ordered=True)
weekday_df = weekday_df.sort_values("weekday")

# ✅ Create charts
fig_monthly = px.line(
    monthly_df, x="month", y="txn_count",
    title="Monthly Transaction Trend",
    markers=True, labels={"txn_count": "Transaction Count"}
)

fig_weekday = px.bar(
    weekday_df, x="weekday", y="txn_count",
    title="Transactions by Weekday",
    labels={"txn_count": "Transaction Count"},
    color_discrete_sequence=["#00CC96"]
)

# ✅ Dash layout
app = dash.Dash(__name__)
app.layout = html.Div([
    html.H2("Seasonal Trend Analysis"),
    dcc.Graph(figure=fig_monthly),
    dcc.Graph(figure=fig_weekday)
])

if __name__ == "__main__":
    app.run (debug=True)
