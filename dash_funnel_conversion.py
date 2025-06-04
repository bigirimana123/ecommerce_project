# dash_funnel_conversion.py
"""
Funnel Conversion Dashboard
Displays user conversion from view → cart → purchase
"""

import json
import dash
from dash import html, dcc
import pandas as pd
import plotly.express as px
import os

# ✅ Load funnel output data
json_path = "output/funnel"
files = [f for f in os.listdir(json_path) if f.endswith(".json")]
if not files:
    raise FileNotFoundError("No funnel output JSON found")

records = []
for file in files:
    with open(os.path.join(json_path, file)) as f:
        for line in f:
            records.append(json.loads(line))

funnel_df = pd.DataFrame(records)
funnel_df = funnel_df.sort_values("event", ascending=True)

# ✅ Ensure correct order for funnel
stage_order = ["view", "cart", "purchase"]
funnel_df["event"] = pd.Categorical(funnel_df["event"], categories=stage_order, ordered=True)
funnel_df = funnel_df.sort_values("event")

# ✅ Create funnel chart
fig = px.funnel(
    funnel_df,
    x="users",
    y="event",
    title="User Funnel Conversion: View → Cart → Purchase",
    labels={"users": "User Count", "event": "Stage"},
    color_discrete_sequence=["#636EFA"]
)

# ✅ Dash app layout
app = dash.Dash(__name__)
app.layout = html.Div([
    html.H2("Funnel Conversion Analysis"),
    dcc.Graph(figure=fig)
])

if __name__ == "__main__":
    app.run (debug=True)
