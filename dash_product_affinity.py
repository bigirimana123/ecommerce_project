# dash_product_affinity.py
"""
Product Affinity Dashboard
Displays top product pairs bought together based on Spark analysis.
Reads from output/recommendations/*.json
"""

import json
import dash
from dash import html, dcc
import pandas as pd
import plotly.express as px
import os

# ✅ Load data from output JSON
json_path = "output/recommendations"
files = [f for f in os.listdir(json_path) if f.endswith(".json")]

if not files:
    raise FileNotFoundError("No output JSON found in 'output/recommendations'")

data = []
for file in files:
    with open(os.path.join(json_path, file)) as f:
        for line in f:
            data.append(json.loads(line))

# ✅ Convert to DataFrame
pairs_df = pd.DataFrame(data)

# ✅ Create bar chart
fig = px.bar(
    pairs_df.sort_values("pair_count", ascending=True),
    x="pair_count",
    y=pairs_df.apply(lambda row: f"{row['product_A']} & {row['product_B']}", axis=1),
    orientation='h',
    labels={"pair_count": "Pair Count", "y": "Product Pair"},
    title="Top Product Pairs Frequently Bought Together"
)

# ✅ Initialize Dash app
app = dash.Dash(__name__)
app.layout = html.Div([
    html.H2("Product Recommendation - Affinity Analysis"),
    dcc.Graph(figure=fig)
])

if __name__ == "__main__":
    app.run (debug=True)
