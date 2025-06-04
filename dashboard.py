import json
import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc
from pathlib import Path

# ---------------------------
# Improved JSON Loader
# ---------------------------
def load_json_folder(path):
    data = []
    for file in sorted(Path(path).glob("*.json")):
        try:
            with open(file, 'r') as f:
                # Try reading as standard JSON first
                try:
                    data.append(json.load(f))
                except json.JSONDecodeError:
                    # If fails, try reading as line-delimited JSON
                    f.seek(0)
                    for line in f:
                        line = line.strip()
                        if line:  # Skip empty lines
                            try:
                                data.append(json.loads(line))
                            except json.JSONDecodeError as e:
                                print(f"Error in {file.name}: {e}")
        except Exception as e:
            print(f"Error reading {file}: {e}")
    return pd.json_normalize(data)

# ---------------------------
# Load Data with Error Handling
# ---------------------------
try:
    recommendations = load_json_folder("output/recommendations")
    cohorts = load_json_folder("output/cohorts")
    clv = load_json_folder("output/clv")
except Exception as e:
    print(f"Failed to load data: {e}")
    # Create empty DataFrames if loading fails
    recommendations = pd.DataFrame()
    cohorts = pd.DataFrame()
    clv = pd.DataFrame()

# ---------------------------
# Dashboard App
# ---------------------------
app = Dash(__name__)
app.title = "E-commerce Analytics Dashboard"

# Helper function to create figures with error handling
def create_figure(data, fig_type, **kwargs):
    if data.empty:
        return px.scatter(title="No data available")
    try:
        return fig_type(data, **kwargs)
    except Exception as e:
        print(f"Figure creation error: {e}")
        return px.scatter(title="Error displaying data")

app.layout = html.Div([
    html.H1(" E-commerce Analytics Dashboard", style={"textAlign": "center"}),
    
    html.H2(" Product Recommendations"),
    dcc.Graph(
        figure=create_figure(
            recommendations.head(10),
            px.bar,
            x="pair_count",
            y="product_A",
            color="product_B",
            orientation="h",
            title="Top 10 Frequently Bought Together Product Pairs"
        )
    ),

    html.H2(" Cohort Spending Matrix"),
    dcc.Graph(
        figure=create_figure(
            cohorts,
            px.scatter,
            x="registration_month",
            y="txn_month",
            size="items_bought",
            color="items_bought",
            title="Cohort Purchase Patterns",
            size_max=30
        )
    ),

    html.H2(" Customer Lifetime Value (CLV)"),
    dcc.Graph(
        figure=create_figure(
            clv.sort_values("clv_per_session", ascending=False).head(10),
            px.bar,
            x="user_id",
            y="clv_per_session",
            title="Top 10 Users by CLV per Session"
        )
    )
])

if __name__ == "__main__":
    app.run(debug=True, port=8050)  # Changed from run_server to run
