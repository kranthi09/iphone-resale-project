from flask import Flask, request
import pandas as pd
import os
import json

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(BASE_DIR, "data", "cleaned_iphone_listings", "cleaned_iphone_listings.csv")

def get_data():
    return pd.read_csv(CSV_PATH)

@app.route("/")
def index():
    model = request.args.get("model", "All")
    state = request.args.get("state", "All")

    df_full = get_data()
    df = df_full.copy()

    if model != "All":
        df = df[df["model_family"] == model]
    if state != "All":
        df = df[df["us_state"] == state]

    # KPI values
    total     = len(df)
    avg_price = round(df["price"].mean(), 2) if total > 0 else 0
    sum_sold  = int(df["sold"].sum())

    # Dropdowns
    all_models = sorted(df_full["model_family"].unique().tolist())
    all_states = sorted(df_full["us_state"].unique().tolist())

    model_options = "".join(
        f'<option value="{m}" {"selected" if m == model else ""}>{m}</option>'
        for m in all_models
    )
    state_options = "".join(
        f'<option value="{s}" {"selected" if s == state else ""}>{s}</option>'
        for s in all_states
    )

    # Bar chart — avg price by model
    bar_data = (
        df.groupby("model_family")["price"]
        .mean().round(2)
        .sort_values(ascending=True)
    )
    bar_labels = json.dumps(bar_data.index.tolist())
    bar_values = json.dumps(bar_data.values.tolist())

    # Donut chart — condition breakdown
    donut_data   = df["condition_group"].value_counts()
    donut_labels = json.dumps(donut_data.index.tolist())
    donut_values = json.dumps(donut_data.values.tolist())

    # Map — listings by state
    state_data = df["us_state"].value_counts().reset_index()
    state_data.columns = ["state", "count"]
    map_states = json.dumps(state_data["state"].tolist())
    map_counts = json.dumps(state_data["count"].tolist())

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
      <title>iPhone Resale Dashboard</title>
      <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
      <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
      <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family: Segoe UI, sans-serif; background: #f4f6f9; }}
        .page {{ padding: 24px; }}

        .filter-bar {{
          background: white; padding: 14px 24px;
          margin-bottom: 20px; border-radius: 4px;
          box-shadow: 0 1px 4px rgba(0,0,0,0.08);
          display: flex; gap: 24px; align-items: center;
        }}
        .filter-bar label {{
          font-size: 0.85rem; font-weight: 600; color: #605e5c;
        }}
        .filter-bar select {{
          padding: 6px 12px; border: 1px solid #ccc;
          border-radius: 4px; font-size: 0.88rem; cursor: pointer;
        }}

        .kpi-row {{ display: flex; gap: 16px; margin-bottom: 24px; }}
        .kpi-card {{
          background: white; border-radius: 4px;
          padding: 20px 24px; flex: 1;
          box-shadow: 0 1px 4px rgba(0,0,0,0.08);
        }}
        .kpi-card .value {{
          font-size: 2.4rem; font-weight: 400;
          color: #252423; margin-bottom: 4px;
        }}
        .kpi-card .label {{ font-size: 0.85rem; color: #605e5c; }}

        .charts-row {{ display: flex; gap: 16px; }}
        .chart-card {{
          background: white; border-radius: 4px;
          padding: 20px 24px;
          box-shadow: 0 1px 4px rgba(0,0,0,0.08);
        }}
        .chart-card h3 {{
          font-size: 0.9rem; font-weight: 600;
          color: #252423; margin-bottom: 16px;
        }}
        .bar-card   {{ flex: 1.2; }}
        .donut-card {{ flex: 0.8; }}
        .map-card   {{ margin-top: 16px; }}
      </style>
    </head>
    <body>
      <div class="page">

        <!-- Filter Bar -->
        <div class="filter-bar">
          <label>Model:</label>
          <select data-testid="filter-model" onchange="applyFilter()">
            <option value="All" {"selected" if model == "All" else ""}>All Models</option>
            {model_options}
          </select>
          <label>State:</label>
          <select data-testid="filter-state" onchange="applyFilter()">
            <option value="All" {"selected" if state == "All" else ""}>All States</option>
            {state_options}
          </select>
        </div>

        <!-- KPI Cards -->
        <div class="kpi-row">
          <div class="kpi-card">
            <div class="value" data-testid="kpi-total">{total/1000:.3f}K</div>
            <div class="label">Count of price</div>
          </div>
          <div class="kpi-card">
            <div class="value" data-testid="kpi-avg-price">{avg_price:,.2f}</div>
            <div class="label">Average of price</div>
          </div>
          <div class="kpi-card">
            <div class="value" data-testid="kpi-sum-sold">{sum_sold//1000}K</div>
            <div class="label">Sum of sold</div>
          </div>
        </div>

        <!-- Bar + Donut -->
        <div class="charts-row">
          <div class="chart-card bar-card">
            <h3>Average of price by model_family</h3>
            <canvas id="barChart" height="320"></canvas>
          </div>
          <div class="chart-card donut-card">
            <h3>Count of price by condition_group</h3>
            <canvas id="donutChart"></canvas>
          </div>
        </div>

        <!-- Map -->
        <div class="chart-card map-card">
          <h3>Count of price by us_state</h3>
          <div id="map" style="height: 480px;"></div>
        </div>

      </div>

      <script>
        // Filter function
        function applyFilter() {{
          const model = document.querySelector('[data-testid="filter-model"]').value;
          const state = document.querySelector('[data-testid="filter-state"]').value;
          window.location.href = `/?model=${{encodeURIComponent(model)}}&state=${{encodeURIComponent(state)}}`;
        }}

        // Bar Chart
        new Chart(document.getElementById('barChart'), {{
          type: 'bar',
          data: {{
            labels: {bar_labels},
            datasets: [{{
              data: {bar_values},
              backgroundColor: '#118DFF',
              borderWidth: 0,
              barThickness: 14
            }}]
          }},
          options: {{
            indexAxis: 'y',
            plugins: {{ legend: {{ display: false }} }},
            scales: {{
              x: {{
                title: {{ display: true, text: 'Average of price', font: {{ size: 11 }} }},
                grid: {{ color: '#f0f0f0' }}
              }},
              y: {{
                title: {{ display: true, text: 'model_family', font: {{ size: 11 }} }},
                grid: {{ display: false }}
              }}
            }}
          }}
        }});

        // Donut Chart
        new Chart(document.getElementById('donutChart'), {{
          type: 'doughnut',
          data: {{
            labels: {donut_labels},
            datasets: [{{
              data: {donut_values},
              backgroundColor: ['#118DFF', '#12239E', '#E66C37', '#750985'],
              borderWidth: 0
            }}]
          }},
          options: {{
            plugins: {{
              legend: {{
                position: 'right',
                labels: {{ font: {{ size: 11 }}, padding: 16 }}
              }}
            }},
            cutout: '60%'
          }}
        }});

        // USA Bubble Map
        var mapStates = {map_states};
        var mapCounts = {map_counts};

        Plotly.newPlot('map', [{{
          type: 'scattergeo',
          locationmode: 'USA-states',
          locations: mapStates,
          marker: {{
            size: mapCounts.map(v => Math.sqrt(v) * 3),
            color: '#118DFF',
            opacity: 0.6,
            sizemode: 'diameter',
            line: {{ width: 0 }}
          }},
          text: mapStates.map((s, i) => s + ': ' + mapCounts[i] + ' listings'),
          hoverinfo: 'text'
        }}], {{
          geo: {{
            scope: 'usa',
            showland: true,
            landcolor: '#f0f0f0',
            showlakes: true,
            lakecolor: '#cce5ff',
            subunitcolor: '#cccccc',
            bgcolor: 'white'
          }},
          margin: {{ t: 0, b: 0, l: 0, r: 0 }},
          paper_bgcolor: 'white'
        }});
      </script>
    </body>
    </html>
    """

if __name__ == "__main__":
    app.run(debug=True, port=5000)