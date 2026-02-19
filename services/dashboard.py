from flask import Flask, render_template_string
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "zivian_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres123")

app = Flask(__name__)

def get_engine():
    return create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Zivian — Elevate Chart Review</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Mono:wght@300;400;500&display=swap" rel="stylesheet"/>
<style>
  :root {
    --bg: #0a0f1e;
    --surface: #111827;
    --surface2: #1a2235;
    --border: #1e3a5f;
    --accent: #00d4ff;
    --accent2: #00ff9d;
    --accent3: #ff6b6b;
    --gold: #ffd700;
    --text: #e2e8f0;
    --muted: #64748b;
  }

  * { margin: 0; padding: 0; box-sizing: border-box; }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: 'DM Mono', monospace;
    min-height: 100vh;
    overflow-x: hidden;
  }

  body::before {
    content: '';
    position: fixed;
    top: 0; left: 0; right: 0; bottom: 0;
    background: 
      radial-gradient(ellipse at 10% 20%, rgba(0,212,255,0.06) 0%, transparent 50%),
      radial-gradient(ellipse at 90% 80%, rgba(0,255,157,0.06) 0%, transparent 50%);
    pointer-events: none;
    z-index: 0;
  }

  .container { max-width: 1400px; margin: 0 auto; padding: 0 2rem; position: relative; z-index: 1; }

  /* HEADER */
  header {
    padding: 2rem 0 1.5rem;
    border-bottom: 1px solid var(--border);
    margin-bottom: 2.5rem;
    animation: fadeDown 0.6s ease both;
  }

  .header-inner { display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 1rem; }

  .logo {
    display: flex; align-items: center; gap: 1rem;
  }

  .logo-icon {
    width: 44px; height: 44px;
    background: linear-gradient(135deg, var(--accent), var(--accent2));
    border-radius: 10px;
    display: flex; align-items: center; justify-content: center;
    font-size: 1.2rem;
    box-shadow: 0 0 20px rgba(0,212,255,0.3);
  }

  .logo-text h1 {
    font-family: 'DM Serif Display', serif;
    font-size: 1.6rem;
    background: linear-gradient(90deg, var(--accent), var(--accent2));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    letter-spacing: -0.5px;
  }

  .logo-text p { font-size: 0.7rem; color: var(--muted); letter-spacing: 2px; text-transform: uppercase; margin-top: 2px; }

  .badge {
    padding: 0.4rem 1rem;
    background: rgba(0,255,157,0.1);
    border: 1px solid rgba(0,255,157,0.3);
    border-radius: 50px;
    font-size: 0.72rem;
    color: var(--accent2);
    letter-spacing: 1px;
    animation: pulse 2s ease infinite;
  }

  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.6} }

  /* STATS GRID */
  .stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1.2rem;
    margin-bottom: 2.5rem;
  }

  .stat-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 16px;
    padding: 1.5rem;
    position: relative;
    overflow: hidden;
    animation: fadeUp 0.6s ease both;
    transition: transform 0.2s, border-color 0.2s;
  }

  .stat-card:hover { transform: translateY(-3px); border-color: var(--accent); }

  .stat-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
  }

  .stat-card.blue::before { background: linear-gradient(90deg, var(--accent), transparent); }
  .stat-card.green::before { background: linear-gradient(90deg, var(--accent2), transparent); }
  .stat-card.red::before { background: linear-gradient(90deg, var(--accent3), transparent); }
  .stat-card.gold::before { background: linear-gradient(90deg, var(--gold), transparent); }

  .stat-card:nth-child(1) { animation-delay: 0.1s; }
  .stat-card:nth-child(2) { animation-delay: 0.2s; }
  .stat-card:nth-child(3) { animation-delay: 0.3s; }
  .stat-card:nth-child(4) { animation-delay: 0.4s; }

  .stat-label { font-size: 0.65rem; color: var(--muted); letter-spacing: 2px; text-transform: uppercase; margin-bottom: 0.8rem; }

  .stat-value {
    font-family: 'DM Serif Display', serif;
    font-size: 2.8rem;
    line-height: 1;
    margin-bottom: 0.4rem;
  }

  .stat-card.blue .stat-value { color: var(--accent); }
  .stat-card.green .stat-value { color: var(--accent2); }
  .stat-card.red .stat-value { color: var(--accent3); }
  .stat-card.gold .stat-value { color: var(--gold); }

  .stat-sub { font-size: 0.7rem; color: var(--muted); }

  /* PIPELINE */
  .pipeline-section {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 16px;
    padding: 1.8rem;
    margin-bottom: 2.5rem;
    animation: fadeUp 0.6s 0.3s ease both;
  }

  .section-title {
    font-size: 0.65rem;
    letter-spacing: 3px;
    text-transform: uppercase;
    color: var(--muted);
    margin-bottom: 1.5rem;
  }

  .pipeline {
    display: flex;
    align-items: center;
    gap: 0;
    flex-wrap: wrap;
  }

  .pipeline-step {
    flex: 1;
    min-width: 140px;
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 1.2rem;
    text-align: center;
    position: relative;
  }

  .pipeline-step .step-icon { font-size: 1.8rem; margin-bottom: 0.5rem; }
  .pipeline-step .step-name { font-size: 0.7rem; color: var(--muted); letter-spacing: 1px; text-transform: uppercase; }
  .pipeline-step .step-count { font-family: 'DM Serif Display', serif; font-size: 1.6rem; color: var(--accent); margin: 0.3rem 0; }
  .pipeline-step .step-status { font-size: 0.65rem; color: var(--accent2); }

  .pipeline-arrow {
    font-size: 1.2rem;
    color: var(--border);
    padding: 0 0.5rem;
    flex-shrink: 0;
  }

  /* CHARTS TABLE */
  .charts-section {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 16px;
    padding: 1.8rem;
    animation: fadeUp 0.6s 0.4s ease both;
  }

  .charts-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 1.5rem;
    flex-wrap: wrap;
    gap: 1rem;
  }

  .ready-badge {
    background: rgba(255,215,0,0.1);
    border: 1px solid rgba(255,215,0,0.3);
    color: var(--gold);
    padding: 0.3rem 0.8rem;
    border-radius: 50px;
    font-size: 0.65rem;
    letter-spacing: 1px;
  }

  .table-wrap { overflow-x: auto; }

  table { width: 100%; border-collapse: collapse; }

  thead tr {
    border-bottom: 1px solid var(--border);
  }

  th {
    font-size: 0.6rem;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: var(--muted);
    padding: 0.8rem 1rem;
    text-align: left;
    font-weight: 400;
  }

  tbody tr {
    border-bottom: 1px solid rgba(30,58,95,0.5);
    transition: background 0.15s;
    animation: fadeUp 0.4s ease both;
  }

  tbody tr:hover { background: var(--surface2); }

  td {
    padding: 0.9rem 1rem;
    font-size: 0.78rem;
    color: var(--text);
  }

  .enc-id {
    font-family: 'DM Mono', monospace;
    color: var(--accent);
    font-size: 0.72rem;
  }

  .patient-id {
    color: var(--accent2);
    font-size: 0.72rem;
  }

  .state-badge {
    background: var(--surface2);
    border: 1px solid var(--border);
    padding: 0.2rem 0.6rem;
    border-radius: 6px;
    font-size: 0.68rem;
    color: var(--text);
  }

  .med-tag {
    background: rgba(0,212,255,0.08);
    border: 1px solid rgba(0,212,255,0.2);
    color: var(--accent);
    padding: 0.15rem 0.5rem;
    border-radius: 4px;
    font-size: 0.62rem;
    display: inline-block;
    margin: 0.1rem;
  }

  .sampled-badge {
    background: rgba(0,255,157,0.1);
    border: 1px solid rgba(0,255,157,0.3);
    color: var(--accent2);
    padding: 0.2rem 0.7rem;
    border-radius: 50px;
    font-size: 0.62rem;
    letter-spacing: 1px;
    white-space: nowrap;
  }

  .notes { color: var(--muted); font-size: 0.7rem; max-width: 220px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }

  /* FOOTER */
  footer {
    text-align: center;
    padding: 2rem 0;
    color: var(--muted);
    font-size: 0.68rem;
    letter-spacing: 1px;
    border-top: 1px solid var(--border);
    margin-top: 2.5rem;
  }

  @keyframes fadeUp { from{opacity:0;transform:translateY(20px)} to{opacity:1;transform:translateY(0)} }
  @keyframes fadeDown { from{opacity:0;transform:translateY(-10px)} to{opacity:1;transform:translateY(0)} }
</style>
</head>
<body>
<div class="container">

  <header>
    <div class="header-inner">
      <div class="logo">
        <div class="logo-icon">🏥</div>
        <div class="logo-text">
          <h1>Zivian Elevate</h1>
          <p>Chart Review Platform</p>
        </div>
      </div>
      <div class="badge">● PIPELINE ACTIVE</div>
    </div>
  </header>

  <!-- STATS -->
  <div class="stats-grid">
    <div class="stat-card blue">
      <div class="stat-label">Total Extracted</div>
      <div class="stat-value">{{ stats.total }}</div>
      <div class="stat-sub">Raw encounters from EHR</div>
    </div>
    <div class="stat-card green">
      <div class="stat-label">Silver Layer</div>
      <div class="stat-value">{{ stats.silver }}</div>
      <div class="stat-sub">Completed encounters</div>
    </div>
    <div class="stat-card red">
      <div class="stat-label">Excluded</div>
      <div class="stat-value">{{ stats.excluded }}</div>
      <div class="stat-sub">Cancelled / pending</div>
    </div>
    <div class="stat-card gold">
      <div class="stat-label">Selected for Review</div>
      <div class="stat-value">{{ stats.sampled }}</div>
      <div class="stat-sub">10% deterministic sample</div>
    </div>
  </div>

  <!-- PIPELINE -->
  <div class="pipeline-section">
    <div class="section-title">Pipeline Flow</div>
    <div class="pipeline">
      <div class="pipeline-step">
        <div class="step-icon">⚡</div>
        <div class="step-name">Extract</div>
        <div class="step-count">{{ stats.total }}</div>
        <div class="step-status">✓ Bronze Layer</div>
      </div>
      <div class="pipeline-arrow">→</div>
      <div class="pipeline-step">
        <div class="step-icon">🔧</div>
        <div class="step-name">Transform</div>
        <div class="step-count">{{ stats.silver }}</div>
        <div class="step-status">✓ Silver Layer</div>
      </div>
      <div class="pipeline-arrow">→</div>
      <div class="pipeline-step">
        <div class="step-icon">🎯</div>
        <div class="step-name">Sample</div>
        <div class="step-count">{{ stats.sampled }}</div>
        <div class="step-status">✓ Gold Layer</div>
      </div>
      <div class="pipeline-arrow">→</div>
      <div class="pipeline-step">
        <div class="step-icon">👨‍⚕️</div>
        <div class="step-name">Elevate</div>
        <div class="step-count">{{ stats.sampled }}</div>
        <div class="step-status">✓ Ready for Review</div>
      </div>
    </div>
  </div>

  <!-- CHARTS TABLE -->
  <div class="charts-section">
    <div class="charts-header">
      <div class="section-title" style="margin:0">Selected Patient Charts — Ready for Physician Review</div>
      <div class="ready-badge">⭐ {{ stats.sampled }} CHARTS READY</div>
    </div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Encounter ID</th>
            <th>Patient ID</th>
            <th>Physician</th>
            <th>Date of Service</th>
            <th>State</th>
            <th>Medications</th>
            <th>Clinical Notes</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          {% for row in charts %}
          <tr style="animation-delay: {{ loop.index0 * 0.03 }}s">
            <td><span class="enc-id">{{ row.encounter_id }}</span></td>
            <td><span class="patient-id">{{ row.patient_id }}</span></td>
            <td>{{ row.physician_id }}</td>
            <td>{{ row.date_of_service }}</td>
            <td><span class="state-badge">{{ row.state }}</span></td>
            <td>
              {% for med in row.medications.split(',') %}
                <span class="med-tag">{{ med.strip() }}</span>
              {% endfor %}
            </td>
            <td><span class="notes" title="{{ row.clinical_notes }}">{{ row.clinical_notes }}</span></td>
            <td><span class="sampled-badge">✓ SELECTED</span></td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>
  </div>

  <footer>
    ZIVIAN ELEVATE PLATFORM &nbsp;·&nbsp; BRONZE → SILVER → GOLD PIPELINE &nbsp;·&nbsp; POWERED BY AIRFLOW + DOCKER
  </footer>

</div>
</body>
</html>
"""

@app.route("/")
def index():
    engine = get_engine()
    
    total = pd.read_sql("SELECT COUNT(*) as c FROM gold_encounters", engine).iloc[0]['c']
    silver = pd.read_sql("SELECT COUNT(*) as c FROM silver_encounters", engine).iloc[0]['c']
    sampled = pd.read_sql("SELECT COUNT(*) as c FROM gold_encounters WHERE is_sampled = TRUE", engine).iloc[0]['c']
    excluded = total - silver

    charts_df = pd.read_sql("""
        SELECT encounter_id, patient_id, physician_id, 
               date_of_service, state, medications, clinical_notes
        FROM gold_encounters 
        WHERE is_sampled = TRUE 
        ORDER BY date_of_service DESC
    """, engine)

    charts = charts_df.to_dict('records')
    stats = {
        "total": int(total),
        "silver": int(silver),
        "excluded": int(excluded),
        "sampled": int(sampled)
    }

    return render_template_string(HTML, stats=stats, charts=charts)

if __name__ == "__main__":
    app.run(debug=True, port=5000)
