import json
import logging
import math
import os
import re
import time
import uuid
from pathlib import Path

from flask import Flask, render_template_string, request, redirect, url_for, send_from_directory, flash

try:
    import folium
    import pandas as pd
    import requests
    from branca.element import Element
    from folium.plugins import AntPath
    from ortools.constraint_solver import pywrapcp, routing_enums_pb2
except ImportError as e:
    raise SystemExit(
        f"\n[FATAL] Missing dependency: {e}\n"
        "Run:\n"
        "pip install flask pandas openpyxl folium requests branca ortools\n"
    )

app = Flask(__name__)
app.secret_key = "route-optimizer-local-secret"

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "outputs"
CACHE_DIR = BASE_DIR / "cache"

OUTPUT_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

GEOCODE_CACHE_PATH = CACHE_DIR / "geocode_cache.json"

INPUT_PATH = "sample_route_data.csv"

DAY_COLUMN = "Collection Day"

DEPOT_NAME = "Central Depot"
DEPOT_ADDRESS = """Central Depot
Sample Industrial Area
Manchester
UK"""

GOOGLE_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()
if not GOOGLE_API_KEY:
    raise SystemExit("[FATAL] GOOGLE_MAPS_API_KEY environment variable is not set.")

SHOW_ANT_PATH = True
GOOGLE_OPTIMIZE_MAX_STOPS = 23
ORTOOLS_TIME_LIMIT_SECONDS = 20

HTTP_TIMEOUT = 60
MAX_RETRIES = 4
BACKOFF_BASE = 1.8
PAUSE_BETWEEN_CALLS_SEC = 0.10

ZOOM_START = 10
KM_TO_MILES = 0.621371
KM_PER_LITRE = 8.0
FUEL_PRICE_GBP = 1.55
CO2_PER_LITRE = 2.68

GEOCODE_REVIEW_THRESHOLD = 40
GEOCODE_GOOD_THRESHOLD = 60

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("route_optimizer_flask")

BASE_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>Route Optimizer</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        :root {
            --bg: #f5f7fb;
            --card: #ffffff;
            --text: #111827;
            --muted: #6b7280;
            --border: #e5e7eb;
            --primary: #0f766e;
            --primary-dark: #115e59;
            --secondary: #1d4ed8;
            --secondary-dark: #1e40af;
            --dark: #111827;
            --dark-hover: #030712;
            --warn-bg: #fef3c7;
            --warn-text: #92400e;
            --warn-border: #fcd34d;
        }
        * { box-sizing: border-box; }
        body {
            font-family: Arial, sans-serif;
            background: var(--bg);
            margin: 0;
            padding: 0;
            color: var(--text);
        }
        .container {
            max-width: 1280px;
            margin: 24px auto;
            padding: 20px;
        }
        .card {
            background: var(--card);
            border-radius: 16px;
            box-shadow: 0 6px 20px rgba(0,0,0,0.08);
            padding: 24px;
            margin-bottom: 20px;
        }
        h1, h2, h3 { margin-top: 0; }
        .muted { color: var(--muted); font-size: 13px; }
        label {
            display: block;
            font-weight: 700;
            margin-bottom: 6px;
        }
        select {
            width: 100%;
            padding: 11px 12px;
            border: 1px solid #d1d5db;
            border-radius: 10px;
            margin-bottom: 16px;
            background: white;
            font-size: 14px;
        }
        button, .btn {
            display: inline-block;
            border: none;
            background: var(--primary);
            color: white;
            padding: 10px 16px;
            border-radius: 10px;
            cursor: pointer;
            font-weight: 700;
            text-decoration: none;
        }
        button:hover, .btn:hover {
            background: var(--primary-dark);
        }
        .btn-secondary { background: var(--secondary); }
        .btn-secondary:hover { background: var(--secondary-dark); }
        .btn-dark { background: var(--dark); }
        .btn-dark:hover { background: var(--dark-hover); }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
            gap: 16px;
        }
        .stat {
            background: #f9fafb;
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 16px;
        }
        .stat .label {
            font-size: 13px;
            color: var(--muted);
            margin-bottom: 6px;
            font-weight: 600;
        }
        .stat .value {
            font-size: 24px;
            font-weight: 700;
            color: var(--text);
        }
        .msg {
            padding: 12px 14px;
            border-radius: 10px;
            margin-bottom: 16px;
            background: var(--warn-bg);
            color: var(--warn-text);
            border: 1px solid var(--warn-border);
        }
        .actions {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
        }
        .pill {
            display: inline-block;
            background: #e5f3ff;
            color: #1d4ed8;
            font-size: 12px;
            border-radius: 999px;
            padding: 4px 10px;
            margin-bottom: 10px;
            font-weight: 700;
        }
        iframe {
            width: 100%;
            height: 760px;
            border: none;
            border-radius: 14px;
            background: #fff;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
        }
        th, td {
            border-bottom: 1px solid var(--border);
            padding: 10px 8px;
            text-align: left;
            vertical-align: top;
        }
        th {
            background: #f9fafb;
            position: sticky;
            top: 0;
            z-index: 1;
        }
        .table-wrap {
            max-height: 420px;
            overflow: auto;
            border: 1px solid var(--border);
            border-radius: 12px;
        }
        .section-head {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 12px;
            margin-bottom: 12px;
        }
        .badge {
            display: inline-block;
            padding: 5px 10px;
            border-radius: 999px;
            font-size: 12px;
            font-weight: 700;
        }
        .badge-good { background: #dcfce7; color: #166534; }
        .badge-review { background: #fef3c7; color: #92400e; }
        .badge-poor { background: #fee2e2; color: #991b1b; }
        .meta-row {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 12px;
        }
        .meta-box {
            background: #f9fafb;
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 14px;
        }
        @media (max-width: 768px) {
            iframe { height: 520px; }
        }
    </style>
</head>
<body>
<div class="container">
    {% with messages = get_flashed_messages() %}
      {% if messages %}
        {% for message in messages %}
          <div class="msg">{{ message }}</div>
        {% endfor %}
      {% endif %}
    {% endwith %}
    {{ content|safe }}
</div>
</body>
</html>
"""

INDEX_CONTENT = """
<div class="card">
    <div class="pill">Fixed File Mode</div>
    <h1>Route Optimizer</h1>
    <p class="muted"><b>Input file:</b> {{ input_path }}</p>
    <p class="muted"><b>Depot:</b> {{ depot_name }}</p>
    <p class="muted"><b>Collection day column:</b> {{ day_column }}</p>

    <form method="post" action="/run">
        <label>Route</label>
        <select name="route" required>
            {% for route in routes %}
              <option value="{{ route }}">{{ route }}</option>
            {% endfor %}
        </select>

        <label>Collection Day</label>
        <select name="day" required>
            {% for day in days %}
              <option value="{{ day }}">{{ day }}</option>
            {% endfor %}
        </select>

        <button type="submit">Run Optimization</button>
    </form>
</div>
"""

RESULT_CONTENT = """
<div class="card">
    <div class="pill">Result</div>
    <h1>Route {{ result.route }} · {{ result.day }}</h1>
    <div class="meta-row">
        <div class="meta-box"><b>Method</b><br><span class="muted">{{ result.method }}</span></div>
        <div class="meta-box"><b>Input File</b><br><span class="muted">{{ result.input_file }}</span></div>
        <div class="meta-box"><b>Depot</b><br><span class="muted">{{ result.depot_name }}</span></div>
        <div class="meta-box"><b>Runtime</b><br><span class="muted">{{ result.runtime_sec }}s</span></div>
    </div>
</div>

<div class="card">
    <h2>Summary</h2>
    <div class="grid">
        <div class="stat"><div class="label">Service Rows</div><div class="value">{{ result.rows }}</div></div>
        <div class="stat"><div class="label">Unique Stops</div><div class="value">{{ result.stops }}</div></div>
        <div class="stat"><div class="label">Changed Stops</div><div class="value">{{ result.changed_stops }}</div></div>
        <div class="stat"><div class="label">Changed %</div><div class="value">{{ result.changed_pct }}</div></div>
        <div class="stat"><div class="label">Review Needed</div><div class="value">{{ result.review_needed }}</div></div>
        <div class="stat"><div class="label">Geocode Failures</div><div class="value">{{ result.failures }}</div></div>
    </div>
</div>

<div class="card">
    <h2>Distance / Time</h2>
    <div class="grid">
        <div class="stat"><div class="label">Manual Miles</div><div class="value">{{ result.manual_miles }}</div></div>
        <div class="stat"><div class="label">Optimized Miles</div><div class="value">{{ result.optimized_miles }}</div></div>
        <div class="stat"><div class="label">Saved Miles</div><div class="value">{{ result.saved_miles }}</div></div>
        <div class="stat"><div class="label">Saved %</div><div class="value">{{ result.saved_pct }}</div></div>
        <div class="stat"><div class="label">Manual Time</div><div class="value">{{ result.manual_time }}</div></div>
        <div class="stat"><div class="label">Optimized Time</div><div class="value">{{ result.optimized_time }}</div></div>
        <div class="stat"><div class="label">Time Saved</div><div class="value">{{ result.time_saved }}</div></div>
    </div>
</div>

<div class="card">
    <h2>Fuel / CO₂</h2>
    <div class="grid">
        <div class="stat"><div class="label">Fuel Saved (L)</div><div class="value">{{ result.fuel_saved_l }}</div></div>
        <div class="stat"><div class="label">Cost Saved (£)</div><div class="value">{{ result.cost_saved_gbp }}</div></div>
        <div class="stat"><div class="label">CO₂ Saved (kg)</div><div class="value">{{ result.co2_saved_kg }}</div></div>
    </div>
</div>

<div class="card">
    <div class="section-head">
        <h2 style="margin:0;">Interactive Map</h2>
        <span class="muted">Displayed directly inside the app</span>
    </div>
    <iframe src="/download/{{ result.map_file }}"></iframe>
</div>

<div class="card">
    <div class="section-head">
        <h2 style="margin:0;">Downloads</h2>
    </div>
    <div class="actions">
        <a class="btn" href="/download/{{ result.excel_file }}">Download Excel</a>
        <a class="btn btn-secondary" href="/download/{{ result.csv_file }}">Download Changed CSV</a>
        <a class="btn btn-secondary" href="/download/{{ result.review_csv_file }}">Download Geocode Review CSV</a>
        <a class="btn btn-secondary" href="/download/{{ result.failed_csv_file }}">Download Failed Geocodes CSV</a>
        <a class="btn btn-dark" href="/download/{{ result.map_file }}" target="_blank">Open Map in New Tab</a>
        <a class="btn btn-secondary" href="/">Back</a>
    </div>
</div>

<div class="card">
    <div class="section-head">
        <h2 style="margin:0;">Geocode Review</h2>
        <span class="muted">{{ review_rows_count }} rows</span>
    </div>
    {% if review_rows %}
    <div class="table-wrap">
        <table>
            <thead>
                <tr>
                    {% for c in review_columns %}
                    <th>{{ c }}</th>
                    {% endfor %}
                </tr>
            </thead>
            <tbody>
                {% for row in review_rows %}
                <tr>
                    {% for c in review_columns %}
                    <td>
                        {% if c == 'GeocodeQuality' %}
                            {% if row[c] == 'Good' %}
                                <span class="badge badge-good">{{ row[c] }}</span>
                            {% elif row[c] == 'Review' %}
                                <span class="badge badge-review">{{ row[c] }}</span>
                            {% else %}
                                <span class="badge badge-poor">{{ row[c] }}</span>
                            {% endif %}
                        {% else %}
                            {{ row[c] }}
                        {% endif %}
                    </td>
                    {% endfor %}
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
    {% else %}
    <p class="muted">No review rows.</p>
    {% endif %}
</div>

<div class="card">
    <div class="section-head">
        <h2 style="margin:0;">Failed Geocodes</h2>
        <span class="muted">{{ failed_rows_count }} rows</span>
    </div>
    {% if failed_rows %}
    <div class="table-wrap">
        <table>
            <thead>
                <tr>
                    {% for c in failed_columns %}
                    <th>{{ c }}</th>
                    {% endfor %}
                </tr>
            </thead>
            <tbody>
                {% for row in failed_rows %}
                <tr>
                    {% for c in failed_columns %}
                    <td>{{ row[c] }}</td>
                    {% endfor %}
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
    {% else %}
    <p class="muted">No failed geocodes.</p>
    {% endif %}
</div>
"""

def safe_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()

def safe_int(v, default=0) -> int:
    try:
        if pd.isna(v):
            return default
        return int(v)
    except Exception:
        return default

def normalize_route_value(v):
    s = safe_str(v)
    if not s:
        return ""
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
        return str(f)
    except Exception:
        return s.upper()

def normalize_postcode(pc: str) -> str:
    pc = safe_str(pc).upper()
    pc = re.sub(r"[^A-Z0-9]", "", pc)
    if len(pc) > 3:
        pc = pc[:-3] + " " + pc[-3:]
    return pc.strip() if pc not in ("", "NAN", "NONE") else ""

def clean_address(raw: str) -> str:
    s = safe_str(raw).upper()
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace(" LANCS", " LANCASHIRE")
    s = s.replace(" MCR ", " MANCHESTER ")
    s = re.sub(r"\b(.+?)\s+\1\b", r"\1", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s.title()

def html_escape(s: str) -> str:
    s = safe_str(s)
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
         .replace('"', "&quot;")
         .replace("'", "&#39;")
    )

def slugify(value: str) -> str:
    s = safe_str(value).strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "value"

def minutes_to_hhmm(minutes: float) -> str:
    total = max(0, int(round(minutes)))
    h, m = divmod(total, 60)
    return f"{h}h {m:02d}m"

def km_to_miles(km: float) -> float:
    return km * KM_TO_MILES

def litres_saved_from_km(km: float) -> float:
    return km / KM_PER_LITRE if KM_PER_LITRE else 0.0

def decode_polyline(polyline_str: str):
    index, lat, lng = 0, 0, 0
    coordinates = []

    while index < len(polyline_str):
        shift = 0
        result = 0
        while True:
            b = ord(polyline_str[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        lat += ~(result >> 1) if (result & 1) else (result >> 1)

        shift = 0
        result = 0
        while True:
            b = ord(polyline_str[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        lng += ~(result >> 1) if (result & 1) else (result >> 1)

        coordinates.append((lat / 1e5, lng / 1e5))

    return coordinates

def load_json_cache(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            log.warning("Could not read cache %s: %s", path, exc)
    return {}

def save_json_cache(path: Path, data: dict) -> None:
    try:
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:
        log.warning("Could not save cache %s: %s", path, exc)

geocode_cache = load_json_cache(GEOCODE_CACHE_PATH)

def google_get(url: str, params: dict) -> dict:
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                status = data.get("status", "OK")
                if status in ("OK", "ZERO_RESULTS"):
                    time.sleep(PAUSE_BETWEEN_CALLS_SEC)
                    return data
                raise RuntimeError(
                    f"Google API status={status} error={data.get('error_message', '')}"
                )
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:500]}")
        except Exception as exc:
            last_exc = exc
            wait = BACKOFF_BASE ** attempt
            log.warning("Request failed (attempt %d/%d): %s", attempt, MAX_RETRIES, exc)
            time.sleep(wait)

    raise RuntimeError(f"Google request failed after retries: {last_exc}")

def geocode_candidates(query: str) -> list[dict]:
    data = google_get(
        "https://maps.googleapis.com/maps/api/geocode/json",
        {"address": query, "region": "gb", "key": GOOGLE_API_KEY},
    )
    return data.get("results", [])

def score_geocode_result(result: dict, clean_addr: str, postcode: str) -> float:
    score = 0.0
    formatted = safe_str(result.get("formatted_address", "")).upper()

    if not result.get("partial_match", False):
        score += 20

    types = set(result.get("types", []))
    if "street_address" in types:
        score += 20
    elif "premise" in types:
        score += 18
    elif "subpremise" in types:
        score += 16
    elif "route" in types:
        score += 12
    elif "postal_code" in types:
        score += 5

    if postcode and postcode.upper() in formatted:
        score += 25

    addr_words = [w for w in re.split(r"\W+", clean_addr.upper()) if len(w) >= 4]
    score += min(25, sum(1 for w in set(addr_words) if w in formatted) * 4)

    for c in result.get("address_components", []):
        if "country" in c.get("types", []) and c.get("short_name") == "GB":
            score += 5
            break

    loc_type = result.get("geometry", {}).get("location_type", "")
    if loc_type == "ROOFTOP":
        score += 10
    elif loc_type == "RANGE_INTERPOLATED":
        score += 4

    return score

def geocode_best(clean_addr: str, postcode: str) -> dict | None:
    postcode = normalize_postcode(postcode)
    cache_key = f"{clean_addr.upper()}|{postcode.upper()}"

    cached = geocode_cache.get(cache_key)
    if cached:
        return cached

    queries = []
    if clean_addr and postcode:
        queries.append(f"{clean_addr}, {postcode}, UK")
    if clean_addr:
        queries.append(f"{clean_addr}, UK")
    if postcode:
        queries.append(f"{postcode}, UK")

    best = None
    best_score = -1

    seen = set()
    for q in queries:
        q = re.sub(r"\s+", " ", q).strip()
        if not q or q in seen:
            continue
        seen.add(q)

        results = geocode_candidates(q)
        for r in results[:5]:
            score = score_geocode_result(r, clean_addr, postcode)
            loc = r["geometry"]["location"]
            candidate = {
                "lat": float(loc["lat"]),
                "lon": float(loc["lng"]),
                "formatted_address": r.get("formatted_address", q),
                "place_id": r.get("place_id", ""),
                "query": q,
                "score": round(score, 2),
                "location_type": r.get("geometry", {}).get("location_type", ""),
                "types": ", ".join(r.get("types", [])),
                "partial_match": bool(r.get("partial_match", False)),
            }
            if score > best_score:
                best = candidate
                best_score = score

    if best:
        geocode_cache[cache_key] = best

    return best

def directions_in_given_order(origin: str, destination: str, ordered_waypoints: list[str]) -> dict:
    params = {
        "origin": origin,
        "destination": destination,
        "mode": "driving",
        "region": "gb",
        "units": "metric",
        "key": GOOGLE_API_KEY,
    }
    if ordered_waypoints:
        params["waypoints"] = "|".join(ordered_waypoints)

    data = google_get("https://maps.googleapis.com/maps/api/directions/json", params)
    routes = data.get("routes", [])
    if not routes:
        raise RuntimeError("No route returned by Google Directions API")
    return routes[0]

def directions_optimize_waypoints(origin: str, destination: str, waypoints: list[str]) -> dict:
    wp_param = "optimize:true|" + "|".join(waypoints) if waypoints else ""
    params = {
        "origin": origin,
        "destination": destination,
        "mode": "driving",
        "region": "gb",
        "units": "metric",
        "key": GOOGLE_API_KEY,
    }
    if wp_param:
        params["waypoints"] = wp_param

    data = google_get("https://maps.googleapis.com/maps/api/directions/json", params)
    routes = data.get("routes", [])
    if not routes:
        raise RuntimeError("No optimized route returned by Google Directions API")
    return routes[0]

def route_metrics(route_json: dict) -> dict:
    total_m = 0.0
    total_s = 0.0
    route_points = []

    for leg in route_json.get("legs", []):
        total_m += float(leg["distance"]["value"])
        total_s += float(leg["duration"]["value"])

        for step in leg.get("steps", []):
            enc = step.get("polyline", {}).get("points", "")
            if not enc:
                continue
            pts = decode_polyline(enc)
            if route_points and pts:
                pts = pts[1:]
            route_points.extend(pts)

    return {
        "distance_km": total_m / 1000,
        "duration_min": total_s / 60,
        "route_points": route_points,
    }

def haversine_distance_m(lat1, lon1, lat2, lon2):
    r = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return int(2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a)))

def build_distance_matrix(depot_geo: dict, stops_df: pd.DataFrame) -> list[list[int]]:
    coords = [(depot_geo["lat"], depot_geo["lon"])] + list(zip(stops_df["Latitude"], stops_df["Longitude"]))
    matrix = []
    for lat1, lon1 in coords:
        row = []
        for lat2, lon2 in coords:
            row.append(haversine_distance_m(lat1, lon1, lat2, lon2))
        matrix.append(row)
    return matrix

def solve_tsp_with_ortools(distance_matrix: list[list[int]]) -> list[int]:
    n = len(distance_matrix)
    manager = pywrapcp.RoutingIndexManager(n, 1, 0)
    routing = pywrapcp.RoutingModel(manager)

    def distance_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return distance_matrix[from_node][to_node]

    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    search_parameters.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    search_parameters.time_limit.seconds = ORTOOLS_TIME_LIMIT_SECONDS

    solution = routing.SolveWithParameters(search_parameters)
    if solution is None:
        raise RuntimeError("OR-Tools could not find a route")

    route_nodes = []
    index = routing.Start(0)
    while not routing.IsEnd(index):
        node = manager.IndexToNode(index)
        route_nodes.append(node)
        index = solution.Value(routing.NextVar(index))
    route_nodes.append(manager.IndexToNode(index))

    stop_order = [node - 1 for node in route_nodes if node != 0]
    return stop_order

def load_input_file(input_path: str) -> pd.DataFrame:
    log.info("Loading file: %s", input_path)

    if input_path.lower().endswith(".csv"):
        df = pd.read_csv(input_path)
    else:
        df = pd.read_excel(input_path)

    required_cols = [
        "Route", DAY_COLUMN, "FullAddress", "Post Code", "Position",
        "Stop No", "Stop Name", "NA Name", "MStop Name"
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    return df

def get_available_routes_days(df: pd.DataFrame):
    routes = sorted(
        df["Route"]
        .apply(normalize_route_value)
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )

    days = sorted(
        df[DAY_COLUMN]
        .dropna()
        .astype(str)
        .str.strip()
        .str.upper()
        .unique()
        .tolist()
    )
    return routes, days

def build_route_subset(raw_df: pd.DataFrame, target_route, target_day: str) -> pd.DataFrame:
    target_route_norm = normalize_route_value(target_route)
    target_day_norm = safe_str(target_day).upper().strip()

    df = raw_df.copy()
    df["Route_Normalized"] = df["Route"].apply(normalize_route_value)
    df["Day_Normalized"] = df[DAY_COLUMN].astype(str).str.upper().str.strip()

    route_raw = df[
        (df["Route_Normalized"] == target_route_norm)
        & (df["Day_Normalized"] == target_day_norm)
    ].copy()

    if route_raw.empty:
        available = (
            df[["Route_Normalized", "Day_Normalized"]]
            .drop_duplicates()
            .sort_values(["Route_Normalized", "Day_Normalized"])
        )
        print("\nAvailable Route/Day combinations:")
        print(available.to_string(index=False))
        raise ValueError(
            f"No records found for Route={target_route_norm}, {DAY_COLUMN}={target_day_norm}"
        )

    route_raw = route_raw.reset_index(drop=True)
    route_raw["OriginalRowOrder"] = range(1, len(route_raw) + 1)
    route_raw["NormPostCode"] = route_raw["Post Code"].apply(normalize_postcode)
    route_raw["CleanAddress"] = route_raw["FullAddress"].apply(clean_address)

    pc_mode = (
        route_raw[route_raw["NormPostCode"] != ""]
        .groupby("CleanAddress")["NormPostCode"]
        .agg(lambda s: s.mode().iloc[0] if not s.mode().empty else s.iloc[0])
        .to_dict()
    )

    route_raw["ResolvedPostCode"] = route_raw.apply(
        lambda r: r["NormPostCode"] if r["NormPostCode"] else pc_mode.get(r["CleanAddress"], ""),
        axis=1,
    )

    route_raw["InitialStopKey"] = (
        route_raw["CleanAddress"].astype(str).str.strip()
        + " | " +
        route_raw["ResolvedPostCode"].astype(str).str.strip()
    )

    return route_raw

def build_unique_candidate_stops(route_raw: pd.DataFrame) -> pd.DataFrame:
    unique_stops = (
        route_raw.sort_values(["Position", "OriginalRowOrder"])
        .drop_duplicates(subset=["InitialStopKey"], keep="first")
        .copy()
        .reset_index(drop=True)
    )
    unique_stops["ManualOrder"] = range(1, len(unique_stops) + 1)
    return unique_stops

def physical_stop_key(row):
    if safe_str(row.get("PlaceId")):
        return f"PID|{row['PlaceId']}"
    lat = round(float(row["Latitude"]), 4)
    lon = round(float(row["Longitude"]), 4)
    pc = safe_str(row.get("ResolvedPostCode"))
    return f"COORD|{lat}|{lon}|{pc}"

def geocode_stops(unique_stops: pd.DataFrame):
    geocoded = []
    failed = []

    for _, row in unique_stops.iterrows():
        g = geocode_best(row["CleanAddress"], row["ResolvedPostCode"])
        if not g:
            failed.append({
                "ManualOrder": row["ManualOrder"],
                "NA Name": safe_str(row.get("NA Name")),
                "MStop Name": safe_str(row.get("MStop Name")),
                "Stop No": safe_str(row.get("Stop No")),
                "Stop Name": safe_str(row.get("Stop Name")),
                "CleanAddress": row["CleanAddress"],
                "ResolvedPostCode": row["ResolvedPostCode"],
            })
            continue

        rec = row.to_dict()
        rec.update({
            "Latitude": g["lat"],
            "Longitude": g["lon"],
            "PlaceId": g["place_id"],
            "GeocodeQuery": g["query"],
            "GeocodeScore": g["score"],
            "GeocodedAddress": g["formatted_address"],
            "LocationType": g.get("location_type", ""),
            "ResultTypes": g.get("types", ""),
            "PartialMatch": g.get("partial_match", False),
        })
        geocoded.append(rec)

    save_json_cache(GEOCODE_CACHE_PATH, geocode_cache)

    if not geocoded:
        raise ValueError("All stops failed geocoding")

    stops_df = pd.DataFrame(geocoded).copy()
    geocoded_source = stops_df.copy()

    stops_df["PhysicalStopKey"] = stops_df.apply(physical_stop_key, axis=1)

    stops_df = (
        stops_df.sort_values(["ManualOrder"])
        .drop_duplicates(subset=["PhysicalStopKey"], keep="first")
        .copy()
        .reset_index(drop=True)
    )

    stops_df["ManualOrder"] = range(1, len(stops_df) + 1)
    stops_df["NeedsReview"] = stops_df["GeocodeScore"] < GEOCODE_REVIEW_THRESHOLD
    stops_df["GeocodeQuality"] = stops_df["GeocodeScore"].apply(
        lambda s: "Good" if s >= GEOCODE_GOOD_THRESHOLD else ("Review" if s >= GEOCODE_REVIEW_THRESHOLD else "Poor")
    )

    failed_df = pd.DataFrame(failed)
    return stops_df, geocoded_source, failed_df

def map_route_rows_to_physical_stops(route_raw: pd.DataFrame, geocoded_source: pd.DataFrame, stops_df: pd.DataFrame):
    temp = geocoded_source.copy()
    temp["PhysicalStopKey"] = temp.apply(physical_stop_key, axis=1)
    tmp_map = dict(zip(temp["InitialStopKey"], temp["PhysicalStopKey"]))

    route_raw["PhysicalStopKey"] = route_raw["InitialStopKey"].map(tmp_map)
    route_raw["RowsAtSamePhysicalStop"] = route_raw.groupby("PhysicalStopKey")["PhysicalStopKey"].transform("size")
    stops_df["RowsAtSamePhysicalStop"] = stops_df["PhysicalStopKey"].map(route_raw.groupby("PhysicalStopKey").size())

    return route_raw, stops_df

def build_manual_waypoints(stops_df: pd.DataFrame) -> list[str]:
    waypoints = []
    for pid, addr, pc in zip(stops_df["PlaceId"], stops_df["CleanAddress"], stops_df["ResolvedPostCode"]):
        if safe_str(pid):
            waypoints.append(f"place_id:{pid}")
        else:
            waypoints.append(f"{addr}, {pc}, UK")
    return waypoints

def build_routes_and_metrics(stops_df: pd.DataFrame, depot_geo: dict):
    manual_waypoints = build_manual_waypoints(stops_df)

    origin = f"place_id:{depot_geo['place_id']}" if depot_geo.get("place_id") else DEPOT_ADDRESS
    destination = origin

    log.info("Requesting manual route in current order...")
    manual_route = directions_in_given_order(origin, destination, manual_waypoints)

    if len(stops_df) <= GOOGLE_OPTIMIZE_MAX_STOPS:
        log.info("Using Google waypoint optimization for %d stops...", len(stops_df))
        optimized_route = directions_optimize_waypoints(origin, destination, manual_waypoints)
        optimized_idx = optimized_route.get("waypoint_order", list(range(len(manual_waypoints))))
        if sorted(optimized_idx) != list(range(len(manual_waypoints))):
            raise RuntimeError(f"Unexpected Google waypoint_order: {optimized_idx}")
        optimization_method = "Google waypoint optimization"
    else:
        log.info("Using OR-Tools fallback for %d stops...", len(stops_df))
        distance_matrix = build_distance_matrix(depot_geo, stops_df)
        optimized_idx = solve_tsp_with_ortools(distance_matrix)
        optimization_method = "OR-Tools TSP fallback (haversine matrix)"
        ordered_waypoints = [manual_waypoints[i] for i in optimized_idx]
        optimized_route = directions_in_given_order(origin, destination, ordered_waypoints)

    stops_df["OptimizedOrder"] = 0
    for rank, idx in enumerate(optimized_idx, start=1):
        stops_df.loc[idx, "OptimizedOrder"] = rank

    manual_metrics = route_metrics(manual_route)
    optimized_metrics = route_metrics(optimized_route)

    return stops_df, manual_metrics, optimized_metrics, optimization_method

def build_output_frames(route_raw: pd.DataFrame, stops_df: pd.DataFrame):
    manual_order_map = dict(zip(stops_df["PhysicalStopKey"], stops_df["ManualOrder"]))
    optimized_order_map = dict(zip(stops_df["PhysicalStopKey"], stops_df["OptimizedOrder"]))

    route_raw["OldSequence"] = route_raw["PhysicalStopKey"].map(manual_order_map)
    route_raw["NewSequence"] = route_raw["PhysicalStopKey"].map(optimized_order_map)
    route_raw["SequenceChanged"] = route_raw["OldSequence"] != route_raw["NewSequence"]

    excel_out = route_raw[[
        "OriginalRowOrder",
        "Position",
        "OldSequence",
        "NewSequence",
        "SequenceChanged",
        "NA Name",
        "MStop Name",
        "Stop No",
        "Stop Name",
        "FullAddress",
        "Post Code",
        "CleanAddress",
        "ResolvedPostCode",
        "RowsAtSamePhysicalStop"
    ]].sort_values(["NewSequence", "Position", "OriginalRowOrder"]).copy()

    changed_only = excel_out[excel_out["SequenceChanged"] == True].copy()

    review_out = stops_df[[
        "ManualOrder",
        "OptimizedOrder",
        "NeedsReview",
        "GeocodeQuality",
        "GeocodeScore",
        "PartialMatch",
        "LocationType",
        "ResultTypes",
        "NA Name",
        "MStop Name",
        "Stop No",
        "Stop Name",
        "CleanAddress",
        "ResolvedPostCode",
        "GeocodedAddress",
        "RowsAtSamePhysicalStop"
    ]].sort_values(
        ["NeedsReview", "GeocodeScore", "OptimizedOrder"],
        ascending=[False, True, True]
    ).copy()

    failed_out = pd.DataFrame(columns=[
        "ManualOrder", "NA Name", "MStop Name", "Stop No", "Stop Name", "CleanAddress", "ResolvedPostCode"
    ])

    return route_raw, excel_out, changed_only, review_out, failed_out

def build_map(
    target_route,
    target_day: str,
    route_raw: pd.DataFrame,
    stops_df: pd.DataFrame,
    failed_df: pd.DataFrame,
    depot_geo: dict,
    manual_metrics: dict,
    optimized_metrics: dict,
    optimization_method: str,
    output_html_path: Path,
):
    manual_points = manual_metrics["route_points"]
    optimized_points = optimized_metrics["route_points"]

    center_lat = stops_df["Latitude"].mean() if not stops_df.empty else depot_geo["lat"]
    center_lon = stops_df["Longitude"].mean() if not stops_df.empty else depot_geo["lon"]

    m = folium.Map(location=[center_lat, center_lon], zoom_start=ZOOM_START, tiles="CartoDB positron")

    custom_css = """
    <style>
    .leaflet-tooltip.route-dark-tooltip {
        background: #111827 !important;
        color: #ffffff !important;
        border: 1px solid #ffffff !important;
        border-radius: 8px !important;
        box-shadow: 0 3px 12px rgba(0,0,0,0.35) !important;
        font-weight: 700 !important;
        padding: 8px 10px !important;
        max-width: 380px !important;
        white-space: normal !important;
        line-height: 1.35 !important;
    }
    .leaflet-tooltip.route-dark-tooltip:before {
        border-top-color: #111827 !important;
        border-bottom-color: #111827 !important;
    }
    .split-badge {
        display: inline-flex;
        align-items: stretch;
        border-radius: 10px;
        overflow: hidden;
        border: 2px solid #ffffff;
        box-shadow: 0 2px 8px rgba(0,0,0,0.35);
        white-space: nowrap;
        font-family: Arial, sans-serif;
        font-size: 12px;
        font-weight: 800;
        line-height: 1;
    }
    .split-badge .m {
        background: #c62828;
        color: #ffffff;
        padding: 6px 8px;
    }
    .split-badge .arrow {
        background: #111827;
        color: #ffffff;
        padding: 6px 6px;
    }
    .split-badge .g {
        background: #2e7d32;
        color: #ffffff;
        padding: 6px 8px;
    }
    .split-badge.same .g {
        background: #1565c0;
    }
    .split-badge.review .g {
        background: #ef6c00;
    }
    .legend-box {
        position: fixed;
        bottom: 24px;
        right: 24px;
        z-index: 9999;
        background: rgba(255,255,255,0.96);
        padding: 12px 14px;
        border-radius: 12px;
        box-shadow: 0 4px 16px rgba(0,0,0,0.22);
        font-family: Arial, sans-serif;
        font-size: 12px;
        min-width: 280px;
    }
    .stats-box {
        position: fixed;
        bottom: 24px;
        left: 24px;
        z-index: 9999;
        background: rgba(255,255,255,0.96);
        padding: 14px 18px;
        border-radius: 12px;
        box-shadow: 0 4px 16px rgba(0,0,0,0.22);
        font-family: Arial, sans-serif;
        font-size: 13px;
        min-width: 400px;
    }
    .swatch {
        display:inline-block;
        width:14px;
        height:14px;
        border-radius:3px;
        margin-right:8px;
        vertical-align:middle;
    }
    </style>
    """
    m.get_root().header.add_child(Element(custom_css))

    manual_fg = folium.FeatureGroup(name="Manual route", show=True)
    optimized_fg = folium.FeatureGroup(name="Optimized route", show=True)
    animated_fg = folium.FeatureGroup(name="Animated optimized route", show=False)
    changed_fg = folium.FeatureGroup(name="Changed stops", show=True)
    unchanged_fg = folium.FeatureGroup(name="Unchanged stops", show=False)
    review_fg = folium.FeatureGroup(name="Review-needed stops", show=True)
    depot_fg = folium.FeatureGroup(name="Depot", show=True)

    if manual_points:
        folium.PolyLine(
            manual_points,
            color="#d32f2f",
            weight=5,
            opacity=0.85,
            tooltip=folium.Tooltip("Manual route", sticky=True, class_name="route-dark-tooltip"),
        ).add_to(manual_fg)

    if optimized_points:
        folium.PolyLine(
            optimized_points,
            color="#2e7d32",
            weight=5,
            opacity=0.92,
            tooltip=folium.Tooltip("Optimized route", sticky=True, class_name="route-dark-tooltip"),
        ).add_to(optimized_fg)

    if SHOW_ANT_PATH and optimized_points:
        AntPath(
            locations=optimized_points,
            color="#16a34a",
            weight=7,
            opacity=0.75,
            delay=900,
            dash_array=[18, 24],
            pulse_color="#bbf7d0",
            tooltip=folium.Tooltip("Animated optimized route", sticky=True, class_name="route-dark-tooltip"),
        ).add_to(animated_fg)

    folium.Marker(
        [depot_geo["lat"], depot_geo["lon"]],
        popup=f"<b>{DEPOT_NAME}</b><br>{html_escape(depot_geo['formatted_address'])}",
        tooltip=folium.Tooltip(DEPOT_NAME, sticky=True, class_name="route-dark-tooltip"),
        icon=folium.Icon(color="blue", icon="home", prefix="fa"),
    ).add_to(depot_fg)

    for _, row in stops_df.iterrows():
        old_seq = safe_int(row["ManualOrder"])
        new_seq = safe_int(row["OptimizedOrder"])
        changed = old_seq != new_seq
        needs_review = bool(row["NeedsReview"])
        review_flag = "Yes" if needs_review else "No"

        badge_class = "split-badge"
        if not changed:
            badge_class += " same"
        elif needs_review:
            badge_class += " review"

        html = f"""
        <div class="{badge_class}">
            <span class="m">O{old_seq}</span>
            <span class="arrow">→</span>
            <span class="g">N{new_seq}</span>
        </div>
        """

        popup = f"""
        <div style="font-family:Arial,sans-serif;font-size:13px;line-height:1.45;">
            <div style="font-size:15px;font-weight:700;margin-bottom:8px;">Old {old_seq} → New {new_seq}</div>
            <b>Customer:</b> {html_escape(row['NA Name'])}<br>
            <b>Master stop:</b> {html_escape(row['MStop Name'])}<br>
            <b>Stop No:</b> {html_escape(row['Stop No'])}<br>
            <b>Stop Name:</b> {html_escape(row['Stop Name'])}<br>
            <b>Changed:</b> {"Yes" if changed else "No"}<br>
            <b>Needs review:</b> {review_flag}<br>
            <b>Geocode quality:</b> {html_escape(row['GeocodeQuality'])}<br>
            <b>Rows at this stop:</b> {safe_int(row['RowsAtSamePhysicalStop'])}<br>
            <b>Postcode:</b> {html_escape(row['ResolvedPostCode'])}<br><br>
            <b>Input address:</b><br>{html_escape(row['CleanAddress'])}<br><br>
            <b>Matched address:</b><br>{html_escape(row['GeocodedAddress'])}<br>
            <b>Geocode score:</b> {row['GeocodeScore']}<br>
            <b>Location type:</b> {html_escape(row['LocationType'])}<br>
            <b>Partial match:</b> {html_escape(str(row['PartialMatch']))}
        </div>
        """

        tooltip = f"{html_escape(row['NA Name'])}<br>Old {old_seq} → New {new_seq}"

        marker = folium.Marker(
            [row["Latitude"], row["Longitude"]],
            tooltip=folium.Tooltip(tooltip, sticky=True, class_name="route-dark-tooltip"),
            popup=folium.Popup(popup, max_width=440),
            icon=folium.DivIcon(html=html),
        )

        if changed:
            marker.add_to(changed_fg)
        else:
            marker.add_to(unchanged_fg)

        if needs_review:
            folium.CircleMarker(
                location=[row["Latitude"], row["Longitude"]],
                radius=16,
                color="#ef6c00",
                fill=False,
                weight=3,
                opacity=0.9,
                tooltip=folium.Tooltip("Review-needed stop", sticky=True, class_name="route-dark-tooltip"),
            ).add_to(review_fg)

    manual_fg.add_to(m)
    optimized_fg.add_to(m)
    if SHOW_ANT_PATH:
        animated_fg.add_to(m)
    changed_fg.add_to(m)
    unchanged_fg.add_to(m)
    review_fg.add_to(m)
    depot_fg.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)

    changed_count = int((stops_df["ManualOrder"] != stops_df["OptimizedOrder"]).sum())
    review_count = int(stops_df["NeedsReview"].sum())

    distance_saved_km = manual_metrics["distance_km"] - optimized_metrics["distance_km"]
    time_saved_min = manual_metrics["duration_min"] - optimized_metrics["duration_min"]
    distance_saved_pct = (
        (distance_saved_km / manual_metrics["distance_km"] * 100)
        if manual_metrics["distance_km"] else 0
    )

    manual_miles = km_to_miles(manual_metrics["distance_km"])
    optimized_miles = km_to_miles(optimized_metrics["distance_km"])
    saved_miles = km_to_miles(distance_saved_km)

    fuel_saved_l = litres_saved_from_km(distance_saved_km)
    cost_saved_gbp = fuel_saved_l * FUEL_PRICE_GBP
    co2_saved_kg = fuel_saved_l * CO2_PER_LITRE

    legend_html = """
    <div class="legend-box">
        <div style="font-size:14px;font-weight:700;margin-bottom:8px;">Map Guide</div>
        <div style="margin:6px 0;"><span class="swatch" style="background:#d32f2f;"></span>Manual route</div>
        <div style="margin:6px 0;"><span class="swatch" style="background:#2e7d32;"></span>Optimized route</div>
        <div style="margin:6px 0;"><span style="background:#c62828;color:#fff;padding:3px 6px;border-radius:6px;font-weight:700;">O</span> old sequence</div>
        <div style="margin:6px 0;"><span style="background:#2e7d32;color:#fff;padding:3px 6px;border-radius:6px;font-weight:700;">N</span> new sequence</div>
        <div style="margin:6px 0;"><span style="background:#1565c0;color:#fff;padding:3px 6px;border-radius:6px;font-weight:700;">Blue N</span> unchanged</div>
        <div style="margin:6px 0;"><span style="background:#ef6c00;color:#fff;padding:3px 6px;border-radius:6px;font-weight:700;">Orange Ring</span> review-needed</div>
    </div>
    """
    m.get_root().html.add_child(Element(legend_html))

    stats_html = f"""
    <div class="stats-box">
        <div style="font-size:16px; font-weight:700; margin-bottom:6px;">Route {html_escape(str(target_route))} · {target_day}</div>
        <div style="margin-bottom:8px;"><b>Method:</b> {html_escape(optimization_method)}</div>
        <table style="width:100%; border-collapse:collapse;">
            <tr><td>Service rows</td><td align="right"><b>{len(route_raw)}</b></td></tr>
            <tr><td>Unique physical stops</td><td align="right"><b>{len(stops_df)}</b></td></tr>
            <tr><td>Changed stops</td><td align="right"><b>{changed_count}</b></td></tr>
            <tr><td>Review-needed stops</td><td align="right"><b>{review_count}</b></td></tr>
            <tr><td>Geocode failures</td><td align="right"><b>{len(failed_df)}</b></td></tr>
            <tr><td colspan="2"><hr></td></tr>
            <tr style="color:#d32f2f"><td>Manual distance</td><td align="right"><b>{manual_miles:.1f} miles</b></td></tr>
            <tr style="color:#2e7d32"><td>Optimized distance</td><td align="right"><b>{optimized_miles:.1f} miles</b></td></tr>
            <tr><td>Distance saved</td><td align="right"><b>{saved_miles:.1f} miles ({distance_saved_pct:.1f}%)</b></td></tr>
            <tr><td colspan="2"><hr></td></tr>
            <tr style="color:#d32f2f"><td>Manual time</td><td align="right"><b>{minutes_to_hhmm(manual_metrics['duration_min'])}</b></td></tr>
            <tr style="color:#2e7d32"><td>Optimized time</td><td align="right"><b>{minutes_to_hhmm(optimized_metrics['duration_min'])}</b></td></tr>
            <tr><td>Time saved</td><td align="right"><b>{minutes_to_hhmm(time_saved_min)}</b></td></tr>
            <tr><td colspan="2"><hr></td></tr>
            <tr><td>Fuel saved</td><td align="right"><b>{fuel_saved_l:.2f} L</b></td></tr>
            <tr><td>Cost saved</td><td align="right"><b>£{cost_saved_gbp:.2f}</b></td></tr>
            <tr><td>CO₂ saved</td><td align="right"><b>{co2_saved_kg:.2f} kg</b></td></tr>
        </table>
    </div>
    """
    m.get_root().html.add_child(Element(stats_html))

    m.save(str(output_html_path))

def run_optimizer(input_path: str, target_route, target_day: str):
    start_time = time.time()

    raw_df = load_input_file(input_path)
    route_raw = build_route_subset(raw_df, target_route, target_day)
    unique_stops = build_unique_candidate_stops(route_raw)

    depot_geo = geocode_best(DEPOT_ADDRESS, "")
    if not depot_geo:
        raise ValueError("Could not geocode depot")

    stops_df, geocoded_source, failed_df = geocode_stops(unique_stops)
    route_raw, stops_df = map_route_rows_to_physical_stops(route_raw, geocoded_source, stops_df)

    stops_df, manual_metrics, optimized_metrics, optimization_method = build_routes_and_metrics(
        stops_df, depot_geo
    )

    route_raw, excel_out, changed_only, review_out, _ = build_output_frames(route_raw, stops_df)

    run_id = uuid.uuid4().hex[:10]
    day_slug = slugify(target_day)
    route_slug = slugify(normalize_route_value(target_route))

    excel_name = f"{run_id}_route{route_slug}_{day_slug}.xlsx"
    csv_name = f"{run_id}_route{route_slug}_{day_slug}_changed.csv"
    review_csv_name = f"{run_id}_route{route_slug}_{day_slug}_geocode_review.csv"
    failed_csv_name = f"{run_id}_route{route_slug}_{day_slug}_failed_geocodes.csv"
    map_name = f"{run_id}_route{route_slug}_{day_slug}_map.html"

    xlsx_path = OUTPUT_DIR / excel_name
    csv_path = OUTPUT_DIR / csv_name
    review_csv_path = OUTPUT_DIR / review_csv_name
    failed_csv_path = OUTPUT_DIR / failed_csv_name
    map_path = OUTPUT_DIR / map_name

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        excel_out.to_excel(writer, index=False, sheet_name="Old_vs_New_Sequence")
        changed_only.to_excel(writer, index=False, sheet_name="Changed_Only")
        review_out.to_excel(writer, index=False, sheet_name="Geocode_Review")
        failed_df.to_excel(writer, index=False, sheet_name="Failed_Geocodes")
        route_raw.to_excel(writer, index=False, sheet_name="Raw_Filtered_Route")

    changed_only.to_csv(csv_path, index=False)
    review_out.to_csv(review_csv_path, index=False)
    failed_df.to_csv(failed_csv_path, index=False)

    build_map(
        target_route=normalize_route_value(target_route),
        target_day=target_day,
        route_raw=route_raw,
        stops_df=stops_df,
        failed_df=failed_df,
        depot_geo=depot_geo,
        manual_metrics=manual_metrics,
        optimized_metrics=optimized_metrics,
        optimization_method=optimization_method,
        output_html_path=map_path,
    )

    distance_saved_km = manual_metrics["distance_km"] - optimized_metrics["distance_km"]
    time_saved_min = manual_metrics["duration_min"] - optimized_metrics["duration_min"]
    distance_saved_pct = (
        (distance_saved_km / manual_metrics["distance_km"] * 100)
        if manual_metrics["distance_km"] else 0
    )

    manual_miles = km_to_miles(manual_metrics["distance_km"])
    optimized_miles = km_to_miles(optimized_metrics["distance_km"])
    saved_miles = km_to_miles(distance_saved_km)

    fuel_saved_l = litres_saved_from_km(distance_saved_km)
    cost_saved_gbp = fuel_saved_l * FUEL_PRICE_GBP
    co2_saved_kg = fuel_saved_l * CO2_PER_LITRE

    changed_count = int((stops_df["ManualOrder"] != stops_df["OptimizedOrder"]).sum())
    changed_pct = (changed_count / len(stops_df) * 100) if len(stops_df) else 0

    result = {
        "route": normalize_route_value(target_route),
        "day": target_day,
        "rows": len(route_raw),
        "stops": len(stops_df),
        "changed_stops": changed_count,
        "changed_pct": f"{changed_pct:.1f}%",
        "review_needed": int(stops_df["NeedsReview"].sum()),
        "failures": len(failed_df),
        "manual_miles": f"{manual_miles:.2f}",
        "optimized_miles": f"{optimized_miles:.2f}",
        "saved_miles": f"{saved_miles:.2f}",
        "saved_pct": f"{distance_saved_pct:.2f}%",
        "manual_time": minutes_to_hhmm(manual_metrics["duration_min"]),
        "optimized_time": minutes_to_hhmm(optimized_metrics["duration_min"]),
        "time_saved": minutes_to_hhmm(time_saved_min),
        "fuel_saved_l": f"{fuel_saved_l:.2f}",
        "cost_saved_gbp": f"{cost_saved_gbp:.2f}",
        "co2_saved_kg": f"{co2_saved_kg:.2f}",
        "method": optimization_method,
        "excel_file": excel_name,
        "csv_file": csv_name,
        "review_csv_file": review_csv_name,
        "failed_csv_file": failed_csv_name,
        "map_file": map_name,
        "runtime_sec": f"{time.time() - start_time:.2f}",
        "input_file": input_path,
        "depot_name": DEPOT_NAME,
    }

    review_rows = review_out.fillna("").to_dict(orient="records")
    review_columns = list(review_out.columns)

    failed_rows = failed_df.fillna("").to_dict(orient="records") if not failed_df.empty else []
    failed_columns = list(failed_df.columns) if not failed_df.empty else []

    return result, review_rows, review_columns, failed_rows, failed_columns

@app.route("/", methods=["GET"])
def index():
    try:
        df = load_input_file(INPUT_PATH)
        routes, days = get_available_routes_days(df)
    except Exception as exc:
        flash(f"Could not load fixed file: {exc}")
        routes, days = [], []

    content = render_template_string(
        INDEX_CONTENT,
        input_path=INPUT_PATH,
        depot_name=DEPOT_NAME,
        day_column=DAY_COLUMN,
        routes=routes,
        days=days,
    )
    return render_template_string(BASE_TEMPLATE, content=content)

@app.route("/run", methods=["POST"])
def run():
    target_route = request.form.get("route", "").strip()
    target_day = request.form.get("day", "").strip().upper()

    try:
        result, review_rows, review_columns, failed_rows, failed_columns = run_optimizer(
            input_path=INPUT_PATH,
            target_route=target_route,
            target_day=target_day,
        )
    except Exception as exc:
        flash(f"Optimization failed: {exc}")
        return redirect(url_for("index"))

    content = render_template_string(
        RESULT_CONTENT,
        result=result,
        review_rows=review_rows,
        review_columns=review_columns,
        review_rows_count=len(review_rows),
        failed_rows=failed_rows,
        failed_columns=failed_columns,
        failed_rows_count=len(failed_rows),
    )
    return render_template_string(BASE_TEMPLATE, content=content)

@app.route("/download/<path:filename>")
def download(filename):
    return send_from_directory(OUTPUT_DIR, filename, as_attachment=False)

if __name__ == "__main__":
    app.run(debug=True)
