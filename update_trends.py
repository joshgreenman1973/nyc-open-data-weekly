#!/usr/bin/env python3
"""
Rebuild weekly-trends.csv from NYC Open Data and inject it into dashboard.html.

Weeks run Monday through Sunday; week_ending is the Sunday. Every run
recomputes the last RECOUNT_WEEKS weeks (agencies publish late records, so a
week's counts keep growing for a while) and adds any weeks that are missing,
back to START. Counts use SoQL aggregates, never row downloads, so no week is
silently capped at a $limit.

Two numbers describe the state of things on the day of the run rather than
the week itself: hpd_open_pct (share of that week's complaints still open)
and vacate_orders_active (all vacate orders citywide not yet rescinded). They
are recorded once, for the newest week, and never recomputed for older weeks.

A source that has not published through a week's end yet gives null for that
week ("no data" on the dashboard) instead of a partial count. The run fails
if every source comes back empty, so a broken pipeline can't look like a
quiet week.

Usage: python3 update_trends.py
"""
import csv, json, os, re, sys, time
import urllib.parse, urllib.request
from datetime import date, datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(HERE, "weekly-trends.csv")
DASH_PATH = os.path.join(HERE, "dashboard.html")
BASE = "https://data.cityofnewyork.us/resource"
START = date(2026, 1, 4)          # first week ending tracked (a Sunday)
RECOUNT_WEEKS = 6

COLUMNS = ["week_ending", "crashes", "persons_injured", "persons_killed",
           "pedestrians_injured", "pedestrians_killed", "cyclists_injured",
           "shooting_incidents", "shooting_victims", "shooting_murders",
           "hpd_complaints", "hpd_heat_hot_water", "hpd_heat_pct", "hpd_bronx_pct",
           "hpd_open_pct", "hpd_immediate_emergency", "vacate_orders_new",
           "vacate_orders_active", "restaurant_inspections", "critical_violations",
           "restaurant_closures", "dob_filings", "dob_large_projects_over_1m"]
SNAPSHOT_COLS = {"hpd_open_pct", "vacate_orders_active"}


def soql(dataset, **params):
    url = f"{BASE}/{dataset}.json?" + urllib.parse.urlencode({f"${k}": v for k, v in params.items()})
    headers = {"User-Agent": "nyc-open-data-weekly"}
    if os.environ.get("SOCRATA_APP_TOKEN"):
        headers["X-App-Token"] = os.environ["SOCRATA_APP_TOKEN"]
    for attempt in range(5):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=120) as r:
                return json.load(r)
        except Exception as e:
            bad_query = getattr(e, "code", 0) == 400
            if attempt == 4 or bad_query:
                raise RuntimeError(f"{dataset}: {e}") from e
            time.sleep(5 * (attempt + 1))


def num(rows, key="n"):
    v = rows[0].get(key) if rows else None
    return int(float(v)) if v not in (None, "") else 0


def latest_date(dataset, field):
    rows = soql(dataset, select=f"max({field}) as mx", where=f"{field} <= '{date.today().isoformat()}'")
    return date.fromisoformat(rows[0]["mx"][:10]) if rows and rows[0].get("mx") else None


def between(field, start, end):
    return f"{field} >= '{start.isoformat()}' AND {field} < '{(end + timedelta(days=1)).isoformat()}'"


def crashes(start, end):
    w = between("crash_date", start, end)
    r = soql("h9gi-nx95", where=w, select=(
        "count(*) as n, sum(number_of_persons_injured) as inj, sum(number_of_persons_killed) as kil, "
        "sum(number_of_pedestrians_injured) as pinj, sum(number_of_pedestrians_killed) as pkil, "
        "sum(number_of_cyclist_injured) as cinj"))
    return {"crashes": num(r), "persons_injured": num(r, "inj"), "persons_killed": num(r, "kil"),
            "pedestrians_injured": num(r, "pinj"), "pedestrians_killed": num(r, "pkil"),
            "cyclists_injured": num(r, "cinj")}


def shootings(start, end):
    # Incidents carry the date; victims (one row each, with the murder flag)
    # live in a separate dataset joined on incident_key.
    keys = [r["incident_key"] for r in soql("5ucz-vwe8", where=between("occur_date", start, end),
                                             select="incident_key", group="incident_key", limit=5000)]
    if not keys:
        return {"shooting_incidents": 0, "shooting_victims": 0, "shooting_murders": 0}
    inlist = "incident_key in (" + ",".join(f"'{k}'" for k in keys) + ")"
    victims = num(soql("pztn-9bne", where=inlist, select="count(*) as n"))
    murders = num(soql("pztn-9bne", where=inlist + " AND upper(stat_murder_flg) in ('Y','TRUE')", select="count(*) as n"))
    return {"shooting_incidents": len(keys), "shooting_victims": victims, "shooting_murders": murders}


def hpd(start, end, snapshot):
    # Rows are problems; a complaint can list several. Count complaints.
    w = between("received_date", start, end)
    def complaints(extra=""):
        return len(soql("ygpa-z7cr", where=w + extra, select="complaint_id", group="complaint_id", limit=50000))
    total = complaints()
    heat = complaints(" AND major_category = 'HEAT/HOT WATER'")
    bronx = complaints(" AND borough = 'BRONX'")
    emergency = complaints(" AND type = 'IMMEDIATE EMERGENCY'")
    out = {"hpd_complaints": total, "hpd_heat_hot_water": heat,
           "hpd_heat_pct": round(100 * heat / total, 1) if total else None,
           "hpd_bronx_pct": round(100 * bronx / total, 1) if total else None,
           "hpd_immediate_emergency": emergency}
    if snapshot:
        still_open = complaints(" AND complaint_status = 'OPEN'")
        out["hpd_open_pct"] = round(100 * still_open / total, 1) if total else None
    return out


def vacates(start, end, snapshot):
    out = {"vacate_orders_new": num(soql("tb8q-a3ar", where=between("vacate_effective_date", start, end), select="count(*) as n"))}
    if snapshot:
        out["vacate_orders_active"] = num(soql("tb8q-a3ar", select="count(*) as n",
            where=f"actual_rescind_date IS NULL AND vacate_effective_date <= '{date.today().isoformat()}'"))
    return out


def restaurants(start, end):
    # Rows are violations; an inspection is one restaurant (camis) on one date.
    w = between("inspection_date", start, end)
    insp = len(soql("43nn-pn8j", where=w, select="camis, inspection_date", group="camis, inspection_date", limit=50000))
    crit = num(soql("43nn-pn8j", where=w + " AND critical_flag = 'Critical'", select="count(*) as n"))
    closed = len(soql("43nn-pn8j", where=w + " AND upper(action) like '%CLOSED%'", select="camis", group="camis", limit=5000))
    return {"restaurant_inspections": insp, "critical_violations": crit, "restaurant_closures": closed}


def dob(start, end):
    w = between("filing_date", start, end)
    filings = num(soql("w9ak-ipjd", where=w, select="count(*) as n"))
    large = num(soql("w9ak-ipjd", where=w + " AND initial_cost::number > 1000000", select="count(*) as n"))  # stored as text
    return {"dob_filings": filings, "dob_large_projects_over_1m": large}


SOURCES = [  # (fetcher, dataset, date field, takes snapshot flag)
    (crashes, "h9gi-nx95", "crash_date", False),
    (shootings, "5ucz-vwe8", "occur_date", False),
    (hpd, "ygpa-z7cr", "received_date", True),
    (vacates, "tb8q-a3ar", "vacate_effective_date", True),
    (restaurants, "43nn-pn8j", "inspection_date", False),
    (dob, "w9ak-ipjd", "filing_date", False),
]


def load_rows():
    if not os.path.exists(CSV_PATH):
        return {}
    with open(CSV_PATH, newline="") as f:
        return {r["week_ending"]: r for r in csv.DictReader(f)}


def main():
    today = date.today()
    last_sunday = today - timedelta(days=(today.weekday() + 1) % 7 or 7)
    weeks = []
    d = START
    while d <= last_sunday:
        weeks.append(d)
        d += timedelta(days=7)

    old = load_rows()
    # Drop rows from before the Monday-Sunday rebuild (any week_ending not a tracked Sunday).
    rows = {k: v for k, v in old.items() if k in {w.isoformat() for w in weeks}}
    todo = [w for w in weeks if w.isoformat() not in rows or w > last_sunday - timedelta(days=7 * RECOUNT_WEEKS)]

    published = {ds: latest_date(ds, field) for _, ds, field, _ in SOURCES}
    print("latest published:", {k: str(v) for k, v in published.items()})
    if not any(published.values()):
        sys.exit("FAIL: no source returned a latest date")

    filled = 0
    for wk in todo:
        start = wk - timedelta(days=6)
        prev = rows.get(wk.isoformat(), {})
        row = {c: prev.get(c, "") for c in COLUMNS}
        row["week_ending"] = wk.isoformat()
        for fetch, ds, _, takes_snapshot in SOURCES:
            if not published[ds] or published[ds] < wk:
                continue  # source hasn't published through this week yet
            args = (start, wk, wk == last_sunday) if takes_snapshot else (start, wk)
            vals = fetch(*args)
            for k, v in vals.items():
                if k in SNAPSHOT_COLS and wk != last_sunday:
                    continue
                row[k] = "" if v is None else v
            filled += 1
        rows[wk.isoformat()] = row
        print(wk, {k: row[k] for k in ("crashes", "hpd_complaints", "restaurant_inspections", "dob_filings")})

    if filled == 0:
        sys.exit("FAIL: no source had data for any week; refusing to write an empty update")

    ordered = [rows[k] for k in sorted(rows)]
    with open(CSV_PATH, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(ordered)

    def typed(r):
        out = {}
        for c in COLUMNS:
            v = r.get(c, "")
            out[c] = v if c == "week_ending" else (None if v in ("", None) else (float(v) if "." in str(v) else int(v)))
        return out
    data = json.dumps([typed(r) for r in ordered], separators=(",", ":"))
    html = open(DASH_PATH).read()
    html, n = re.subn(r"const TRENDS_DATA = \[.*?\];", lambda m: f"const TRENDS_DATA = {data};", html, count=1, flags=re.S)
    if n != 1:
        sys.exit("FAIL: TRENDS_DATA line not found in dashboard.html")
    html = re.sub(r'(<span id="updated">)[^<]*(</span>)', lambda m: m.group(1) + today.strftime("%B %-d, %Y") + m.group(2), html)
    open(DASH_PATH, "w").write(html)
    print(f"wrote {len(ordered)} weeks through {ordered[-1]['week_ending']}")


if __name__ == "__main__":
    main()
