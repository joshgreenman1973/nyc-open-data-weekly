#!/usr/bin/env python3
"""
NYC Open Data Weekly Scanner
Fetches recently updated datasets and key metrics from NYC Open Data (Socrata API).
Run weekly to generate a consolidated insights report.
"""

import requests
import json
from datetime import datetime, timedelta
from collections import Counter

BASE = "https://data.cityofnewyork.us/resource"
CATALOG = "https://api.us.socrata.com/api/catalog/v1"

def fetch_json(url, params=None):
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def get_updated_datasets(days=7, limit=200):
    """Get all datasets updated in the past N days."""
    data = fetch_json(CATALOG, {
        "domains": "data.cityofnewyork.us",
        "order": "updatedAt DESC",
        "limit": limit,
        "only": "datasets",
        "provenance": "official"
    })
    cutoff = datetime.utcnow() - timedelta(days=days)
    recent = []
    for r in data.get("results", []):
        updated = r["resource"].get("updatedAt", "")
        if updated:
            dt = datetime.fromisoformat(updated.replace("Z", "+00:00")).replace(tzinfo=None)
            if dt > cutoff:
                agency = ""
                for m in r.get("classification", {}).get("domain_metadata", []):
                    if m.get("key") == "Dataset-Information_Agency":
                        agency = m["value"]
                recent.append({
                    "name": r["resource"]["name"],
                    "id": r["resource"]["id"],
                    "updated": updated,
                    "category": r.get("classification", {}).get("domain_category", ""),
                    "agency": agency,
                    "description": r["resource"].get("description", "")[:120],
                    "views_last_week": r["resource"].get("page_views", {}).get("page_views_last_week", 0),
                })
    return recent

# --- Individual dataset scanners ---

def scan_collisions(days=7):
    """NYPD Motor Vehicle Collisions - Crashes (h9gi-nx95)"""
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = fetch_json(f"{BASE}/h9gi-nx95.json", {
        "$where": f"crash_date>='{since}'",
        "$limit": 5000
    })
    if not rows:
        return None
    total = len(rows)
    injured = sum(int(r.get("number_of_persons_injured", 0)) for r in rows)
    killed = sum(int(r.get("number_of_persons_killed", 0)) for r in rows)
    ped_injured = sum(int(r.get("number_of_pedestrians_injured", 0)) for r in rows)
    ped_killed = sum(int(r.get("number_of_pedestrians_killed", 0)) for r in rows)
    cyc_injured = sum(int(r.get("number_of_cyclist_injured", 0)) for r in rows)
    boroughs = Counter(r.get("borough", "UNSPECIFIED") for r in rows)
    factors = Counter(r.get("contributing_factor_vehicle_1", "Unspecified") for r in rows)
    dates = sorted(set(r.get("crash_date", "")[:10] for r in rows))
    return {
        "total_crashes": total,
        "date_range": f"{dates[0]} to {dates[-1]}" if dates else "",
        "persons_injured": injured,
        "persons_killed": killed,
        "pedestrians_injured": ped_injured,
        "pedestrians_killed": ped_killed,
        "cyclists_injured": cyc_injured,
        "borough_breakdown": dict(boroughs.most_common()),
        "top_factors": dict(factors.most_common(10)),
    }

def scan_hpd_complaints(days=7):
    """HPD Housing Maintenance Complaints (ygpa-z7cr)"""
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = fetch_json(f"{BASE}/ygpa-z7cr.json", {
        "$where": f"receiveddate>='{since}'",
        "$select": "count(*) as total",
    })
    total = int(rows[0]["total"]) if rows else 0
    # Get complaint type breakdown
    types = fetch_json(f"{BASE}/ygpa-z7cr.json", {
        "$where": f"receiveddate>='{since}'",
        "$select": "majorcategory, count(*) as cnt",
        "$group": "majorcategory",
        "$order": "cnt DESC",
        "$limit": 10,
    })
    boroughs = fetch_json(f"{BASE}/ygpa-z7cr.json", {
        "$where": f"receiveddate>='{since}'",
        "$select": "borough, count(*) as cnt",
        "$group": "borough",
        "$order": "cnt DESC",
    })
    return {
        "total_complaints": total,
        "top_categories": {r["majorcategory"]: int(r["cnt"]) for r in types},
        "borough_breakdown": {r["borough"]: int(r["cnt"]) for r in boroughs},
    }

def scan_hpd_vacates(days=7):
    """HPD Order to Repair/Vacate (tb8q-a3ar)"""
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = fetch_json(f"{BASE}/tb8q-a3ar.json", {
        "$where": f"vacate_effective_date>='{since}'",
        "$order": "vacate_effective_date DESC",
        "$limit": 200,
    })
    active = [r for r in rows if not r.get("actual_rescind_date")]
    reasons = Counter(r.get("primary_vacate_reason", "") for r in rows)
    boroughs = Counter(r.get("boro_short_name", "") for r in rows)
    return {
        "new_orders": len(rows),
        "still_active": len(active),
        "reasons": dict(reasons.most_common()),
        "boroughs": dict(boroughs.most_common()),
        "details": [{
            "address": f"{r.get('house_number', '')} {r.get('street_name', '')}",
            "borough": r.get("boro_short_name", ""),
            "reason": r.get("primary_vacate_reason", ""),
            "type": r.get("vacate_type", ""),
            "units": r.get("number_of_vacated_units", ""),
            "date": r.get("vacate_effective_date", "")[:10],
        } for r in active[:10]],
    }

def scan_restaurant_inspections(days=7):
    """DOHMH Restaurant Inspections (43nn-pn8j)"""
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = fetch_json(f"{BASE}/43nn-pn8j.json", {
        "$where": f"inspection_date>='{since}'",
        "$limit": 5000,
    })
    grades = Counter(r.get("grade", "No Grade") for r in rows)
    critical = [r for r in rows if r.get("critical_flag") == "Critical"]
    boroughs = Counter(r.get("boro", "") for r in rows)
    violations = Counter(r.get("violation_code", "") for r in rows)
    return {
        "total_inspections": len(rows),
        "grade_distribution": dict(grades.most_common()),
        "critical_violations": len(critical),
        "borough_breakdown": dict(boroughs.most_common()),
        "top_violations": dict(violations.most_common(10)),
    }

def scan_shootings(days=7):
    """NYPD Shooting Incidents (5ucz-vwe8), Victims (pztn-9bne), Offenders (gdk4-mbsv)"""
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    incidents = fetch_json(f"{BASE}/5ucz-vwe8.json", {
        "$where": f"occur_date>='{since}'",
        "$limit": 5000,
    })
    if not incidents:
        return None
    total = len(incidents)
    boroughs = Counter(r.get("boro", "UNKNOWN") for r in incidents)
    precincts = Counter(r.get("precinct", "") for r in incidents)
    dates = sorted(set(r.get("occur_date", "")[:10] for r in incidents))
    # Victims
    keys = [r["incident_key"] for r in incidents if "incident_key" in r]
    victims = []
    if keys:
        # Fetch victims for this period
        victims = fetch_json(f"{BASE}/pztn-9bne.json", {
            "$where": f"incident_key in({','.join(repr(k) for k in keys[:200])})",
            "$limit": 5000,
        })
    murders = sum(1 for v in victims if v.get("stat_murder_flg") == "Y")
    victim_ages = Counter(v.get("victim_age_group", "UNKNOWN") for v in victims)
    return {
        "total_incidents": total,
        "date_range": f"{dates[0]} to {dates[-1]}" if dates else "",
        "total_victims": len(victims),
        "murders": murders,
        "borough_breakdown": dict(boroughs.most_common()),
        "top_precincts": dict(precincts.most_common(10)),
        "victim_age_groups": dict(victim_ages.most_common()),
    }

def scan_dob_filings(days=7):
    """DOB NOW Job Application Filings (w9ak-ipjd)"""
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = fetch_json(f"{BASE}/w9ak-ipjd.json", {
        "$where": f"filing_date>='{since}'",
        "$limit": 2000,
    })
    job_types = Counter(r.get("job_type", "") for r in rows)
    boroughs = Counter(r.get("borough", "") for r in rows)
    big_projects = [r for r in rows if float(r.get("initial_cost", 0) or 0) > 1_000_000]
    return {
        "total_filings": len(rows),
        "job_types": dict(job_types.most_common()),
        "borough_breakdown": dict(boroughs.most_common()),
        "large_projects_over_1m": [{
            "address": f"{r.get('house_number', '')} {r.get('street_name', '')}",
            "borough": r.get("borough", ""),
            "type": r.get("job_type", ""),
            "cost": f"${float(r.get('initial_cost', 0)):,.0f}",
        } for r in sorted(big_projects, key=lambda x: float(x.get("initial_cost", 0)), reverse=True)[:10]],
    }


if __name__ == "__main__":
    print("=" * 70)
    print(f"NYC OPEN DATA WEEKLY SCAN — {datetime.now().strftime('%B %d, %Y')}")
    print("=" * 70)

    print("\n📊 RECENTLY UPDATED DATASETS")
    datasets = get_updated_datasets()
    by_agency = Counter(d["agency"] for d in datasets)
    print(f"  {len(datasets)} datasets updated in the past 7 days")
    print("  Top agencies by update volume:")
    for agency, count in by_agency.most_common(10):
        print(f"    {agency or '(none)'}: {count}")

    print("\n🚗 MOTOR VEHICLE COLLISIONS")
    c = scan_collisions()
    if c:
        print(f"  {c['total_crashes']} crashes ({c['date_range']})")
        print(f"  {c['persons_injured']} injured, {c['persons_killed']} killed")
        print(f"  {c['pedestrians_injured']} pedestrians injured, {c['pedestrians_killed']} killed")
        print(f"  {c['cyclists_injured']} cyclists injured")

    print("\n🔫 SHOOTING INCIDENTS")
    s = scan_shootings()
    if s:
        print(f"  {s['total_incidents']} incidents ({s['date_range']})")
        print(f"  {s['total_victims']} victims, {s['murders']} murders")
        print(f"  Boroughs: {s['borough_breakdown']}")
        print(f"  Top precincts: {dict(list(s['top_precincts'].items())[:5])}")

    print("\n🏠 HPD HOUSING COMPLAINTS")
    h = scan_hpd_complaints()
    print(f"  {h['total_complaints']} total complaints")
    for cat, cnt in list(h["top_categories"].items())[:5]:
        print(f"    {cat}: {cnt}")

    print("\n🚨 HPD VACATE/REPAIR ORDERS")
    v = scan_hpd_vacates()
    print(f"  {v['new_orders']} new orders, {v['still_active']} still active")
    for d in v["details"][:5]:
        print(f"    {d['address']}, {d['borough']} — {d['reason']} ({d['type']}, {d['units']} units)")

    print("\n🍽️ RESTAURANT INSPECTIONS")
    ri = scan_restaurant_inspections()
    print(f"  {ri['total_inspections']} inspection records")
    print(f"  {ri['critical_violations']} critical violations")
    print(f"  Grades: {ri['grade_distribution']}")

    print("\n🏗️ DOB JOB FILINGS")
    dob = scan_dob_filings()
    print(f"  {dob['total_filings']} filings")
    print(f"  Types: {dob['job_types']}")
    if dob["large_projects_over_1m"]:
        print("  Large projects (>$1M):")
        for p in dob["large_projects_over_1m"][:5]:
            print(f"    {p['address']}, {p['borough']} — {p['type']} — {p['cost']}")

    print("\n" + "=" * 70)
    print("Scan complete.")
