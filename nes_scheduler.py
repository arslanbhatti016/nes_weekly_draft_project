"""
NES Scheduler — v1.4
=====================
Run this one file. That's it.

Put these 3 CSVs in the same folder:
  Jobs.csv, WeekOf.csv, Technicians.csv

Then run:
  python nes_scheduler.py

Output files:
  NES_Routes.csv       — one route block per group (Day / Technician / Area)
                         start here — review one route at a time
  NES_Weekly_Draft.csv — flat job list with route info, for reference
  NES_Summary.csv      — one-page snapshot of totals

v1.4 change: Each job now appears exactly once across the entire week.
  Standby slots are filled from the unassigned pool — never from jobs
  already active on another route or day.
"""

import csv
import datetime
from collections import Counter, defaultdict

# ── Config ────────────────────────────────────────────────────────────────────
JOBS_FILE         = "Jobs.csv"
WEEKOF_FILE       = "WeekOf.csv"
TECHS_FILE        = "Technicians.csv"
ROUTES_FILE       = "NES_Routes.csv"
DRAFT_FILE        = "NES_Weekly_Draft.csv"
SUMMARY_FILE      = "NES_Summary.csv"

DAYS              = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
JOBS_PER_ROUTE    = 8   # active jobs per route
STANDBY_PER_ROUTE = 2   # standby jobs per route

AREA_NAMES = {
    "01": "Providence RI",         "02": "South Coast MA",
    "03": "Plymouth MA",           "04": "Worcester MA",
    "05": "South RI",              "06": "NW RI / CT",
    "07": "MetroWest MA",          "08": "South Shore MA",
    "09": "Newton / Westwood",     "10": "Lexington / Arlington",
    "11": "Medford / Malden",      "12": "Cape Cod",
    "13": "North Central MA",      "14": "Boston",
    "15": "North Shore MA",        "16": "NH Seacoast",
    "17": "Connecticut",           "18": "Central NH",
    "19": "Northern NH",           "20": "NH Mountains",
    "21": "Holyoke / Springfield", "23": "Lawrence MA",
}


# ── Load data ─────────────────────────────────────────────────────────────────
def load_csv(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def load_jobs():
    jobs = []
    for r in load_csv(JOBS_FILE):
        jobs.append({
            "job_id":       r.get("Job Num", "").strip(),
            "area":         r.get("Area", "").strip().zfill(2),
            "address":      r.get("Full Address", "").strip(),
            "category":     r.get("Category", "").strip(),
            "days_old":     int(r.get("Num Days Old", 0) or 0),
            "age_critical": r.get("Age Critical Flag", "").strip().lower() == "true",
            "priority":     "‼️" in r.get("Priority Flag", ""),
        })
    return jobs


def load_technicians():
    try:
        rows = load_csv(TECHS_FILE)
        techs = [r.get("Name", "").strip() for r in rows
                 if r.get("Can Run Solo", "").strip().lower() == "true"]
        return techs if techs else ["Unassigned A", "Unassigned B",
                                    "Unassigned C", "Unassigned D"]
    except Exception:
        return ["Unassigned A", "Unassigned B", "Unassigned C", "Unassigned D"]


def load_week_context():
    rows = load_csv(WEEKOF_FILE)
    if not rows:
        return {"blocked": set(), "week_of": "", "season": ""}
    r = rows[0]
    blocked = set()
    try:
        week_date = datetime.datetime.fromisoformat(
            r["Week Of"].replace("Z", "+00:00")).date()
        def pd(s):
            return datetime.datetime.fromisoformat(
                s.replace("Z", "+00:00")).date() if s.strip() else None
        bs, be = pd(r.get("Block Start", "")), pd(r.get("Block End", ""))
        if bs and be:
            for i, day in enumerate(DAYS):
                if bs <= week_date + datetime.timedelta(days=i) <= be:
                    blocked.add(day)
    except Exception:
        pass
    return {
        "week_of": r.get("Week Of", "")[:10],
        "season":  r.get("Season", "").strip(),
        "blocked": blocked,
    }


# ── Core logic ────────────────────────────────────────────────────────────────
def sort_jobs(jobs):
    """Priority first → age critical → oldest."""
    return sorted(jobs, key=lambda j: (
        not j["priority"], not j["age_critical"], -j["days_old"]
    ))


def build_routes(jobs, blocked, technicians):
    """
    Assign jobs to routes so that every job has exactly one role in the week:
    ACTIVE on one route, STANDBY on one route, or HOLD.

    Approach
    --------
    Pass 1 — Active assignment
      Group jobs by area. Sort each bucket by priority → age critical → oldest.
      Assign the largest area buckets to Day+Technician slots first (greedy).
      The first JOBS_PER_ROUTE jobs in each bucket become ACTIVE on that slot.
      Remaining jobs from filled areas go into a shared unassigned pool.
      Area groups that don't fit any slot also go into the pool.

    Pass 2 — Standby assignment
      Each filled route can absorb up to STANDBY_PER_ROUTE standby jobs.
      Pull standbys from the unassigned pool, preferring jobs from the *same
      area* as the route (geographically sensible for a real swap), then by
      the same priority → age critical → oldest order.
      Each job is removed from the pool as soon as it is assigned standby —
      so a job can only be standby on one route, never duplicated.

    Pass 3 — Hold
      Everything left in the pool after standby assignment is HOLD.
    """
    available_days = [d for d in DAYS if d not in blocked]

    # ── Pass 1: group, sort, assign active ───────────────────────────────────
    area_buckets = defaultdict(list)
    for job in jobs:
        area_buckets[job["area"]].append(job)
    for area in area_buckets:
        area_buckets[area] = sort_jobs(area_buckets[area])

    # Build route slots: day × technician (in day order, then tech order)
    slots = []
    for day in available_days:
        for tech in technicians:
            slots.append({
                "route_id":  f"{day[:3].upper()}-{tech.split()[0].upper()}-??",
                "day":       day,
                "tech":      tech,
                "area_code": None,
                "area_name": None,
                "active":    [],
                "standby":   [],
            })

    sorted_areas = sorted(area_buckets.keys(), key=lambda a: -len(area_buckets[a]))
    slot_idx = 0

    # unassigned_pool holds all jobs not yet given ACTIVE status, keyed by area
    unassigned_pool = defaultdict(list)   # area -> [jobs]

    for area in sorted_areas:
        bucket = area_buckets[area]
        if not bucket:
            continue

        # Advance to next empty slot
        while slot_idx < len(slots) and slots[slot_idx]["area_code"] is not None:
            slot_idx += 1

        if slot_idx >= len(slots):
            # No slot available — entire bucket goes to pool
            for j in bucket:
                unassigned_pool[j["area"]].append(j)
            continue

        slot = slots[slot_idx]

        # Build a readable route ID that includes the area
        slot["route_id"]   = f"{slot['day'][:3].upper()}-{slot['tech'].split()[0].upper()}-{area}"
        slot["area_code"]  = area
        slot["area_name"]  = AREA_NAMES.get(area, f"Area {area}")

        active_jobs  = bucket[:JOBS_PER_ROUTE]
        leftover     = bucket[JOBS_PER_ROUTE:]

        for j in active_jobs:
            j.update({
                "status":   "ACTIVE",
                "route_id": slot["route_id"],
                "day":      slot["day"],
                "tech":     slot["tech"],
            })
        slot["active"] = active_jobs

        # Leftovers go to unassigned pool (they may become standby or hold)
        for j in leftover:
            unassigned_pool[j["area"]].append(j)

        slot_idx += 1

    filled = [s for s in slots if s["area_code"] is not None]

    # ── Pass 2: assign standby — each job used at most once ──────────────────
    #
    # For each filled route, we pull up to STANDBY_PER_ROUTE jobs from the
    # unassigned pool.  We prefer jobs from the same area as the route (a
    # realistic swap candidate), then fall back to any area.
    # Jobs are consumed from the pool as they are assigned, so no duplication.

    for slot in filled:
        slots_needed = STANDBY_PER_ROUTE
        if slots_needed <= 0:
            continue

        chosen = []

        # First: same-area candidates
        same_area = unassigned_pool.get(slot["area_code"], [])
        while same_area and len(chosen) < slots_needed:
            chosen.append(same_area.pop(0))

        # Second: any-area candidates (sorted pool-wide for fairness)
        if len(chosen) < slots_needed:
            # Flatten remaining pool, re-sort, take what we need
            remaining = []
            for area_jobs in unassigned_pool.values():
                remaining.extend(area_jobs)
            remaining = sort_jobs(remaining)

            # Remove chosen jobs from remaining (they were already popped above)
            chosen_ids = {j["job_id"] for j in chosen}
            remaining  = [j for j in remaining if j["job_id"] not in chosen_ids]

            for j in remaining:
                if len(chosen) >= slots_needed:
                    break
                chosen.append(j)
                # Remove from its area bucket in the pool
                pool_bucket = unassigned_pool.get(j["area"], [])
                if j in pool_bucket:
                    pool_bucket.remove(j)

        # Assign standby status and attach to the slot
        for j in chosen:
            j.update({
                "status":   "STANDBY",
                "route_id": slot["route_id"],
                "day":      slot["day"],
                "tech":     slot["tech"],
            })
        slot["standby"] = chosen

    # ── Pass 3: everything remaining is HOLD ─────────────────────────────────
    hold = []
    for area_jobs in unassigned_pool.values():
        for j in area_jobs:
            j.update({"status": "HOLD", "route_id": "", "day": "", "tech": ""})
            hold.append(j)

    # Sort hold by area then priority for tidy output
    hold = sorted(hold, key=lambda j: (j["area"], not j["priority"],
                                        not j["age_critical"], -j["days_old"]))

    return filled, hold


# ── Helpers ───────────────────────────────────────────────────────────────────
def get_flags(job):
    flags = []
    if job.get("priority"):     flags.append("Priority")
    if job.get("age_critical"): flags.append("Age Critical")
    return ", ".join(flags)


def make_reason(job):
    status = job.get("status", "")
    if status == "ACTIVE":
        flags = get_flags(job)
        return job.get("day", "") + (f" — {flags}" if flags else "")
    elif status == "STANDBY":
        return f"Standby for {job.get('route_id', '')} — fires if active job cancels"
    return "Hold for next week"


# ── Write NES_Routes.csv ──────────────────────────────────────────────────────
def write_routes(routes, hold):
    """
    One route block at a time:
      ▶ ROUTE  — header row (Day / Technician / Area / counts)
      ACTIVE   — active jobs (ordered: priority → age critical → oldest)
      STANDBY  — standby jobs for this route
      [blank]  — separator
      HOLD     — all hold jobs at the bottom
    """
    DAY_ORDER = {d: i for i, d in enumerate(DAYS)}
    FIELDS = [
        "Route ID", "Day", "Technician", "Area Code", "Area",
        "Job ID", "Address", "Category", "Days Old", "Flags", "Status",
        "Active Count", "Standby Count",
    ]
    rows = []

    for route in sorted(routes, key=lambda r: (DAY_ORDER.get(r["day"], 99), r["tech"])):
        rows.append({
            "Route ID":      route["route_id"],
            "Day":           route["day"],
            "Technician":    route["tech"],
            "Area Code":     route["area_code"],
            "Area":          route["area_name"],
            "Job ID": "", "Address": "", "Category": "", "Days Old": "", "Flags": "",
            "Status":        "▶ ROUTE",
            "Active Count":  len(route["active"]),
            "Standby Count": len(route["standby"]),
        })
        for job in route["active"]:
            rows.append({
                "Route ID":   route["route_id"], "Day": route["day"],
                "Technician": route["tech"],
                "Area Code":  job["area"],
                "Area":       AREA_NAMES.get(job["area"], job["area"]),
                "Job ID":     job["job_id"], "Address": job["address"],
                "Category":   job["category"], "Days Old": job["days_old"],
                "Flags":      get_flags(job), "Status": "ACTIVE",
                "Active Count": "", "Standby Count": "",
            })
        for job in route["standby"]:
            rows.append({
                "Route ID":   route["route_id"], "Day": route["day"],
                "Technician": route["tech"],
                "Area Code":  job["area"],
                "Area":       AREA_NAMES.get(job["area"], job["area"]),
                "Job ID":     job["job_id"], "Address": job["address"],
                "Category":   job["category"], "Days Old": job["days_old"],
                "Flags":      get_flags(job), "Status": "STANDBY",
                "Active Count": "", "Standby Count": "",
            })
        rows.append({f: "" for f in FIELDS})   # blank separator

    for job in hold:
        rows.append({
            "Route ID":   "HOLD", "Day": "—", "Technician": "—",
            "Area Code":  job["area"],
            "Area":       AREA_NAMES.get(job["area"], job["area"]),
            "Job ID":     job["job_id"], "Address": job["address"],
            "Category":   job["category"], "Days Old": job["days_old"],
            "Flags":      get_flags(job), "Status": "HOLD",
            "Active Count": "", "Standby Count": "",
        })

    with open(ROUTES_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)
    print(f"[✓] {ROUTES_FILE}  ({len(routes)} routes)")


# ── Write NES_Weekly_Draft.csv ────────────────────────────────────────────────
def write_draft(routes, hold):
    """Flat job list grouped by route — active, then standby, then hold."""
    DAY_ORDER = {d: i for i, d in enumerate(DAYS)}
    FIELDS = [
        "Route ID", "Day", "Technician", "Area Code", "Area",
        "Job ID", "Address", "Category", "Days Old", "Flags", "Status", "Reason",
    ]
    rows = []

    for route in sorted(routes, key=lambda r: (DAY_ORDER.get(r["day"], 99), r["tech"])):
        for job in route["active"] + route["standby"]:
            rows.append({
                "Route ID":   route["route_id"], "Day": route["day"],
                "Technician": route["tech"],
                "Area Code":  job["area"],
                "Area":       AREA_NAMES.get(job["area"], job["area"]),
                "Job ID":     job["job_id"], "Address": job["address"],
                "Category":   job["category"], "Days Old": job["days_old"],
                "Flags":      get_flags(job), "Status": job["status"],
                "Reason":     make_reason(job),
            })

    for job in hold:
        rows.append({
            "Route ID":   "—", "Day": "—", "Technician": "—",
            "Area Code":  job["area"],
            "Area":       AREA_NAMES.get(job["area"], job["area"]),
            "Job ID":     job["job_id"], "Address": job["address"],
            "Category":   job["category"], "Days Old": job["days_old"],
            "Flags":      get_flags(job), "Status": "HOLD",
            "Reason":     make_reason(job),
        })

    with open(DRAFT_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)
    print(f"[✓] {DRAFT_FILE}  ({len(rows)} rows)")


# ── Write NES_Summary.csv ─────────────────────────────────────────────────────
def write_summary(routes, hold, week_ctx):
    blocked     = week_ctx["blocked"]
    active_all  = [j for r in routes for j in r["active"]]
    standby_all = [j for r in routes for j in r["standby"]]
    day_counts  = Counter(j["day"] for j in active_all)
    rows = []

    rows += [
        {"Section": "WEEK",   "Label": "Week Of",      "Value": week_ctx["week_of"]},
        {"Section": "WEEK",   "Label": "Season",        "Value": week_ctx["season"]},
        {"Section": "WEEK",   "Label": "Blocked Days",
         "Value": ", ".join(sorted(blocked)) if blocked else "None"},
        {"Section": "WEEK",   "Label": "Routes Built",  "Value": len(routes)},
        {"Section": "",       "Label": "",              "Value": ""},
        {"Section": "TOTALS", "Label": "Total Jobs",    "Value": len(active_all)+len(standby_all)+len(hold)},
        {"Section": "TOTALS", "Label": "Active",        "Value": len(active_all)},
        {"Section": "TOTALS", "Label": "Standby",       "Value": len(standby_all)},
        {"Section": "TOTALS", "Label": "Hold",          "Value": len(hold)},
        {"Section": "",       "Label": "",              "Value": ""},
    ]
    for day in DAYS:
        tag = " [blocked]" if day in blocked else ""
        rows.append({"Section": "BY DAY", "Label": day + tag,
                     "Value": day_counts.get(day, 0)})
    rows.append({"Section": "", "Label": "", "Value": ""})

    for route in routes:
        rows.append({
            "Section": "BY ROUTE",
            "Label":   f"{route['route_id']}  {route['day']} / {route['tech']} / {route['area_name']}",
            "Value":   f"Active: {len(route['active'])}   Standby: {len(route['standby'])}",
        })

    with open(SUMMARY_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["Section", "Label", "Value"])
        w.writeheader()
        w.writerows(rows)
    print(f"[✓] {SUMMARY_FILE}")


# ── Integrity check ───────────────────────────────────────────────────────────
def check_no_duplicates(routes, hold):
    """Verify every job appears exactly once. Print a warning if not."""
    seen = {}
    duplicates = []
    all_jobs = (
        [(j, r["route_id"], j["status"]) for r in routes for j in r["active"]]
      + [(j, r["route_id"], j["status"]) for r in routes for j in r["standby"]]
      + [(j, "HOLD", "HOLD") for j in hold]
    )
    for job, route_id, status in all_jobs:
        jid = job["job_id"]
        if jid in seen:
            duplicates.append(
                f"  !! Job {jid} appears as {seen[jid]} AND as {status} on {route_id}"
            )
        else:
            seen[jid] = f"{status} on {route_id}"
    if duplicates:
        print("[WARN] Duplicate job assignments found:")
        for d in duplicates:
            print(d)
    else:
        print(f"[✓] Integrity check passed — all {len(seen)} jobs appear exactly once")


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("[NES] Loading data...")
    jobs        = load_jobs()
    week_ctx    = load_week_context()
    technicians = load_technicians()

    print(f"[NES] {len(jobs)} jobs | Season: {week_ctx['season']} | "
          f"Blocked: {week_ctx['blocked'] or 'none'}")
    print(f"[NES] Technicians: {technicians}")

    print("[NES] Building routes...")
    routes, hold = build_routes(jobs, week_ctx["blocked"], technicians)

    active_count  = sum(len(r["active"])  for r in routes)
    standby_count = sum(len(r["standby"]) for r in routes)
    print(f"[NES] Routes: {len(routes)} | Active: {active_count} | "
          f"Standby: {standby_count} | Hold: {len(hold)}")

    check_no_duplicates(routes, hold)

    write_routes(routes, hold)
    write_draft(routes, hold)
    write_summary(routes, hold, week_ctx)

    print("\n  Done. Open these files to review:")
    print(f"  → {ROUTES_FILE}   (start here — one route block at a time)")
    print(f"  → {DRAFT_FILE}    (flat list for reference)")
    print(f"  → {SUMMARY_FILE}  (totals snapshot)")
