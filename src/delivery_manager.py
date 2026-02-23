#!/usr/bin/env python3
"""BlackRoad Delivery Manager — delivery tracking and logistics manager."""

from __future__ import annotations

import argparse
import json
import random
import sqlite3
import string
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Optional

# ── ANSI Colors ───────────────────────────────────────────────────────────────
GREEN  = "\033[0;32m"
RED    = "\033[0;31m"
YELLOW = "\033[1;33m"
CYAN   = "\033[0;36m"
BLUE   = "\033[0;34m"
MAGENTA = "\033[0;35m"
BOLD   = "\033[1m"
NC     = "\033[0m"

DB_PATH = Path.home() / ".blackroad" / "delivery-manager.db"


class DeliveryStatus(str, Enum):
    PENDING          = "pending"
    PICKED_UP        = "picked_up"
    IN_TRANSIT       = "in_transit"
    OUT_FOR_DELIVERY = "out_for_delivery"
    DELIVERED        = "delivered"
    FAILED_ATTEMPT   = "failed_attempt"
    RETURNED         = "returned"
    CANCELLED        = "cancelled"


STATUS_COLOR = {
    DeliveryStatus.PENDING:          YELLOW,
    DeliveryStatus.PICKED_UP:        CYAN,
    DeliveryStatus.IN_TRANSIT:       BLUE,
    DeliveryStatus.OUT_FOR_DELIVERY: MAGENTA,
    DeliveryStatus.DELIVERED:        GREEN,
    DeliveryStatus.FAILED_ATTEMPT:   RED,
    DeliveryStatus.RETURNED:         RED,
    DeliveryStatus.CANCELLED:        RED,
}


@dataclass
class Delivery:
    """Represents a single delivery shipment."""

    sender:             str
    recipient:          str
    destination:        str
    weight_kg:          float          = 0.0
    status:             DeliveryStatus = DeliveryStatus.PENDING
    tracking_number:    str            = field(default_factory=lambda: "BR" + "".join(
                                            random.choices(string.ascii_uppercase + string.digits, k=10)))
    estimated_delivery: Optional[str]  = None
    actual_delivery:    Optional[str]  = None
    courier:            str            = ""
    notes:              str            = ""
    created_at:         str            = field(default_factory=lambda: datetime.now().isoformat())
    updated_at:         str            = field(default_factory=lambda: datetime.now().isoformat())
    id:                 Optional[int]  = None

    def is_active(self) -> bool:
        return self.status not in (DeliveryStatus.DELIVERED,
                                   DeliveryStatus.RETURNED,
                                   DeliveryStatus.CANCELLED)

    def status_color(self) -> str:
        return STATUS_COLOR.get(self.status, NC)


@dataclass
class TrackingEvent:
    """A single status-change event attached to a delivery."""

    delivery_id: int
    status:      str
    location:    str = ""
    message:     str = ""
    timestamp:   str = field(default_factory=lambda: datetime.now().isoformat())
    id:          Optional[int] = None


class DeliveryManager:
    """SQLite-backed delivery tracking engine."""

    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS deliveries (
                    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                    tracking_number    TEXT    UNIQUE NOT NULL,
                    sender             TEXT    NOT NULL,
                    recipient          TEXT    NOT NULL,
                    destination        TEXT    NOT NULL,
                    weight_kg          REAL    DEFAULT 0.0,
                    status             TEXT    DEFAULT 'pending',
                    estimated_delivery TEXT,
                    actual_delivery    TEXT,
                    courier            TEXT    DEFAULT '',
                    notes              TEXT    DEFAULT '',
                    created_at         TEXT    NOT NULL,
                    updated_at         TEXT    NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tracking_events (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    delivery_id INTEGER NOT NULL REFERENCES deliveries(id),
                    status      TEXT    NOT NULL,
                    location    TEXT    DEFAULT '',
                    message     TEXT    DEFAULT '',
                    timestamp   TEXT    NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tracking ON deliveries(tracking_number)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_del_status ON deliveries(status)")
            conn.commit()

    def _row_to_delivery(self, row: sqlite3.Row) -> Delivery:
        return Delivery(
            id=row["id"], tracking_number=row["tracking_number"],
            sender=row["sender"], recipient=row["recipient"],
            destination=row["destination"], weight_kg=row["weight_kg"],
            status=DeliveryStatus(row["status"]),
            estimated_delivery=row["estimated_delivery"],
            actual_delivery=row["actual_delivery"],
            courier=row["courier"] or "", notes=row["notes"] or "",
            created_at=row["created_at"], updated_at=row["updated_at"],
        )

    def create(self, sender: str, recipient: str, destination: str,
               weight_kg: float = 0.0, courier: str = "",
               estimated_delivery: Optional[str] = None, notes: str = "") -> Delivery:
        """Create a new delivery and return it with generated tracking number."""
        now = datetime.now().isoformat()
        d = Delivery(sender=sender, recipient=recipient, destination=destination,
                     weight_kg=weight_kg, courier=courier,
                     estimated_delivery=estimated_delivery, notes=notes,
                     created_at=now, updated_at=now)
        with self._conn() as conn:
            cur = conn.execute(
                "INSERT INTO deliveries (tracking_number,sender,recipient,destination,"
                "weight_kg,status,estimated_delivery,actual_delivery,courier,notes,created_at,updated_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (d.tracking_number, d.sender, d.recipient, d.destination, d.weight_kg,
                 d.status.value, d.estimated_delivery, d.actual_delivery, d.courier,
                 d.notes, d.created_at, d.updated_at),
            )
            conn.commit()
            d.id = cur.lastrowid
        self._log_event(d.id, d.status.value, message="Shipment created")
        return d

    def update_status(self, tracking_number: str, new_status: str,
                      location: str = "", message: str = "") -> Optional[Delivery]:
        """Advance delivery status and append a tracking event."""
        now = datetime.now().isoformat()
        actual = now if new_status == DeliveryStatus.DELIVERED.value else None
        with self._conn() as conn:
            conn.execute(
                "UPDATE deliveries SET status=?,actual_delivery=COALESCE(?,actual_delivery),"
                "updated_at=? WHERE tracking_number=?",
                (new_status, actual, now, tracking_number),
            )
            conn.commit()
        d = self.get(tracking_number)
        if d:
            self._log_event(d.id, new_status, location=location, message=message)
        return d

    def _log_event(self, delivery_id: int, status: str,
                   location: str = "", message: str = "") -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO tracking_events (delivery_id,status,location,message,timestamp)"
                " VALUES (?,?,?,?,?)",
                (delivery_id, status, location, message, datetime.now().isoformat()),
            )
            conn.commit()

    def get(self, tracking_number: str) -> Optional[Delivery]:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM deliveries WHERE tracking_number=?",
                               (tracking_number,)).fetchone()
        return self._row_to_delivery(row) if row else None

    def list_deliveries(self, status: Optional[str] = None) -> List[Delivery]:
        sql = "SELECT * FROM deliveries WHERE 1=1"
        params: list = []
        if status:
            sql += " AND status=?"; params.append(status)
        sql += " ORDER BY updated_at DESC"
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._row_to_delivery(r) for r in rows]

    def history(self, tracking_number: str) -> list:
        d = self.get(tracking_number)
        if not d:
            return []
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM tracking_events WHERE delivery_id=? ORDER BY timestamp",
                (d.id,)).fetchall()
        return [dict(r) for r in rows]

    def export_json(self, path: str) -> int:
        items = self.list_deliveries()
        records = [asdict(i) | {"status": i.status.value} for i in items]
        with open(path, "w") as fh:
            json.dump(records, fh, indent=2, default=str)
        return len(records)

    def stats(self) -> dict:
        items = self.list_deliveries()
        by_status: dict = {}
        for d in items:
            by_status[d.status.value] = by_status.get(d.status.value, 0) + 1
        active = sum(1 for d in items if d.is_active())
        return {"total": len(items), "active": active, "by_status": by_status}


# ── CLI ───────────────────────────────────────────────────────────────────────

def _print_delivery(d: Delivery) -> None:
    sc = d.status_color()
    print(f"  {BOLD}#{d.id:<4}{NC} {CYAN}{d.tracking_number}{NC}  "
          f"{sc}{d.status.value:<18}{NC} {d.recipient}")
    print(f"            {d.sender} → {d.destination}"
          + (f"  courier:{d.courier}" if d.courier else "")
          + (f"  eta:{YELLOW}{d.estimated_delivery}{NC}" if d.estimated_delivery else ""))


def cmd_list(args: argparse.Namespace, mgr: DeliveryManager) -> None:
    items = mgr.list_deliveries(status=args.filter_status)
    if not items:
        print(f"{YELLOW}No deliveries found.{NC}"); return
    print(f"\n{BOLD}{BLUE}── Deliveries ({len(items)}) {'─'*38}{NC}")
    for d in items:
        _print_delivery(d)
    print()


def cmd_add(args: argparse.Namespace, mgr: DeliveryManager) -> None:
    d = mgr.create(args.sender, args.recipient, args.destination,
                   weight_kg=args.weight, courier=args.courier,
                   estimated_delivery=args.eta, notes=args.notes)
    print(f"{GREEN}✓ Shipment created{NC}")
    print(f"  Tracking : {BOLD}{CYAN}{d.tracking_number}{NC}")
    print(f"  From     : {d.sender}  →  {d.recipient}")
    print(f"  Dest     : {d.destination}")


def cmd_track(args: argparse.Namespace, mgr: DeliveryManager) -> None:
    d = mgr.get(args.tracking_number)
    if not d:
        print(f"{RED}✗ Tracking number not found: {args.tracking_number}{NC}"); return
    sc = d.status_color()
    print(f"\n{BOLD}{BLUE}── Tracking: {CYAN}{d.tracking_number}{NC}")
    print(f"  Status     : {sc}{BOLD}{d.status.value}{NC}")
    print(f"  Sender     : {d.sender}")
    print(f"  Recipient  : {d.recipient}")
    print(f"  Destination: {d.destination}")
    if d.estimated_delivery:
        print(f"  ETA        : {YELLOW}{d.estimated_delivery}{NC}")
    if d.actual_delivery:
        print(f"  Delivered  : {GREEN}{d.actual_delivery}{NC}")
    events = mgr.history(args.tracking_number)
    if events:
        print(f"\n  {BOLD}History:{NC}")
        for e in events:
            loc = f" @ {e['location']}" if e["location"] else ""
            print(f"    {e['timestamp'][:19]}  {e['status']}{loc}  {e['message']}")
    print()


def cmd_update(args: argparse.Namespace, mgr: DeliveryManager) -> None:
    d = mgr.update_status(args.tracking_number, args.new_status,
                          location=args.location, message=args.message)
    if d:
        print(f"{GREEN}✓ {d.tracking_number} → {args.new_status}{NC}")
    else:
        print(f"{RED}✗ Tracking number not found{NC}")


def cmd_status_cmd(args: argparse.Namespace, mgr: DeliveryManager) -> None:
    s = mgr.stats()
    print(f"\n{BOLD}{BLUE}── Delivery Manager Status {'─'*34}{NC}")
    print(f"  Total    : {BOLD}{s['total']}{NC}   Active: {BOLD}{s['active']}{NC}")
    print(f"\n  {BOLD}By Status:{NC}")
    for name, count in sorted(s["by_status"].items()):
        color = STATUS_COLOR.get(DeliveryStatus(name), NC)
        bar   = "█" * min(count, 40)
        print(f"    {color}{name:<20}{NC} {count:>4}  {bar}")
    print()


def cmd_export(args: argparse.Namespace, mgr: DeliveryManager) -> None:
    n = mgr.export_json(args.output)
    print(f"{GREEN}✓ Exported {n} deliveries → {args.output}{NC}")


def build_parser() -> argparse.ArgumentParser:
    p   = argparse.ArgumentParser(description="BlackRoad Delivery Manager")
    sub = p.add_subparsers(dest="command", required=True)

    ls = sub.add_parser("list", help="List deliveries")
    ls.add_argument("--filter-status", dest="filter_status", metavar="STATUS")

    add = sub.add_parser("add", help="Create a new shipment")
    add.add_argument("sender");    add.add_argument("recipient");  add.add_argument("destination")
    add.add_argument("--weight", type=float, default=0.0, metavar="KG")
    add.add_argument("--courier", default="")
    add.add_argument("--eta",    dest="eta", metavar="YYYY-MM-DD")
    add.add_argument("--notes",  default="")

    tr = sub.add_parser("track", help="Track a shipment")
    tr.add_argument("tracking_number")

    up = sub.add_parser("update", help="Update delivery status")
    up.add_argument("tracking_number")
    up.add_argument("new_status", choices=[x.value for x in DeliveryStatus])
    up.add_argument("--location", default="")
    up.add_argument("--message",  default="")

    sub.add_parser("status", help="Show statistics")

    ex = sub.add_parser("export", help="Export to JSON")
    ex.add_argument("--output", "-o", default="deliveries_export.json")

    return p


def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()
    mgr    = DeliveryManager()
    {"list": cmd_list, "add": cmd_add, "track": cmd_track,
     "update": cmd_update, "status": cmd_status_cmd,
     "export": cmd_export}[args.command](args, mgr)


if __name__ == "__main__":
    main()
