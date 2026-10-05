"""
Kraken -> Odoo sync consumer.

Consumes ticket/incident events from the Kraken Kafka topic, transforms them
into Odoo project.task fields, and creates/updates tasks via Odoo's XML-RPC API.

Setup:
    pip install kafka-python

    Set credentials as environment variables (do NOT hardcode them):
        export ODOO_URL="https://your-odoo-instance.example.com"
        export ODOO_DB="your-db-name"
        export ODOO_USERNAME="integration-user@example.com"
        export ODOO_API_KEY="your-api-key"

Run:
    python kraken_to_odoo_consumer.py

STILL NEEDS CONFIRMING (marked TODO throughout):
  - Exact JSON key for "admin group" in the real Kafka message
  - Valid Odoo selection values for x_priority, x_issue_type, requestor_type
  - Kraken username -> Odoo user id lookup table (ASSIGNEE_MAP / REPORTER_MAP)
  - Whether ticketDTO arrives wrapped ({"eventType":..,"ticketDTO":{...}}) or raw
  - Whether departments live on hr.department or a custom model (x_department_id's
    comodel - check the field definition in Odoo Studio / Settings > Technical)
"""

import json
import logging
import os
import sqlite3
import xmlrpc.client
from datetime import datetime, timezone
from pathlib import Path

from dotenv import dotenv_values
from kafka import KafkaConsumer

BASE_DIR = Path(__file__).resolve().parent.parent

# Load environment variables from the root folder
env = dotenv_values(BASE_DIR / ".env")

# Configuration

KAFKA_BOOTSTRAP_SERVERS = env.get("KAFKA_BOOTSTRAP_SERVERS") or os.environ.get("KAFKA_BOOTSTRAP_SERVERS") or "localhost:9094"
KAFKA_TOPIC = "tickets"
KAFKA_GROUP_ID = "odoo-ticket-sync"                
KAFKA_AUTO_OFFSET_RESET = "earliest"               # 'latest' once you've caught up historically

ODOO_URL = env.get("ODOO_URL")
ODOO_DB = env.get("ODOO_DB")
ODOO_USERNAME = env.get("ODOO_USERNAME")
ODOO_API_KEY = env.get("ODOO_API_KEY")

# Maps Kraken adminGroup to a specific Odoo Project Name
PROJECT_MAP = {
    "IT SUPPORT": "Kraken - IT & DC",
    "IT & DC": "Kraken - IT & DC",
    "APPLICATIONS": "Kraken - Applications",
}
DEFAULT_PROJECT_NAME = "Kraken Testing"  # Fallback if adminGroup isn't in PROJECT_MAP

DEPARTMENT_MAPPING_FILE = BASE_DIR / "config" / "admin_group_to_odoo_department.json"
IDEMPOTENCY_DB_FILE = os.environ.get("DB_FILE_PATH", BASE_DIR / "data" / "kraken_odoo_sync.db")
LOG_FILE = os.environ.get("LOG_FILE_PATH", BASE_DIR / "data" / "sync.log")

ADMIN_GROUP_JSON_KEY = "adminGroup"   # TODO: confirm real key name from a live message

# Mapped from Odoo's x_priority selection values
PRIORITY_MAP = {
    "CRITICAL": "3",
    "HIGH": "2",
    "MEDIUM": "1",
    "LOW": "0",
}

# Maps Kraken ticket status to Odoo Kanban stage_id   {production}
# STATUS_TO_STAGE_MAP = {
#     "OPEN": 1,             # Backlog
#     "ASSIGNED": 2,         # Planned for Sprint
#     "IN_PROGRESS": 3,      # In Progress
#     "ON_HOLD": 4,          # On Hold
#     "PENDING": 4,          # On Hold
#     "RESOLVED": 5,         # Done
#     "CLOSED": 5,           # Done
# }


# STATUS_TO_STAGE_MAP = {
#     "OPEN": 22,            # Backlog
#     "ASSIGNED": 23,        # Planned for Sprint
#     "IN_PROGRESS": 24,     # In Progress
#     "ON_HOLD": 25,         # On Hold
#     "PENDING": 25,         # On Hold
#     "RESOLVED": 20,        # Done
#     "CLOSED": 20,          # Done
# }


ISSUE_TYPE_MAPPING_FILE = BASE_DIR / "config" / "ticket_type_to_odoo_issue_type.json"
REQUESTOR_TYPE_DEFAULT = "external"  # TODO: confirm valid selection value - Kraken tickets are all external

USER_MAPPING_FILE = BASE_DIR / "config" / "user_mapping copy.json"

def load_user_mapping() -> dict:
    try:
        with open(USER_MAPPING_FILE) as f:
            data = json.load(f)
            # Map the Kraken username (the dict key) to the Odoo user ID
            return {k: v.get("odoo_user_id") for k, v in data.get("mapping", {}).items()}
    except FileNotFoundError:
        return {}

USER_MAP = load_user_mapping()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
log = logging.getLogger("kraken_odoo_sync")


# Idempotency store (sqlite - kraken ticket id -> odoo task id)

def init_db():
    conn = sqlite3.connect(IDEMPOTENCY_DB_FILE)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ticket_map (
            kraken_id TEXT PRIMARY KEY,
            odoo_task_id INTEGER,
            last_status TEXT,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS failed_tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kraken_id TEXT,
            payload TEXT,
            error_message TEXT,
            failed_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()
    return conn


def get_mapped_task_id(conn, kraken_id: str):
    row = conn.execute(
        "SELECT odoo_task_id FROM ticket_map WHERE kraken_id = ?", (kraken_id,)
    ).fetchone()
    return row[0] if row else None


def save_mapping(conn, kraken_id: str, odoo_task_id: int, status: str):
    conn.execute(
        """INSERT INTO ticket_map (kraken_id, odoo_task_id, last_status)
           VALUES (?, ?, ?)
           ON CONFLICT(kraken_id) DO UPDATE SET
               odoo_task_id=excluded.odoo_task_id,
               last_status=excluded.last_status""",
        (kraken_id, odoo_task_id, status),
    )
    conn.commit()

def save_failed_ticket(conn, kraken_id: str, payload: dict, error_message: str):
    conn.execute(
        "INSERT INTO failed_tickets (kraken_id, payload, error_message) VALUES (?, ?, ?)",
        (kraken_id, json.dumps(payload), error_message),
    )
    conn.commit()


# Odoo connection

class OdooClient:
    def __init__(self):
        self.common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common", allow_none=True)
        self.uid = self.common.authenticate(ODOO_DB, ODOO_USERNAME, ODOO_API_KEY, {})
        if not self.uid:
            raise RuntimeError("Odoo authentication failed - check ODOO_* env vars")
        self.models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object", allow_none=True)
        self._project_id_cache = {}  # { project_name: project_id }
        self._department_id_cache = {}
        self._issue_type_id_cache = {}
        self._user_cache = {}  # { identifier: user_id }

    def _execute(self, model, method, *args, **kwargs):
        return self.models.execute_kw(ODOO_DB, self.uid, ODOO_API_KEY, model, method, list(args), kwargs)

    def get_user_id(self, identifier: str):
        if not identifier:
            return None
            
        # 1. Check manual JSON overrides first (user_mapping.json)
        if identifier in USER_MAP:
            return USER_MAP[identifier]
            
        # 2. Check local runtime cache
        if identifier in self._user_cache:
            return self._user_cache[identifier]
            
        # 3. Query Odoo dynamically (search by login/email, or exact name match)
        domain = [
            '|', ('login', 'ilike', identifier),
            '|', ('email', 'ilike', identifier),
                 ('name', 'ilike', identifier)
        ]
        
        try:
            ids = self._execute("res.users", "search", domain)
            if ids:
                self._user_cache[identifier] = ids[0]
                return ids[0]
        except Exception as e:
            log.warning("Failed to search Odoo for user '%s': %s", identifier, e)

        self._user_cache[identifier] = None
        return None

    def get_project_id(self, project_name: str) -> int:
        if project_name in self._project_id_cache:
            return self._project_id_cache[project_name]
        ids = self._execute("project.project", "search", [("name", "=", project_name)])
        if not ids:
            raise ValueError(f"Project '{project_name}' not found in Odoo")
        self._project_id_cache[project_name] = ids[0]
        return ids[0]

    def get_department_id(self, department_name: str):
        if department_name in self._department_id_cache:
            return self._department_id_cache[department_name]
        ids = self._execute("hr.department", "search", [("name", "=", department_name)])
        dept_id = ids[0] if ids else None
        self._department_id_cache[department_name] = dept_id
        return dept_id

    def get_issue_type_id(self, issue_type_name: str):
        if issue_type_name in self._issue_type_id_cache:
            return self._issue_type_id_cache[issue_type_name]
        ids = self._execute("project.task.issue.type", "search", [("name", "=", issue_type_name)])
        type_id = ids[0] if ids else None
        self._issue_type_id_cache[issue_type_name] = type_id
        return type_id

    def create_task(self, values: dict) -> int:
        return self._execute("project.task", "create", values)

    def update_task(self, task_id: int, values: dict):
        self._execute("project.task", "write", [task_id], values)


# Transform: Kraken ticketDTO -> Odoo project.task fields

def epoch_to_odoo_datetime(epoch_seconds) -> str | bool:
    if not epoch_seconds:
        return False
    dt = datetime.fromtimestamp(int(epoch_seconds), tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def load_mapping_file(path: str, fallback_key: str) -> tuple[dict, str]:
    with open(path) as f:
        data = json.load(f)
    return data["mapping"], data.get(fallback_key)


DEPARTMENT_MAPPING, FALLBACK_DEPARTMENT = load_mapping_file(
    DEPARTMENT_MAPPING_FILE, "fallback_department"
)
ISSUE_TYPE_MAPPING, FALLBACK_ISSUE_TYPE = load_mapping_file(
    ISSUE_TYPE_MAPPING_FILE, "fallback_issue_type"
)


def resolve_mapped_value(raw_value, mapping: dict, fallback: str, field_label: str) -> str:
    """Look up raw_value in mapping; fall back (with a warning) if missing or unreviewed."""
    mapped = mapping.get(raw_value, fallback)
    if mapped in (None, "NEEDS_REVIEW"):
        log.warning("%s '%s' unmapped, using fallback '%s'", field_label, raw_value, fallback)
        mapped = fallback
    return mapped


def transform_ticket(ticket: dict, odoo: OdooClient) -> dict:
    admin_group = ticket.get(ADMIN_GROUP_JSON_KEY)
    department_name = resolve_mapped_value(
        admin_group, DEPARTMENT_MAPPING, FALLBACK_DEPARTMENT, "Admin group"
    )
    department_id = odoo.get_department_id(department_name)

    issue_type_name = resolve_mapped_value(
        ticket.get("type"), ISSUE_TYPE_MAPPING, FALLBACK_ISSUE_TYPE, "Ticket type"
    )
    issue_type_id = odoo.get_issue_type_id(issue_type_name)

    log.info("DEBUG: adminGroup='%s' -> mapped to '%s' -> dept_id=%s", admin_group, department_name, department_id)

    assignee_id = odoo.get_user_id(ticket.get("assignedTo"))
    if assignee_id is None:
        log.warning("No Odoo user found for assignedTo='%s'", ticket.get("assignedTo"))

    reporter_id = odoo.get_user_id(ticket.get("owner") or ticket.get("createdBy"))

    description_parts = [ticket.get("description") or ""]
    if ticket.get("cause"):
        description_parts.append(f"<p><b>Cause:</b> {ticket['cause']}</p>")
    if ticket.get("solution"):
        description_parts.append(f"<p><b>Solution:</b> {ticket['solution']}</p>")

    target_project_name = PROJECT_MAP.get(admin_group, DEFAULT_PROJECT_NAME)

    values = {
        "name": f"[{ticket.get('serviceRecordNumber')}] {(ticket.get('description') or '')[:80]}",
        "description": "".join(description_parts),
        "project_id": odoo.get_project_id(target_project_name),
        "date_deadline": epoch_to_odoo_datetime(ticket.get("dueDate")),
        "date_start": epoch_to_odoo_datetime(ticket.get("createdTime")),
        "x_department_id": department_id,
        "x_priority": PRIORITY_MAP.get(ticket.get("priority")),
        "priority": PRIORITY_MAP.get(ticket.get("priority")),
        "x_issue_type": issue_type_id,
        "requestor_type": REQUESTOR_TYPE_DEFAULT,
    }
    if assignee_id:
        values["x_assignee_id"] = assignee_id
    if reporter_id:
        values["x_reporter_id"] = reporter_id
        
    # stage_id = STATUS_TO_STAGE_MAP.get(ticket.get("status"))
    # if stage_id:
    #     values["stage_id"] = stage_id

    return values


# Main consume loop

def extract_ticket_dto(raw_value: dict) -> dict:
    """Handle both a wrapped envelope ({"ticketDTO": {...}}) and a raw payload."""
    if "ticketDTO" in raw_value:
        return raw_value["ticketDTO"]
    return raw_value


def run():
    conn = init_db()
    odoo = OdooClient()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=True,
    )

    log.info("Listening on topic '%s' as group '%s'...", KAFKA_TOPIC, KAFKA_GROUP_ID)

    for message in consumer:
        try:
            ticket = extract_ticket_dto(message.value)
            kraken_id = str(ticket.get("id"))
            status = ticket.get("status")

            odoo_fields = transform_ticket(ticket, odoo)
            existing_task_id = get_mapped_task_id(conn, kraken_id)

            if existing_task_id:
                # Odoo Quirk: Sending project_id during an update forces Odoo to reset 
                # the task to the default 'Backlog' stage. We remove it for updates!
                odoo_fields.pop("project_id", None)
                
                odoo.update_task(existing_task_id, odoo_fields)
                save_mapping(conn, kraken_id, existing_task_id, status)
                log.info("Updated Odoo task %s for ticket %s (status=%s)", existing_task_id, kraken_id, status)
            else:
                new_task_id = odoo.create_task(odoo_fields)
                save_mapping(conn, kraken_id, new_task_id, status)
                log.info("Created Odoo task %s for ticket %s", new_task_id, kraken_id)

        except Exception as e:
            log.exception("Failed to process message at offset %s", message.offset)
            # Dead Letter Queue: Save failed tickets so they aren't permanently lost
            kraken_id = None
            try:
                ticket_data = extract_ticket_dto(message.value)
                kraken_id = str(ticket_data.get("id"))
            except Exception:
                pass
            
            save_failed_ticket(conn, kraken_id, message.value, str(e))


if __name__ == "__main__":
    run()
