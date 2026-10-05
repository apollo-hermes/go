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

BASE_DIR = Path(__file__).resolve().parent

# Load environment variables from the root folder
env = dotenv_values(BASE_DIR / ".env")

# Configuration

KAFKA_BOOTSTRAP_SERVERS = env.get("KAFKA_BOOTSTRAP_SERVERS") or os.environ.get("KAFKA_BOOTSTRAP_SERVERS") 
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
    "APPLICATIONS - AX": "Kraken - Applications",
}
DEFAULT_PROJECT_NAME = "Kraken Testing"  # Fallback if adminGroup isn't in PROJECT_MAP

DEPARTMENT_MAPPING_FILE = BASE_DIR / "admin_group_to_odoo_department.json"
IDEMPOTENCY_DB_FILE = os.environ.get("DB_FILE_PATH", BASE_DIR / "data" / "kraken_odoo_sync.db")
LOG_FILE = os.environ.get("LOG_FILE_PATH", BASE_DIR / "data" / "sync.log")

ADMIN_GROUP_JSON_KEY = "unitName"   # Confirmed from live message

# Mapped from Odoo's x_priority selection values
PRIORITY_MAP = {
    "CRITICAL": "3",
    "HIGH": "2",
    "MEDIUM": "1",
    "LOW": "0",
}

# Maps Kraken ticket status to Odoo Kanban stage Name dynamically
STATUS_TO_STAGE_NAME = {
    "OPEN": "Backlog",
    "ASSIGNED": "Planned for Sprint",
    "IN_PROGRESS": "In Progress",
    "ON_HOLD": "On Hold",
    "PENDING": "On Hold",
    "RESOLVED": "Done",
    "CLOSED": "Done",
}


ISSUE_TYPE_MAPPING_FILE = BASE_DIR / "ticket_type_to_odoo_issue_type.json"
REQUESTOR_TYPE_DEFAULT = "external"  # TODO: confirm valid selection value - Kraken tickets are all external

USER_MAPPING_FILE = BASE_DIR / "user_mapping copy.json"

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
        self._stage_id_cache = {}  # { 'project_id_stage_name': stage_id }
        self._partner_cache = {}  # { user_id: partner_id }

    def _execute(self, model, method, *args, **kwargs):
        return self.models.execute_kw(ODOO_DB, self.uid, ODOO_API_KEY, model, method, list(args), kwargs)

    def get_partner_id(self, user_id: int) -> int | None:
        if not user_id:
            return None
        if user_id in self._partner_cache:
            return self._partner_cache[user_id]
        try:
            users = self._execute("res.users", "read", [user_id], fields=["partner_id"])
            if users and users[0].get("partner_id"):
                partner_id = users[0]["partner_id"][0]
                self._partner_cache[user_id] = partner_id
                return partner_id
        except Exception as e:
            log.warning("Failed to look up partner for user %s: %s", user_id, e)
        return None

    def notify_assignee(self, task_id: int, assignee_id: int, task_name: str):
        partner_id = self.get_partner_id(assignee_id)
        if not partner_id:
            return
            
        body = f"<p>Hello,</p><p>You have been assigned to a new Kraken ticket: <b>{task_name}</b>.</p><p>Please review it in your Odoo tasks.</p>"
        try:
            self._execute(
                "project.task", "message_post", [task_id],
                body=body,
                subject=f"Assigned: {task_name}",
                message_type="comment",
                subtype_xmlid="mail.mt_comment",
                partner_ids=[partner_id]
            )
            log.info("Sent email notification to assignee (partner %s) for task %s", partner_id, task_id)
        except Exception as e:
            log.error("Failed to send email notification for task %s: %s", task_id, e)

    def get_stage_id(self, project_id: int, stage_name: str) -> int | None:
        """Dynamically look up stage ID by name within a specific project."""
        if not project_id or not stage_name:
            return None
        cache_key = f"{project_id}_{stage_name}"
        if cache_key in self._stage_id_cache:
            return self._stage_id_cache[cache_key]
            
        try:
            # _execute calls list(args), which wraps this domain in another list
            # So we pass [(...), (...)] and it becomes [[(...), (...)]] for Odoo
            domain = [("project_ids", "in", [project_id]), ("name", "=", stage_name)]
            stages = self.models.execute_kw(
                ODOO_DB, self.uid, ODOO_API_KEY,
                "project.task.type", "search_read",
                [domain],
                {"fields": ["id"], "limit": 1}
            )
            if stages:
                stage_id = stages[0]["id"]
                self._stage_id_cache[cache_key] = stage_id
                log.info("Resolved stage '%s' for project %s -> stage_id=%s", stage_name, project_id, stage_id)
                return stage_id
            else:
                log.warning("Stage '%s' not found for project %s", stage_name, project_id)
        except Exception as e:
            log.warning("Failed to look up stage '%s' for project %s: %s", stage_name, project_id, e)
        return None

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

    issue_raw = ticket.get("typeName") or ticket.get("type")
    issue_type_name = resolve_mapped_value(
        issue_raw, ISSUE_TYPE_MAPPING, FALLBACK_ISSUE_TYPE, "Ticket type"
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

    title = ticket.get('title') or ticket.get('description') or ''
    title = title[:80] if title else "Ticket"

    priority_raw = ticket.get("priorityName") or ticket.get("priority") or ""
    priority_val = PRIORITY_MAP.get(priority_raw.upper())

    values = {
        "name": f"[{ticket.get('serviceRecordNumber', 'NEW')}] {title}",
        "description": "".join(description_parts),
        "project_id": odoo.get_project_id(target_project_name),
        "date_deadline": epoch_to_odoo_datetime(ticket.get("dueDate")),
        "date_start": epoch_to_odoo_datetime(ticket.get("createdTime")),
        "x_department_id": department_id,
        "x_priority": priority_val,
        "priority": priority_val,
        "x_issue_type": issue_type_id,
        "requestor_type": REQUESTOR_TYPE_DEFAULT,
    }
    if assignee_id:
        values["x_assignee_id"] = assignee_id
        # Add both the assignee AND the API user (us) so we retain visibility
        user_id_list = list(set([assignee_id, odoo.uid]))
        values["user_ids"] = [(6, 0, user_id_list)]
    else:
        # No assignee mapped — assign to ourselves so task is visible
        values["user_ids"] = [(6, 0, [odoo.uid])]
    if reporter_id:
        values["x_reporter_id"] = reporter_id
        
    status_key = ticket.get("statusName") or ticket.get("status") or ""
    stage_name = STATUS_TO_STAGE_NAME.get(status_key.upper())
    if stage_name and values.get("project_id"):
        stage_id = odoo.get_stage_id(values["project_id"], stage_name)
        if stage_id:
            values["stage_id"] = stage_id

    return values


# Main consume loop

def extract_ticket_dto(raw_value: dict) -> dict:
    """Handle both a wrapped envelope and a raw payload."""
    if "incident" in raw_value:
        return raw_value["incident"]
    if "payload" in raw_value:
        return raw_value["payload"]
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
            log.info("RAW KRAKEN PAYLOAD RECEIVED: %s", ticket)
            
            kraken_id = str(ticket.get("id"))
            status = (ticket.get("statusName") or ticket.get("status") or "").upper()

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
                
                # Send email notification via Odoo's message_post if an assignee exists
                if "x_assignee_id" in odoo_fields:
                    odoo.notify_assignee(new_task_id, odoo_fields["x_assignee_id"], odoo_fields.get("name", "New Ticket"))

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
