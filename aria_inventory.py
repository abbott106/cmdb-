"""
VMware Aria Inventory Collector
Queries multiple vSphere/Aria instances and populates a PostgreSQL database.

Requirements:
    pip install requests psycopg2-binary python-dotenv pyyaml

Usage:
    python aria_inventory.py
"""

import os
import sys
import logging
import yaml
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from dotenv import load_dotenv

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("aria_inventory.log"),
    ],
)
log = logging.getLogger(__name__)

load_dotenv()  # Load credentials from .env file


# ── Database Connection ───────────────────────────────────────────────────────
def get_db_connection():
    """Create and return a PostgreSQL connection."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "vsphere_inventory"),
        user=os.getenv("DB_USER", "aria_user"),
        password=os.getenv("DB_PASSWORD"),
    )


def init_database(conn):
    """Create tables if they don't already exist."""
    ddl = """
    -- vSphere instances being monitored
    CREATE TABLE IF NOT EXISTS vsphere_instances (
        id          SERIAL PRIMARY KEY,
        name        VARCHAR(255) NOT NULL UNIQUE,
        hostname    VARCHAR(255) NOT NULL,
        last_synced TIMESTAMPTZ
    );

    -- Datacenters within each instance
    CREATE TABLE IF NOT EXISTS datacenters (
        id          SERIAL PRIMARY KEY,
        instance_id INTEGER REFERENCES vsphere_instances(id) ON DELETE CASCADE,
        name        VARCHAR(255) NOT NULL,
        moref       VARCHAR(255),
        UNIQUE (instance_id, moref)
    );

    -- Clusters within each datacenter
    CREATE TABLE IF NOT EXISTS clusters (
        id            SERIAL PRIMARY KEY,
        datacenter_id INTEGER REFERENCES datacenters(id) ON DELETE CASCADE,
        name          VARCHAR(255) NOT NULL,
        moref         VARCHAR(255),
        total_cpu     INTEGER,
        total_memory_gb NUMERIC(10,2),
        UNIQUE (datacenter_id, moref)
    );

    -- ESXi Hosts
    CREATE TABLE IF NOT EXISTS hosts (
        id              SERIAL PRIMARY KEY,
        cluster_id      INTEGER REFERENCES clusters(id) ON DELETE CASCADE,
        name            VARCHAR(255) NOT NULL,
        ip_address      VARCHAR(64),
        cpu_cores       INTEGER,
        memory_gb       NUMERIC(10,2),
        connection_state VARCHAR(64),
        power_state     VARCHAR(64),
        moref           VARCHAR(255),
        UNIQUE (cluster_id, moref)
    );

    -- Virtual Machines
    CREATE TABLE IF NOT EXISTS vms (
        id             SERIAL PRIMARY KEY,
        host_id        INTEGER REFERENCES hosts(id) ON DELETE CASCADE,
        name           VARCHAR(255) NOT NULL,
        power_state    VARCHAR(64),
        guest_os       VARCHAR(255),
        cpu_count      INTEGER,
        memory_gb      NUMERIC(10,2),
        ip_address     VARCHAR(64),
        dns_name       VARCHAR(255),
        moref          VARCHAR(255),
        num_disks      INTEGER,
        storage_gb     NUMERIC(10,2),
        UNIQUE (host_id, moref)
    );

    -- Datastores
    CREATE TABLE IF NOT EXISTS datastores (
        id           SERIAL PRIMARY KEY,
        instance_id  INTEGER REFERENCES vsphere_instances(id) ON DELETE CASCADE,
        name         VARCHAR(255) NOT NULL,
        type         VARCHAR(64),
        capacity_gb  NUMERIC(12,2),
        free_gb      NUMERIC(12,2),
        moref        VARCHAR(255),
        UNIQUE (instance_id, moref)
    );

    -- Networks
    CREATE TABLE IF NOT EXISTS networks (
        id          SERIAL PRIMARY KEY,
        instance_id INTEGER REFERENCES vsphere_instances(id) ON DELETE CASCADE,
        name        VARCHAR(255) NOT NULL,
        type        VARCHAR(64),
        moref       VARCHAR(255),
        UNIQUE (instance_id, moref)
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    log.info("Database schema initialized.")


# ── Aria / vSphere API Client ─────────────────────────────────────────────────
class AriaClient:
    """Thin REST client for VMware Aria Operations + vSphere REST API."""

    def __init__(self, hostname: str, username: str, password: str, verify_ssl: bool = True):
        self.base_aria = f"https://{hostname}/suite-api/api"
        self.base_vsphere = f"https://{hostname}/api"
        self.username = username
        self.password = password
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        self.session.verify = verify_ssl
        self.token = None

        if not verify_ssl:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # ── Authentication ──────────────────────────────────────────────────────

    def authenticate(self):
        """Acquire an Aria Operations auth token."""
        url = f"{self.base_aria}/auth/token/acquire"
        payload = {
            "username": self.username,
            "password": self.password,
            "authSource": "LOCAL",
        }
        resp = self.session.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        self.token = resp.json().get("token")
        self.session.headers.update({
            "Authorization": f"vRealizeOpsToken {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        log.info(f"Authenticated to Aria on {self.base_aria}")

    def authenticate_vsphere(self):
        """Acquire a vSphere REST API session token."""
        url = f"{self.base_vsphere}/session"
        resp = self.session.post(url, auth=(self.username, self.password), timeout=30)
        resp.raise_for_status()
        vsphere_token = resp.json()
        self.session.headers.update({"vmware-api-session-id": vsphere_token})
        log.info("Authenticated to vSphere REST API.")

    # ── Paginated GET helper ────────────────────────────────────────────────

    def _paginate_aria(self, endpoint: str, result_key: str, page_size: int = 1000) -> list:
        """Fetch all pages from an Aria Operations list endpoint."""
        results = []
        page = 0
        while True:
            url = f"{self.base_aria}/{endpoint}"
            params = {"pageSize": page_size, "page": page}
            resp = self.session.get(url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            items = data.get(result_key, [])
            results.extend(items)
            if len(items) < page_size:
                break
            page += 1
        return results

    def _get_vsphere(self, endpoint: str, params: dict = None) -> list:
        """GET from the vSphere REST API (handles simple lists)."""
        url = f"{self.base_vsphere}/{endpoint}"
        resp = self.session.get(url, params=params or {}, timeout=60)
        resp.raise_for_status()
        return resp.json()

    # ── Resource collectors ─────────────────────────────────────────────────

    def get_datacenters(self) -> list:
        """Fetch all datacenters via vSphere REST API."""
        raw = self._get_vsphere("vcenter/datacenter")
        return [
            {
                "name": dc.get("name"),
                "moref": dc.get("datacenter"),
            }
            for dc in raw
        ]

    def get_clusters(self, datacenter_moref: str) -> list:
        """Fetch clusters in a given datacenter."""
        raw = self._get_vsphere("vcenter/cluster", {"datacenters": datacenter_moref})
        return [
            {
                "name": c.get("name"),
                "moref": c.get("cluster"),
            }
            for c in raw
        ]

    def get_hosts(self, cluster_moref: str) -> list:
        """Fetch hosts in a given cluster."""
        raw = self._get_vsphere("vcenter/host", {"clusters": cluster_moref})
        return [
            {
                "name": h.get("name"),
                "moref": h.get("host"),
                "connection_state": h.get("connection_state"),
                "power_state": h.get("power_state"),
            }
            for h in raw
        ]

    def get_vms(self, host_moref: str) -> list:
        """Fetch VMs on a given host."""
        raw = self._get_vsphere("vcenter/vm", {"hosts": host_moref})
        vms = []
        for vm in raw:
            vms.append({
                "name": vm.get("name"),
                "moref": vm.get("vm"),
                "power_state": vm.get("power_state"),
                "cpu_count": vm.get("cpu_count"),
                "memory_gb": round(vm.get("memory_size_MiB", 0) / 1024, 2) if vm.get("memory_size_MiB") else None,
                "guest_os": vm.get("guest_OS"),
            })
        return vms

    def get_datastores(self) -> list:
        """Fetch all datastores."""
        raw = self._get_vsphere("vcenter/datastore")
        return [
            {
                "name": ds.get("name"),
                "moref": ds.get("datastore"),
                "type": ds.get("type"),
                "capacity_gb": round(ds.get("capacity", 0) / (1024 ** 3), 2) if ds.get("capacity") else None,
                "free_gb": round(ds.get("free_space", 0) / (1024 ** 3), 2) if ds.get("free_space") else None,
            }
            for ds in raw
        ]

    def get_networks(self) -> list:
        """Fetch all networks."""
        raw = self._get_vsphere("vcenter/network")
        return [
            {
                "name": n.get("name"),
                "moref": n.get("network"),
                "type": n.get("type"),
            }
            for n in raw
        ]


# ── Database Upsert Helpers ───────────────────────────────────────────────────

def upsert_instance(conn, name: str, hostname: str) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO vsphere_instances (name, hostname)
            VALUES (%s, %s)
            ON CONFLICT (name) DO UPDATE SET hostname = EXCLUDED.hostname
            RETURNING id
        """, (name, hostname))
        return cur.fetchone()[0]


def update_last_synced(conn, instance_id: int):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE vsphere_instances SET last_synced = %s WHERE id = %s",
            (datetime.utcnow(), instance_id),
        )


def upsert_datacenter(conn, instance_id: int, dc: dict) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO datacenters (instance_id, name, moref)
            VALUES (%s, %s, %s)
            ON CONFLICT (instance_id, moref) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
        """, (instance_id, dc["name"], dc["moref"]))
        return cur.fetchone()[0]


def upsert_cluster(conn, datacenter_id: int, cluster: dict) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO clusters (datacenter_id, name, moref)
            VALUES (%s, %s, %s)
            ON CONFLICT (datacenter_id, moref) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
        """, (datacenter_id, cluster["name"], cluster["moref"]))
        return cur.fetchone()[0]


def upsert_host(conn, cluster_id: int, host: dict) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO hosts (cluster_id, name, moref, connection_state, power_state)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (cluster_id, moref) DO UPDATE SET
                name = EXCLUDED.name,
                connection_state = EXCLUDED.connection_state,
                power_state = EXCLUDED.power_state
            RETURNING id
        """, (cluster_id, host["name"], host["moref"],
              host.get("connection_state"), host.get("power_state")))
        return cur.fetchone()[0]


def upsert_vm(conn, host_id: int, vm: dict):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO vms (host_id, name, moref, power_state, cpu_count, memory_gb, guest_os)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (host_id, moref) DO UPDATE SET
                name        = EXCLUDED.name,
                power_state = EXCLUDED.power_state,
                cpu_count   = EXCLUDED.cpu_count,
                memory_gb   = EXCLUDED.memory_gb,
                guest_os    = EXCLUDED.guest_os
        """, (host_id, vm["name"], vm["moref"], vm.get("power_state"),
              vm.get("cpu_count"), vm.get("memory_gb"), vm.get("guest_os")))


def upsert_datastore(conn, instance_id: int, ds: dict):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO datastores (instance_id, name, moref, type, capacity_gb, free_gb)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (instance_id, moref) DO UPDATE SET
                name        = EXCLUDED.name,
                type        = EXCLUDED.type,
                capacity_gb = EXCLUDED.capacity_gb,
                free_gb     = EXCLUDED.free_gb
        """, (instance_id, ds["name"], ds["moref"], ds.get("type"),
              ds.get("capacity_gb"), ds.get("free_gb")))


def upsert_network(conn, instance_id: int, net: dict):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO networks (instance_id, name, moref, type)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (instance_id, moref) DO UPDATE SET
                name = EXCLUDED.name,
                type = EXCLUDED.type
        """, (instance_id, net["name"], net["moref"], net.get("type")))


# ── Main Collection Loop ──────────────────────────────────────────────────────

def collect_instance(conn, instance_cfg: dict):
    """Run a full inventory collection for one vSphere instance."""
    name = instance_cfg["name"]
    hostname = instance_cfg["hostname"]
    username = instance_cfg["username"]
    password = instance_cfg.get("password") or os.getenv(instance_cfg.get("password_env", ""))
    verify_ssl = instance_cfg.get("verify_ssl", True)

    log.info(f"=== Starting collection for instance: {name} ({hostname}) ===")

    client = AriaClient(hostname, username, password, verify_ssl)

    try:
        client.authenticate_vsphere()
    except Exception as e:
        log.error(f"Authentication failed for {name}: {e}")
        return

    instance_id = upsert_instance(conn, name, hostname)

    # ── Datacenters ──
    try:
        datacenters = client.get_datacenters()
        log.info(f"  Found {len(datacenters)} datacenters")
    except Exception as e:
        log.error(f"  Failed to fetch datacenters: {e}")
        datacenters = []

    for dc in datacenters:
        dc_id = upsert_datacenter(conn, instance_id, dc)

        # ── Clusters ──
        try:
            clusters = client.get_clusters(dc["moref"])
        except Exception as e:
            log.warning(f"    Could not fetch clusters for DC {dc['name']}: {e}")
            clusters = []

        for cluster in clusters:
            cluster_id = upsert_cluster(conn, dc_id, cluster)

            # ── Hosts ──
            try:
                hosts = client.get_hosts(cluster["moref"])
            except Exception as e:
                log.warning(f"      Could not fetch hosts for cluster {cluster['name']}: {e}")
                hosts = []

            for host in hosts:
                host_id = upsert_host(conn, cluster_id, host)

                # ── VMs ──
                try:
                    vms = client.get_vms(host["moref"])
                    for vm in vms:
                        upsert_vm(conn, host_id, vm)
                    log.info(f"        Host {host['name']}: {len(vms)} VMs")
                except Exception as e:
                    log.warning(f"        Could not fetch VMs for host {host['name']}: {e}")

    # ── Datastores ──
    try:
        datastores = client.get_datastores()
        for ds in datastores:
            upsert_datastore(conn, instance_id, ds)
        log.info(f"  Datastores: {len(datastores)}")
    except Exception as e:
        log.warning(f"  Could not fetch datastores: {e}")

    # ── Networks ──
    try:
        networks = client.get_networks()
        for net in networks:
            upsert_network(conn, instance_id, net)
        log.info(f"  Networks: {len(networks)}")
    except Exception as e:
        log.warning(f"  Could not fetch networks: {e}")

    conn.commit()
    update_last_synced(conn, instance_id)
    conn.commit()
    log.info(f"=== Finished collection for: {name} ===\n")


def load_config(path: str = "instances.yaml") -> list:
    """Load vSphere instance definitions from YAML config."""
    with open(path) as f:
        cfg = yaml.safe_load(f)
    return cfg.get("instances", [])


def main():
    config_path = os.getenv("CONFIG_PATH", "instances.yaml")

    try:
        instances = load_config(config_path)
    except FileNotFoundError:
        log.error(f"Config file not found: {config_path}")
        sys.exit(1)

    try:
        conn = get_db_connection()
    except Exception as e:
        log.error(f"Could not connect to database: {e}")
        sys.exit(1)

    init_database(conn)

    for instance in instances:
        try:
            collect_instance(conn, instance)
        except Exception as e:
            log.error(f"Unhandled error for instance {instance.get('name')}: {e}")
            conn.rollback()

    conn.close()
    log.info("All instances processed. Done.")


if __name__ == "__main__":
    main()
