"""
VMware Aria Operations Inventory Collector
Queries Aria Operations (vRealize Operations) API and populates a PostgreSQL database.

Aria Operations already monitors all your vCenters — this script queries Aria once
and pulls inventory across every vCenter Aria knows about.

Collects:
    - vCenters, Datacenters, Clusters, Hosts
    - Virtual Machines + NIC cards
    - Datastores, Networks

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
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("aria_inventory.log"),
    ],
)
log = logging.getLogger(__name__)

load_dotenv(dotenv_path=Path(__file__).parent / ".env")

# ── Aria Operations Resource Kind Constants ───────────────────────────────────
ADAPTER_KIND   = "VMWARE"
KIND_VCENTER   = "VMwareAdapter Instance"
KIND_DC        = "Datacenter"
KIND_CLUSTER   = "ClusterComputeResource"
KIND_HOST      = "HostSystem"
KIND_VM        = "VirtualMachine"
KIND_DATASTORE = "Datastore"
KIND_NETWORK   = "DistributedVirtualPortgroup"


# ── Database Connection ───────────────────────────────────────────────────────
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "vsphere_inventory"),
        user=os.getenv("DB_USER", "aria_user"),
        password=os.getenv("DB_PASSWORD"),
    )


def init_database(conn):
    """Create all tables if they don't exist."""
    ddl = """
    CREATE TABLE IF NOT EXISTS aria_instances (
        id          SERIAL PRIMARY KEY,
        name        VARCHAR(255) NOT NULL UNIQUE,
        hostname    VARCHAR(255) NOT NULL,
        last_synced TIMESTAMPTZ
    );

    CREATE TABLE IF NOT EXISTS vcenters (
        id           SERIAL PRIMARY KEY,
        aria_id      INTEGER REFERENCES aria_instances(id) ON DELETE CASCADE,
        name         VARCHAR(255),
        aria_uuid    VARCHAR(255) UNIQUE,
        version      VARCHAR(64),
        health_state VARCHAR(64),
        last_synced  TIMESTAMPTZ
    );

    CREATE TABLE IF NOT EXISTS datacenters (
        id          SERIAL PRIMARY KEY,
        vcenter_id  INTEGER REFERENCES vcenters(id) ON DELETE CASCADE,
        name        VARCHAR(255),
        aria_uuid   VARCHAR(255) UNIQUE
    );

    CREATE TABLE IF NOT EXISTS clusters (
        id               SERIAL PRIMARY KEY,
        datacenter_id    INTEGER REFERENCES datacenters(id) ON DELETE CASCADE,
        name             VARCHAR(255),
        aria_uuid        VARCHAR(255) UNIQUE,
        num_hosts        INTEGER,
        health_state     VARCHAR(64)
    );

    CREATE TABLE IF NOT EXISTS hosts (
        id               SERIAL PRIMARY KEY,
        cluster_id       INTEGER REFERENCES clusters(id) ON DELETE CASCADE,
        name             VARCHAR(255),
        aria_uuid        VARCHAR(255) UNIQUE,
        cpu_cores        INTEGER,
        memory_gb        NUMERIC(10,2),
        connection_state VARCHAR(64),
        power_state      VARCHAR(64),
        health_state     VARCHAR(64),
        version          VARCHAR(64),
        num_vms          INTEGER
    );

    CREATE TABLE IF NOT EXISTS vms (
        id             SERIAL PRIMARY KEY,
        host_id        INTEGER REFERENCES hosts(id) ON DELETE CASCADE,
        name           VARCHAR(255),
        aria_uuid      VARCHAR(255) UNIQUE,
        power_state    VARCHAR(64),
        guest_os       VARCHAR(255),
        cpu_count      INTEGER,
        memory_gb      NUMERIC(10,2),
        ip_address     VARCHAR(64),
        dns_name       VARCHAR(255),
        storage_gb     NUMERIC(12,2),
        num_disks      INTEGER,
        snapshot_count INTEGER,
        health_state   VARCHAR(64)
    );

    CREATE TABLE IF NOT EXISTS vm_nics (
        id           SERIAL PRIMARY KEY,
        vm_id        INTEGER REFERENCES vms(id) ON DELETE CASCADE,
        nic_key      VARCHAR(64),
        label        VARCHAR(255),
        mac_address  VARCHAR(64),
        connected    BOOLEAN,
        network_name VARCHAR(255),
        adapter_type VARCHAR(64),
        ip_address   VARCHAR(64),
        UNIQUE (vm_id, nic_key)
    );

    CREATE TABLE IF NOT EXISTS datastores (
        id           SERIAL PRIMARY KEY,
        vcenter_id   INTEGER REFERENCES vcenters(id) ON DELETE CASCADE,
        name         VARCHAR(255),
        aria_uuid    VARCHAR(255) UNIQUE,
        type         VARCHAR(64),
        capacity_gb  NUMERIC(12,2),
        free_gb      NUMERIC(12,2),
        health_state VARCHAR(64)
    );

    CREATE TABLE IF NOT EXISTS networks (
        id           SERIAL PRIMARY KEY,
        vcenter_id   INTEGER REFERENCES vcenters(id) ON DELETE CASCADE,
        name         VARCHAR(255),
        aria_uuid    VARCHAR(255) UNIQUE,
        type         VARCHAR(64),
        vlan_id      VARCHAR(64)
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    log.info("Database schema initialized.")


# ── Aria Operations API Client ────────────────────────────────────────────────
class AriaOpsClient:
    """
    Client for the VMware Aria Operations REST API.
    API base: https://<aria-hostname>/suite-api/api/
    """

    def __init__(self, hostname: str, username: str, password: str,
                 auth_source: str = "LOCAL", verify_ssl: bool = True):
        self.base = f"https://{hostname}/suite-api/api"
        self.hostname = hostname
        self.username = username
        self.password = password
        self.auth_source = auth_source
        self.verify_ssl = verify_ssl
        self.session = requests.Session()
        self.session.verify = verify_ssl
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        if not verify_ssl:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def authenticate(self):
        """Acquire Aria Operations auth token."""
        url = f"{self.base}/auth/token/acquire"
        payload = {
            "username": self.username,
            "password": self.password,
            "authSource": self.auth_source,
        }
        resp = self.session.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        token = resp.json().get("token")
        if not token:
            raise ValueError("No token returned — check credentials and authSource")
        self.session.headers.update({"Authorization": f"vRealizeOpsToken {token}"})
        log.info(f"Authenticated to Aria Operations: {self.hostname}")

    def release_token(self):
        try:
            self.session.post(f"{self.base}/auth/token/release", timeout=10)
        except Exception:
            pass

    # ── Paginated resource fetcher ────────────────────────────────────────────

    def get_resources(self, resource_kind: str, adapter_kind: str = ADAPTER_KIND,
                      page_size: int = 1000) -> list:
        """Fetch all resources of a given kind, handling pagination."""
        resources = []
        page = 0
        url = f"{self.base}/resources"
        while True:
            params = {
                "adapterKind": adapter_kind,
                "resourceKind": resource_kind,
                "pageSize": page_size,
                "page": page,
            }
            resp = self.session.get(url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            items = data.get("resourceList", [])
            resources.extend(items)
            total = data.get("pageInfo", {}).get("totalCount", 0)
            if len(resources) >= total or len(items) < page_size:
                break
            page += 1
        log.info(f"  {resource_kind}: {len(resources)} found")
        return resources

    # ── Property fetcher ──────────────────────────────────────────────────────

    def get_properties(self, resource_uuid: str, prop_keys: list) -> dict:
        """Fetch named properties for a resource. Returns {key: value}."""
        url = f"{self.base}/resources/{resource_uuid}/properties"
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            return {
                p["name"]: p.get("value")
                for p in resp.json().get("property", [])
                if p.get("name") in prop_keys
            }
        except Exception as e:
            log.debug(f"    Properties fetch failed for {resource_uuid}: {e}")
            return {}

    def get_all_properties(self, resource_uuid: str) -> dict:
        """Fetch ALL properties for a resource — used for NIC discovery."""
        url = f"{self.base}/resources/{resource_uuid}/properties"
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            return {
                p["name"]: p.get("value")
                for p in resp.json().get("property", [])
            }
        except Exception as e:
            log.debug(f"    All-properties fetch failed for {resource_uuid}: {e}")
            return {}

    # ── Parent relationship lookup ────────────────────────────────────────────

    def get_parent_uuid(self, resource_uuid: str, parent_kind: str) -> str:
        """Return the UUID of the nearest parent of a given resourceKind."""
        url = f"{self.base}/resources/{resource_uuid}/relationships/parents"
        try:
            resp = self.session.get(url, params={"resourceKind": parent_kind}, timeout=30)
            resp.raise_for_status()
            items = resp.json().get("resourceList", [])
            if items:
                return items[0].get("identifier")
        except Exception:
            pass
        return None

    # ── High-level collectors ─────────────────────────────────────────────────

    def collect_vcenters(self) -> list:
        raw = self.get_resources(KIND_VCENTER)
        result = []
        for r in raw:
            uuid = r.get("identifier")
            props = self.get_properties(uuid, ["summary|version"])
            result.append({
                "aria_uuid": uuid,
                "name": r.get("resourceKey", {}).get("name"),
                "health_state": _health(r),
                "version": props.get("summary|version"),
            })
        return result

    def collect_datacenters(self) -> list:
        raw = self.get_resources(KIND_DC)
        return [{
            "aria_uuid": r.get("identifier"),
            "name": r.get("resourceKey", {}).get("name"),
            "parent_vcenter_uuid": self.get_parent_uuid(r.get("identifier"), KIND_VCENTER),
        } for r in raw]

    def collect_clusters(self) -> list:
        raw = self.get_resources(KIND_CLUSTER)
        result = []
        for r in raw:
            uuid = r.get("identifier")
            props = self.get_properties(uuid, ["summary|number_hosts"])
            result.append({
                "aria_uuid": uuid,
                "name": r.get("resourceKey", {}).get("name"),
                "health_state": _health(r),
                "num_hosts": _safe_int(props.get("summary|number_hosts")),
                "parent_dc_uuid": self.get_parent_uuid(uuid, KIND_DC),
            })
        return result

    def collect_hosts(self) -> list:
        raw = self.get_resources(KIND_HOST)
        result = []
        for r in raw:
            uuid = r.get("identifier")
            props = self.get_properties(uuid, [
                "summary|runtime|powerState",
                "summary|runtime|connectionState",
                "summary|hardware|numCpuCores",
                "summary|hardware|memorySize",
                "summary|config|product|version",
                "summary|number_running_vms",
            ])
            result.append({
                "aria_uuid": uuid,
                "name": r.get("resourceKey", {}).get("name"),
                "health_state": _health(r),
                "power_state": props.get("summary|runtime|powerState"),
                "connection_state": props.get("summary|runtime|connectionState"),
                "cpu_cores": _safe_int(props.get("summary|hardware|numCpuCores")),
                "memory_gb": _safe_gb(props.get("summary|hardware|memorySize"), "bytes"),
                "version": props.get("summary|config|product|version"),
                "num_vms": _safe_int(props.get("summary|number_running_vms")),
                "parent_cluster_uuid": self.get_parent_uuid(uuid, KIND_CLUSTER),
            })
        return result

    def collect_vms(self) -> list:
        raw = self.get_resources(KIND_VM)
        result = []
        for r in raw:
            uuid = r.get("identifier")
            props = self.get_properties(uuid, [
                "summary|runtime|powerState",
                "config|guestFullName",
                "config|hardware|numCpu",
                "config|hardware|memoryKB",
                "summary|guest|ipAddress",
                "summary|guest|hostName",
                "summary|storage|committed",
                "config|hardware|numVirtualDisks",
                "snapshot|count",
            ])
            result.append({
                "aria_uuid": uuid,
                "name": r.get("resourceKey", {}).get("name"),
                "health_state": _health(r),
                "power_state": props.get("summary|runtime|powerState"),
                "guest_os": props.get("config|guestFullName"),
                "cpu_count": _safe_int(props.get("config|hardware|numCpu")),
                "memory_gb": _safe_gb(props.get("config|hardware|memoryKB"), "kb"),
                "ip_address": props.get("summary|guest|ipAddress"),
                "dns_name": props.get("summary|guest|hostName"),
                "storage_gb": _safe_gb(props.get("summary|storage|committed"), "bytes"),
                "num_disks": _safe_int(props.get("config|hardware|numVirtualDisks")),
                "snapshot_count": _safe_int(props.get("snapshot|count")),
                "parent_host_uuid": self.get_parent_uuid(uuid, KIND_HOST),
            })
        return result

    def collect_vm_nics(self, vm_uuid: str) -> list:
        """
        Fetch NIC details for a VM from Aria property store.
        Aria stores multi-NIC properties with an index suffix e.g. [0], [1].
        For single NIC VMs the index is omitted.
        """
        all_props = self.get_all_properties(vm_uuid)

        # Filter only NIC-related properties
        nic_props = {k: v for k, v in all_props.items()
                     if any(term in k.lower() for term in
                            ["virtualethernetcard", "net|portgroup", "net|ipaddress"])}

        if not nic_props:
            return []

        # Determine how many NICs by counting unique mac address entries
        nics = []
        found_indexed = False

        # Try indexed NICs first (multi-NIC VMs)
        for i in range(16):  # max 16 NICs per VM
            mac = (
                nic_props.get(f"config|hardware|device|VirtualEthernetCard|macAddress[{i}]")
            )
            if not mac:
                if i > 0:
                    break
                # Fall through to non-indexed check
                continue

            found_indexed = True
            nics.append({
                "nic_key": str(i),
                "mac_address": mac,
                "label": nic_props.get(
                    f"config|hardware|device|VirtualEthernetCard|deviceInfo|label[{i}]"
                ),
                "adapter_type": nic_props.get(
                    f"config|hardware|device|VirtualEthernetCard|adapterType[{i}]"
                ),
                "connected": nic_props.get(
                    f"config|hardware|device|VirtualEthernetCard|connectable|connected[{i}]"
                ) == "true",
                "network_name": nic_props.get(f"net|portgroup[{i}]"),
                "ip_address": nic_props.get(f"net|ipAddress[{i}]"),
            })

        # Single NIC fallback — no index suffix
        if not found_indexed:
            mac = nic_props.get("config|hardware|device|VirtualEthernetCard|macAddress")
            if mac:
                nics.append({
                    "nic_key": "0",
                    "mac_address": mac,
                    "label": nic_props.get(
                        "config|hardware|device|VirtualEthernetCard|deviceInfo|label"
                    ),
                    "adapter_type": nic_props.get(
                        "config|hardware|device|VirtualEthernetCard|adapterType"
                    ),
                    "connected": nic_props.get(
                        "config|hardware|device|VirtualEthernetCard|connectable|connected"
                    ) == "true",
                    "network_name": nic_props.get("net|portgroup"),
                    "ip_address": nic_props.get("net|ipAddress"),
                })

        return nics

    def collect_datastores(self) -> list:
        raw = self.get_resources(KIND_DATASTORE)
        result = []
        for r in raw:
            uuid = r.get("identifier")
            props = self.get_properties(uuid, [
                "summary|type",
                "summary|capacity",
                "summary|freeSpace",
            ])
            result.append({
                "aria_uuid": uuid,
                "name": r.get("resourceKey", {}).get("name"),
                "health_state": _health(r),
                "type": props.get("summary|type"),
                "capacity_gb": _safe_gb(props.get("summary|capacity"), "bytes"),
                "free_gb": _safe_gb(props.get("summary|freeSpace"), "bytes"),
                "parent_vcenter_uuid": self.get_parent_uuid(uuid, KIND_VCENTER),
            })
        return result

    def collect_networks(self) -> list:
        raw = self.get_resources(KIND_NETWORK)
        result = []
        for r in raw:
            uuid = r.get("identifier")
            props = self.get_properties(uuid, [
                "summary|type",
                "config|defaultPortConfig|vlan|vlanId",
            ])
            result.append({
                "aria_uuid": uuid,
                "name": r.get("resourceKey", {}).get("name"),
                "type": props.get("summary|type"),
                "vlan_id": props.get("config|defaultPortConfig|vlan|vlanId"),
                "parent_vcenter_uuid": self.get_parent_uuid(uuid, KIND_VCENTER),
            })
        return result


# ── Utility helpers ───────────────────────────────────────────────────────────

def _health(resource: dict) -> str:
    states = resource.get("resourceStatusStates", [{}])
    return states[0].get("healthState", "UNKNOWN") if states else "UNKNOWN"

def _safe_int(val) -> int:
    try:
        return int(float(val)) if val is not None else None
    except (ValueError, TypeError):
        return None

def _safe_gb(val, unit="bytes") -> float:
    try:
        v = float(val)
        if unit == "bytes": return round(v / (1024 ** 3), 2)
        if unit == "kb":    return round(v / (1024 ** 2), 2)
        if unit == "mb":    return round(v / 1024, 2)
        return round(v, 2)
    except (ValueError, TypeError):
        return None


# ── Database upserts ──────────────────────────────────────────────────────────

def upsert_aria_instance(conn, name, hostname):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO aria_instances (name, hostname)
            VALUES (%s, %s)
            ON CONFLICT (name) DO UPDATE SET hostname = EXCLUDED.hostname
            RETURNING id
        """, (name, hostname))
        return cur.fetchone()[0]

def upsert_vcenter(conn, aria_id, vc):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO vcenters (aria_id, name, aria_uuid, version, health_state)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (aria_uuid) DO UPDATE SET
                name         = EXCLUDED.name,
                version      = EXCLUDED.version,
                health_state = EXCLUDED.health_state
            RETURNING id
        """, (aria_id, vc["name"], vc["aria_uuid"],
              vc.get("version"), vc.get("health_state")))
        return cur.fetchone()[0]

def upsert_datacenter(conn, vcenter_id, dc):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO datacenters (vcenter_id, name, aria_uuid)
            VALUES (%s, %s, %s)
            ON CONFLICT (aria_uuid) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
        """, (vcenter_id, dc["name"], dc["aria_uuid"]))
        return cur.fetchone()[0]

def upsert_cluster(conn, datacenter_id, cluster):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO clusters (datacenter_id, name, aria_uuid, num_hosts, health_state)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (aria_uuid) DO UPDATE SET
                name = EXCLUDED.name, num_hosts = EXCLUDED.num_hosts,
                health_state = EXCLUDED.health_state
            RETURNING id
        """, (datacenter_id, cluster["name"], cluster["aria_uuid"],
              cluster.get("num_hosts"), cluster.get("health_state")))
        return cur.fetchone()[0]

def upsert_host(conn, cluster_id, host):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO hosts
                (cluster_id, name, aria_uuid, cpu_cores, memory_gb,
                 connection_state, power_state, health_state, version, num_vms)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (aria_uuid) DO UPDATE SET
                name             = EXCLUDED.name,
                cpu_cores        = EXCLUDED.cpu_cores,
                memory_gb        = EXCLUDED.memory_gb,
                connection_state = EXCLUDED.connection_state,
                power_state      = EXCLUDED.power_state,
                health_state     = EXCLUDED.health_state,
                version          = EXCLUDED.version,
                num_vms          = EXCLUDED.num_vms
            RETURNING id
        """, (cluster_id, host["name"], host["aria_uuid"],
              host.get("cpu_cores"), host.get("memory_gb"),
              host.get("connection_state"), host.get("power_state"),
              host.get("health_state"), host.get("version"), host.get("num_vms")))
        return cur.fetchone()[0]

def upsert_vm(conn, host_id, vm):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO vms
                (host_id, name, aria_uuid, power_state, guest_os, cpu_count,
                 memory_gb, ip_address, dns_name, storage_gb, num_disks,
                 snapshot_count, health_state)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (aria_uuid) DO UPDATE SET
                name           = EXCLUDED.name,
                power_state    = EXCLUDED.power_state,
                guest_os       = EXCLUDED.guest_os,
                cpu_count      = EXCLUDED.cpu_count,
                memory_gb      = EXCLUDED.memory_gb,
                ip_address     = EXCLUDED.ip_address,
                dns_name       = EXCLUDED.dns_name,
                storage_gb     = EXCLUDED.storage_gb,
                num_disks      = EXCLUDED.num_disks,
                snapshot_count = EXCLUDED.snapshot_count,
                health_state   = EXCLUDED.health_state
            RETURNING id
        """, (host_id, vm["name"], vm["aria_uuid"], vm.get("power_state"),
              vm.get("guest_os"), vm.get("cpu_count"), vm.get("memory_gb"),
              vm.get("ip_address"), vm.get("dns_name"), vm.get("storage_gb"),
              vm.get("num_disks"), vm.get("snapshot_count"), vm.get("health_state")))
        return cur.fetchone()[0]

def upsert_vm_nic(conn, vm_id, nic):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO vm_nics
                (vm_id, nic_key, label, mac_address, connected,
                 network_name, adapter_type, ip_address)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (vm_id, nic_key) DO UPDATE SET
                label        = EXCLUDED.label,
                mac_address  = EXCLUDED.mac_address,
                connected    = EXCLUDED.connected,
                network_name = EXCLUDED.network_name,
                adapter_type = EXCLUDED.adapter_type,
                ip_address   = EXCLUDED.ip_address
        """, (vm_id, nic.get("nic_key"), nic.get("label"),
              nic.get("mac_address"), nic.get("connected"),
              nic.get("network_name"), nic.get("adapter_type"),
              nic.get("ip_address")))

def upsert_datastore(conn, vcenter_id, ds):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO datastores
                (vcenter_id, name, aria_uuid, type, capacity_gb, free_gb, health_state)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (aria_uuid) DO UPDATE SET
                name         = EXCLUDED.name,
                type         = EXCLUDED.type,
                capacity_gb  = EXCLUDED.capacity_gb,
                free_gb      = EXCLUDED.free_gb,
                health_state = EXCLUDED.health_state
        """, (vcenter_id, ds["name"], ds["aria_uuid"], ds.get("type"),
              ds.get("capacity_gb"), ds.get("free_gb"), ds.get("health_state")))

def upsert_network(conn, vcenter_id, net):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO networks (vcenter_id, name, aria_uuid, type, vlan_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (aria_uuid) DO UPDATE SET
                name    = EXCLUDED.name,
                type    = EXCLUDED.type,
                vlan_id = EXCLUDED.vlan_id
        """, (vcenter_id, net["name"], net["aria_uuid"],
              net.get("type"), net.get("vlan_id")))


# ── Main collection loop ──────────────────────────────────────────────────────

def collect_instance(conn, instance_cfg: dict):
    name       = instance_cfg["name"]
    hostname   = instance_cfg["hostname"]
    username   = instance_cfg["username"]
    password   = instance_cfg.get("password") or os.getenv(instance_cfg.get("password_env", ""))
    auth_src   = instance_cfg.get("auth_source", "LOCAL")
    verify_ssl = instance_cfg.get("verify_ssl", True)

    log.info(f"=== Starting Aria Operations collection: {name} ({hostname}) ===")

    client = AriaOpsClient(hostname, username, password, auth_src, verify_ssl)
    try:
        client.authenticate()
    except Exception as e:
        log.error(f"  Authentication failed: {e}")
        return

    aria_id = upsert_aria_instance(conn, name, hostname)

    # 1. vCenters
    log.info("  Collecting vCenters...")
    vcenters = client.collect_vcenters()
    vc_uuid_to_id = {}
    for vc in vcenters:
        vc_id = upsert_vcenter(conn, aria_id, vc)
        vc_uuid_to_id[vc["aria_uuid"]] = vc_id
    default_vc_id = next(iter(vc_uuid_to_id.values()), None)

    # 2. Datacenters
    log.info("  Collecting Datacenters...")
    datacenters = client.collect_datacenters()
    dc_uuid_to_id = {}
    for dc in datacenters:
        vc_id = vc_uuid_to_id.get(dc.get("parent_vcenter_uuid"), default_vc_id)
        if vc_id:
            dc_id = upsert_datacenter(conn, vc_id, dc)
            dc_uuid_to_id[dc["aria_uuid"]] = dc_id
    default_dc_id = next(iter(dc_uuid_to_id.values()), None)

    # 3. Clusters
    log.info("  Collecting Clusters...")
    clusters = client.collect_clusters()
    cl_uuid_to_id = {}
    for cl in clusters:
        dc_id = dc_uuid_to_id.get(cl.get("parent_dc_uuid"), default_dc_id)
        if dc_id:
            cl_id = upsert_cluster(conn, dc_id, cl)
            cl_uuid_to_id[cl["aria_uuid"]] = cl_id
    default_cl_id = next(iter(cl_uuid_to_id.values()), None)

    # 4. Hosts
    log.info("  Collecting Hosts...")
    hosts = client.collect_hosts()
    host_uuid_to_id = {}
    for host in hosts:
        cl_id = cl_uuid_to_id.get(host.get("parent_cluster_uuid"), default_cl_id)
        if cl_id:
            h_id = upsert_host(conn, cl_id, host)
            host_uuid_to_id[host["aria_uuid"]] = h_id
    default_host_id = next(iter(host_uuid_to_id.values()), None)

    # 5. VMs + NICs
    log.info("  Collecting VMs and NICs...")
    vms = client.collect_vms()
    nic_total = 0
    for vm in vms:
        h_id = host_uuid_to_id.get(vm.get("parent_host_uuid"), default_host_id)
        if h_id:
            vm_db_id = upsert_vm(conn, h_id, vm)
            # Collect NICs for this VM
            try:
                nics = client.collect_vm_nics(vm["aria_uuid"])
                for nic in nics:
                    upsert_vm_nic(conn, vm_db_id, nic)
                nic_total += len(nics)
            except Exception as e:
                log.debug(f"    NIC collection failed for {vm['name']}: {e}")

    log.info(f"  VMs written: {len(vms)} | NICs written: {nic_total}")

    # 6. Datastores
    log.info("  Collecting Datastores...")
    datastores = client.collect_datastores()
    for ds in datastores:
        vc_id = vc_uuid_to_id.get(ds.get("parent_vcenter_uuid"), default_vc_id)
        if vc_id:
            upsert_datastore(conn, vc_id, ds)
    log.info(f"  Datastores written: {len(datastores)}")

    # 7. Networks
    log.info("  Collecting Networks...")
    networks = client.collect_networks()
    for net in networks:
        vc_id = vc_uuid_to_id.get(net.get("parent_vcenter_uuid"), default_vc_id)
        if vc_id:
            upsert_network(conn, vc_id, net)
    log.info(f"  Networks written: {len(networks)}")

    conn.commit()
    with conn.cursor() as cur:
        cur.execute("UPDATE aria_instances SET last_synced = %s WHERE id = %s",
                    (datetime.utcnow(), aria_id))
    conn.commit()
    client.release_token()
    log.info(f"=== Finished: {name} ===\n")


def load_config(path="instances.yaml"):
    with open(path) as f:
        return yaml.safe_load(f).get("instances", [])


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
            log.error(f"Unhandled error for {instance.get('name')}: {e}")
            conn.rollback()

    conn.close()
    log.info("All instances processed. Done.")


if __name__ == "__main__":
    main()
