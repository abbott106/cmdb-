"""
VMware Aria Inventory — CSV Exporter
Exports all inventory tables from PostgreSQL to CSV files for Excel.

Requirements:
    pip install psycopg2-binary python-dotenv

Usage:
    python export_inventory.py

Output:
    Creates an export folder with timestamped CSV files:
        exports/
            YYYYMMDD_HHMMSS/
                01_summary.csv
                02_vcenters.csv
                03_datacenters.csv
                04_clusters.csv
                05_hosts.csv
                06_vms.csv
                07_datastores.csv
                08_networks.csv
                09_vms_powered_off.csv
                10_vms_with_snapshots.csv
                11_datastore_space.csv
                12_host_health.csv
"""

import os
import csv
import psycopg2
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent / ".env")

# ── Database Connection ───────────────────────────────────────────────────────
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "vsphere_inventory"),
        user=os.getenv("DB_USER", "aria_user"),
        password=os.getenv("DB_PASSWORD"),
    )


# ── CSV Writer ────────────────────────────────────────────────────────────────
def write_csv(filepath: Path, cursor, query: str, params=None):
    """Execute a query and write results to a CSV file."""
    cursor.execute(query, params or ())
    rows = cursor.fetchall()
    headers = [desc[0] for desc in cursor.description]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    print(f"  Exported {len(rows):>6} rows  →  {filepath.name}")
    return len(rows)


# ── Export Queries ────────────────────────────────────────────────────────────
EXPORTS = [

    # 1. Summary counts
    (
        "01_summary.csv",
        "Summary",
        """
        SELECT
            'vCenters'    AS object_type, COUNT(*) AS total FROM vcenters
        UNION ALL SELECT
            'Datacenters', COUNT(*) FROM datacenters
        UNION ALL SELECT
            'Clusters',    COUNT(*) FROM clusters
        UNION ALL SELECT
            'Hosts',       COUNT(*) FROM hosts
        UNION ALL SELECT
            'VMs (total)', COUNT(*) FROM vms
        UNION ALL SELECT
            'VMs (powered on)',  COUNT(*) FROM vms WHERE power_state = 'POWERED_ON'
        UNION ALL SELECT
            'VMs (powered off)', COUNT(*) FROM vms WHERE power_state = 'POWERED_OFF'
        UNION ALL SELECT
            'VMs (with snapshots)', COUNT(*) FROM vms WHERE snapshot_count > 0
        UNION ALL SELECT
            'Datastores',  COUNT(*) FROM datastores
        UNION ALL SELECT
            'Networks',    COUNT(*) FROM networks
        ORDER BY object_type
        """
    ),

    # 2. vCenters
    (
        "02_vcenters.csv",
        "vCenters",
        """
        SELECT
            v.name          AS vcenter_name,
            v.version,
            v.health_state,
            a.name          AS aria_instance,
            a.hostname      AS aria_hostname,
            a.last_synced
        FROM vcenters v
        JOIN aria_instances a ON v.aria_id = a.id
        ORDER BY v.name
        """
    ),

    # 3. Datacenters
    (
        "03_datacenters.csv",
        "Datacenters",
        """
        SELECT
            dc.name         AS datacenter,
            vc.name         AS vcenter
        FROM datacenters dc
        JOIN vcenters vc ON dc.vcenter_id = vc.id
        ORDER BY vc.name, dc.name
        """
    ),

    # 4. Clusters
    (
        "04_clusters.csv",
        "Clusters",
        """
        SELECT
            cl.name         AS cluster,
            cl.num_hosts,
            cl.health_state,
            dc.name         AS datacenter,
            vc.name         AS vcenter
        FROM clusters cl
        JOIN datacenters dc ON cl.datacenter_id = dc.id
        JOIN vcenters vc    ON dc.vcenter_id = vc.id
        ORDER BY vc.name, dc.name, cl.name
        """
    ),

    # 5. Hosts — full detail
    (
        "05_hosts.csv",
        "Hosts",
        """
        SELECT
            h.name              AS host_name,
            h.power_state,
            h.connection_state,
            h.health_state,
            h.cpu_cores,
            h.memory_gb,
            h.num_vms,
            h.version           AS esxi_version,
            cl.name             AS cluster,
            dc.name             AS datacenter,
            vc.name             AS vcenter
        FROM hosts h
        JOIN clusters cl    ON h.cluster_id = cl.id
        JOIN datacenters dc ON cl.datacenter_id = dc.id
        JOIN vcenters vc    ON dc.vcenter_id = vc.id
        ORDER BY vc.name, dc.name, cl.name, h.name
        """
    ),

    # 6. VMs — full detail
    (
        "06_vms.csv",
        "Virtual Machines",
        """
        SELECT
            v.name              AS vm_name,
            v.power_state,
            v.health_state,
            v.guest_os,
            v.cpu_count,
            v.memory_gb,
            v.storage_gb,
            v.ip_address,
            v.dns_name,
            v.num_disks,
            v.snapshot_count,
            h.name              AS host,
            cl.name             AS cluster,
            dc.name             AS datacenter,
            vc.name             AS vcenter
        FROM vms v
        JOIN hosts h        ON v.host_id = h.id
        JOIN clusters cl    ON h.cluster_id = cl.id
        JOIN datacenters dc ON cl.datacenter_id = dc.id
        JOIN vcenters vc    ON dc.vcenter_id = vc.id
        ORDER BY vc.name, dc.name, v.name
        """
    ),

    # 7. Datastores
    (
        "07_datastores.csv",
        "Datastores",
        """
        SELECT
            ds.name             AS datastore,
            ds.type,
            ds.capacity_gb,
            ds.free_gb,
            ROUND(
                (ds.free_gb / NULLIF(ds.capacity_gb, 0)) * 100, 1
            )                   AS pct_free,
            ds.health_state,
            vc.name             AS vcenter
        FROM datastores ds
        JOIN vcenters vc ON ds.vcenter_id = vc.id
        ORDER BY pct_free ASC
        """
    ),

    # 8. Networks
    (
        "08_networks.csv",
        "Networks",
        """
        SELECT
            n.name          AS network,
            n.type,
            n.vlan_id,
            vc.name         AS vcenter
        FROM networks n
        JOIN vcenters vc ON n.vcenter_id = vc.id
        ORDER BY vc.name, n.name
        """
    ),

    # 9. DR Report — Powered Off VMs
    (
        "09_vms_powered_off.csv",
        "Powered Off VMs",
        """
        SELECT
            v.name              AS vm_name,
            v.guest_os,
            v.cpu_count,
            v.memory_gb,
            v.storage_gb,
            v.ip_address,
            v.health_state,
            h.name              AS host,
            cl.name             AS cluster,
            dc.name             AS datacenter,
            vc.name             AS vcenter
        FROM vms v
        JOIN hosts h        ON v.host_id = h.id
        JOIN clusters cl    ON h.cluster_id = cl.id
        JOIN datacenters dc ON cl.datacenter_id = dc.id
        JOIN vcenters vc    ON dc.vcenter_id = vc.id
        WHERE v.power_state = 'POWERED_OFF'
        ORDER BY v.name
        """
    ),

    # 10. DR Report — VMs with Snapshots
    (
        "10_vms_with_snapshots.csv",
        "VMs With Snapshots",
        """
        SELECT
            v.name              AS vm_name,
            v.snapshot_count,
            v.power_state,
            v.guest_os,
            v.storage_gb,
            v.ip_address,
            h.name              AS host,
            cl.name             AS cluster,
            dc.name             AS datacenter,
            vc.name             AS vcenter
        FROM vms v
        JOIN hosts h        ON v.host_id = h.id
        JOIN clusters cl    ON h.cluster_id = cl.id
        JOIN datacenters dc ON cl.datacenter_id = dc.id
        JOIN vcenters vc    ON dc.vcenter_id = vc.id
        WHERE v.snapshot_count > 0
        ORDER BY v.snapshot_count DESC
        """
    ),

    # 11. DR Report — Datastore Space
    (
        "11_datastore_space.csv",
        "Datastore Space",
        """
        SELECT
            ds.name             AS datastore,
            ds.type,
            ds.capacity_gb,
            ds.free_gb,
            ROUND(ds.capacity_gb - ds.free_gb, 2) AS used_gb,
            ROUND(
                (ds.free_gb / NULLIF(ds.capacity_gb, 0)) * 100, 1
            )                   AS pct_free,
            CASE
                WHEN (ds.free_gb / NULLIF(ds.capacity_gb, 0)) < 0.10 THEN 'CRITICAL'
                WHEN (ds.free_gb / NULLIF(ds.capacity_gb, 0)) < 0.20 THEN 'WARNING'
                ELSE 'OK'
            END                 AS space_status,
            ds.health_state,
            vc.name             AS vcenter
        FROM datastores ds
        JOIN vcenters vc ON ds.vcenter_id = vc.id
        ORDER BY pct_free ASC
        """
    ),

    # 12. DR Report — Host Health Summary
    (
        "12_host_health.csv",
        "Host Health",
        """
        SELECT
            h.name              AS host_name,
            h.health_state,
            h.power_state,
            h.connection_state,
            h.cpu_cores,
            h.memory_gb,
            h.num_vms,
            h.version           AS esxi_version,
            cl.name             AS cluster,
            dc.name             AS datacenter,
            vc.name             AS vcenter
        FROM hosts h
        JOIN clusters cl    ON h.cluster_id = cl.id
        JOIN datacenters dc ON cl.datacenter_id = dc.id
        JOIN vcenters vc    ON dc.vcenter_id = vc.id
        ORDER BY
            CASE h.health_state
                WHEN 'RED'      THEN 1
                WHEN 'ORANGE'   THEN 2
                WHEN 'YELLOW'   THEN 3
                WHEN 'GREEN'    THEN 4
                ELSE 5
            END,
            h.name
        """
    ),
]


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    # Create timestamped export folder
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    export_dir = Path(__file__).parent / "exports" / timestamp
    export_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nVMware Aria Inventory Export")
    print(f"{'='*50}")
    print(f"Export folder: {export_dir}\n")

    try:
        conn = get_db_connection()
    except Exception as e:
        print(f"Could not connect to database: {e}")
        return

    total_rows = 0
    with conn.cursor() as cur:
        for filename, label, query in EXPORTS:
            filepath = export_dir / filename
            try:
                rows = write_csv(filepath, cur, query)
                total_rows += rows
            except Exception as e:
                print(f"  ERROR exporting {label}: {e}")

    conn.close()

    print(f"\n{'='*50}")
    print(f"Export complete — {total_rows} total rows")
    print(f"Files saved to: {export_dir}")
    print(f"\nOpen any CSV file in Excel.")
    print(f"Tip: Open 06_vms.csv for your full VM inventory.")
    print(f"Tip: Open 11_datastore_space.csv for DR storage review.\n")


if __name__ == "__main__":
    main()
