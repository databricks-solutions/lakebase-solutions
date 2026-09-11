#!/usr/bin/env python3
"""
Generate realistic raw data files for the network infrastructure pipeline.

Produces:
  - network_nodes.csv         (~2,000 nodes: cell towers, fiber cabinets, central offices)
  - network_performance.csv   (~500,000 rows: hourly metrics per node over 30 days)
  - network_outages.json      (~3,000 outage events over 12 months)

These files simulate exports from a telco's network monitoring system (e.g., SolarWinds,
Nagios, or a custom NMS). They land in a UC Volume and are ingested by Declarative Pipelines.

Usage:
    python generate_raw_data.py                    # Generate to current directory
    python generate_raw_data.py --output /path/to  # Generate to specific directory

Can also be run as a Databricks notebook to generate files directly into a Volume.
"""

import csv
import json
import random
import argparse
import os
from datetime import datetime, timedelta


# ── Region config (matches field_service.service_regions) ──────────────
REGIONS = {
    1: {'code': 'PNW', 'states': ['WA', 'OR', 'ID'],       'lat': 47.6, 'lon': -122.3, 'node_count': 280},
    2: {'code': 'SW',  'states': ['AZ', 'NM', 'NV', 'UT'], 'lat': 33.4, 'lon': -112.0, 'node_count': 320},
    3: {'code': 'SC',  'states': ['TX', 'OK', 'AR', 'LA'],  'lat': 32.7, 'lon': -96.8,  'node_count': 400},
    4: {'code': 'SE',  'states': ['FL', 'GA', 'NC', 'SC'],  'lat': 33.7, 'lon': -84.3,  'node_count': 380},
    5: {'code': 'MW',  'states': ['IL', 'OH', 'MI', 'IN'],  'lat': 41.8, 'lon': -87.6,  'node_count': 340},
    6: {'code': 'NE',  'states': ['NY', 'NJ', 'PA', 'MA'],  'lat': 40.7, 'lon': -74.0,  'node_count': 360},
}

NODE_TYPES = [
    {'type': 'cell_tower_5g',    'weight': 0.15, 'capacity_range': (500, 2000),  'power_kw': (3.0, 8.0)},
    {'type': 'cell_tower_4g',    'weight': 0.25, 'capacity_range': (200, 800),   'power_kw': (2.0, 5.0)},
    {'type': 'fiber_cabinet',    'weight': 0.25, 'capacity_range': (100, 500),   'power_kw': (0.5, 2.0)},
    {'type': 'central_office',   'weight': 0.08, 'capacity_range': (5000, 20000),'power_kw': (50.0, 200.0)},
    {'type': 'remote_terminal',  'weight': 0.12, 'capacity_range': (50, 300),    'power_kw': (0.3, 1.5)},
    {'type': 'small_cell',       'weight': 0.10, 'capacity_range': (50, 200),    'power_kw': (0.5, 1.5)},
    {'type': 'microwave_relay',  'weight': 0.05, 'capacity_range': (100, 1000),  'power_kw': (1.0, 3.0)},
]

VENDORS = ['Ericsson', 'Nokia', 'Samsung', 'Huawei', 'Cisco', 'CommScope', 'Calix', 'ADTRAN']

OUTAGE_CAUSES = [
    'Power failure', 'Fiber cut - construction', 'Fiber cut - vehicle accident',
    'Equipment failure - radio', 'Equipment failure - switch', 'Equipment failure - power supply',
    'Software bug', 'Configuration error', 'Overload - capacity exceeded',
    'Weather - lightning strike', 'Weather - ice accumulation', 'Weather - wind damage',
    'Weather - flooding', 'Vandalism', 'Planned maintenance overrun',
    'Backhaul failure', 'Core network issue', 'Cooling system failure',
    'Battery exhaustion', 'Generator failure',
]

OUTAGE_SEVERITIES = ['critical', 'major', 'minor', 'warning']


def generate_network_nodes(output_dir: str) -> list:
    """Generate network_nodes.csv — all infrastructure nodes across regions."""
    filepath = os.path.join(output_dir, 'network_nodes.csv')
    nodes = []
    node_id = 1000

    for region_id, region in REGIONS.items():
        for i in range(region['node_count']):
            # Pick node type
            node_type_info = random.choices(NODE_TYPES, weights=[nt['weight'] for nt in NODE_TYPES])[0]
            node_type = node_type_info['type']

            # Location: scatter around region center
            lat = region['lat'] + random.uniform(-2.5, 2.5)
            lon = region['lon'] + random.uniform(-2.5, 2.5)

            state = random.choice(region['states'])
            capacity_min, capacity_max = node_type_info['capacity_range']
            power_min, power_max = node_type_info['power_kw']

            # Install date: older nodes are more common
            days_old = int(random.triangular(30, 3650, 1200))
            install_date = (datetime.now() - timedelta(days=days_old)).strftime('%Y-%m-%d')

            # Some fields have messy data (realistic for raw exports)
            vendor = random.choice(VENDORS)
            firmware = f"v{random.randint(2,6)}.{random.randint(0,15)}.{random.randint(0,99)}"

            # Occasionally missing data (realistic)
            elevation_ft = round(random.uniform(50, 8000), 1) if random.random() > 0.05 else ''
            backhaul_type = random.choice(['fiber', 'microwave', 'copper', '']) if node_type != 'central_office' else 'fiber'

            node = {
                'node_id': f'NODE-{node_id:05d}',
                'node_name': f'{region["code"]}-{node_type.upper().replace("_", "-")}-{i+1:04d}',
                'node_type': node_type,
                'region_code': region['code'],
                'state': state,
                'latitude': round(lat, 6),
                'longitude': round(lon, 6),
                'elevation_ft': elevation_ft,
                'install_date': install_date,
                'vendor': vendor,
                'model': f'{vendor}-{node_type[:3].upper()}-{random.randint(100,999)}',
                'firmware_version': firmware,
                'max_capacity_mbps': random.randint(capacity_min, capacity_max),
                'power_consumption_kw': round(random.uniform(power_min, power_max), 2),
                'backhaul_type': backhaul_type,
                'has_backup_power': random.choice(['Y', 'N', 'Y', 'Y']),  # 75% have backup
                'status': random.choices(
                    ['active', 'active', 'active', 'active', 'active',
                     'degraded', 'maintenance', 'decommissioned'],
                    weights=[0.80, 0.05, 0.05, 0.02, 0.02, 0.03, 0.02, 0.01]
                )[0],
                'last_maintenance_date': (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d'),
            }
            nodes.append(node)
            node_id += 1

    # Write CSV with some intentional data quality issues for DLT expectations to catch
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=nodes[0].keys())
        writer.writeheader()
        for node in nodes:
            # Occasionally introduce data quality issues
            if random.random() < 0.01:
                node['latitude'] = 'INVALID'  # Bad coordinate
            if random.random() < 0.005:
                node['max_capacity_mbps'] = -1  # Negative value
            if random.random() < 0.008:
                node['status'] = ''  # Missing status
            writer.writerow(node)

    print(f"  Generated {len(nodes)} network nodes -> {filepath}")
    return nodes


def generate_performance_metrics(output_dir: str, nodes: list):
    """Generate network_performance.csv — hourly metrics for each node over 30 days."""
    filepath = os.path.join(output_dir, 'network_performance.csv')

    # Use pipe delimiter for variety (shows DLT can handle different formats)
    fieldnames = [
        'measurement_id', 'node_id', 'timestamp', 'signal_strength_dbm',
        'throughput_mbps', 'latency_ms', 'packet_loss_pct', 'jitter_ms',
        'connected_users', 'cpu_utilization_pct', 'memory_utilization_pct',
        'temperature_celsius', 'uptime_hours', 'error_count', 'bandwidth_utilization_pct'
    ]

    # Only generate for active/degraded nodes
    active_nodes = [n for n in nodes if n['status'] in ('active', 'degraded')]
    # Sample ~600 nodes for 30 days hourly = ~432K rows
    sampled_nodes = random.sample(active_nodes, min(600, len(active_nodes)))

    row_count = 0
    measurement_id = 1

    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='|')
        writer.writeheader()

        for node in sampled_nodes:
            node_type = node['node_type']
            max_cap = int(node['max_capacity_mbps'])
            is_degraded = node['status'] == 'degraded'

            # 12% of active nodes behave as "failing" — high latency, errors, packet loss
            # This ensures the pipeline produces critical/high risk nodes
            is_failing = (not is_degraded) and (hash(node['node_id']) % 100 < 12)

            # Base characteristics by node type
            base_signal = -45 if 'cell' in node_type else -30 if 'fiber' in node_type else -50
            base_throughput_pct = 0.35 if is_failing else (0.65 if is_degraded else 0.85)

            # Generate 30 days of hourly data (720 data points per node)
            base_time = datetime.now() - timedelta(days=30)

            for hour in range(720):
                ts = base_time + timedelta(hours=hour)
                hour_of_day = ts.hour

                # Time-of-day pattern for connected users and throughput
                if 0 <= hour_of_day < 6:
                    usage_factor = 0.2
                elif 6 <= hour_of_day < 9:
                    usage_factor = 0.6
                elif 9 <= hour_of_day < 17:
                    usage_factor = 0.85
                elif 17 <= hour_of_day < 21:
                    usage_factor = 1.0  # Peak evening
                else:
                    usage_factor = 0.5

                # Add some noise
                noise = random.uniform(0.85, 1.15)
                usage = usage_factor * noise

                # Metrics — failing nodes get terrible numbers
                if is_failing:
                    signal = base_signal + random.uniform(-20, -5)
                    throughput = max_cap * base_throughput_pct * usage * random.uniform(0.3, 0.7)
                    latency = random.uniform(60, 200) * (1.3 if usage > 0.8 else 1.0)
                    packet_loss = random.uniform(1.5, 5.0)
                    jitter = random.uniform(15, 50)
                    connected = int(max_cap * 0.1 * usage * random.uniform(0.3, 1.0))
                    cpu = min(99, max(60, 75 * usage + random.uniform(-5, 20)))
                    memory = min(98, max(50, 70 * usage + random.uniform(-5, 15)))
                    temp = random.uniform(55, 80)
                    uptime = random.uniform(0, 500)
                    errors = int(random.expovariate(1.0 / 30)) + random.randint(5, 20)
                else:
                    signal = base_signal + random.uniform(-8, 3)
                    throughput = max_cap * base_throughput_pct * usage * random.uniform(0.7, 1.0)
                    latency = random.uniform(5, 25) * (1.5 if is_degraded else 1.0) * (1.2 if usage > 0.9 else 1.0)
                    packet_loss = random.uniform(0, 0.5) * (3.0 if is_degraded else 1.0)
                    jitter = random.uniform(1, 10) * (2.0 if is_degraded else 1.0)
                    connected = int(max_cap * 0.3 * usage * random.uniform(0.5, 1.5))
                    cpu = min(99, max(5, 30 * usage + random.uniform(-10, 20)))
                    memory = min(95, max(10, 40 * usage + random.uniform(-10, 15)))
                    temp = random.uniform(25, 55) + (10 if usage > 0.8 else 0)
                    uptime = random.uniform(0, 8760)
                    errors = int(random.expovariate(1.0 / 2))
                bw_util = min(100, throughput / max_cap * 100) if max_cap > 0 else 0

                row = {
                    'measurement_id': measurement_id,
                    'node_id': node['node_id'],
                    'timestamp': ts.strftime('%Y-%m-%d %H:%M:%S'),
                    'signal_strength_dbm': round(signal, 1),
                    'throughput_mbps': round(throughput, 2),
                    'latency_ms': round(latency, 2),
                    'packet_loss_pct': round(packet_loss, 4),
                    'jitter_ms': round(jitter, 2),
                    'connected_users': connected,
                    'cpu_utilization_pct': round(cpu, 1),
                    'memory_utilization_pct': round(memory, 1),
                    'temperature_celsius': round(temp, 1),
                    'uptime_hours': round(uptime, 1),
                    'error_count': errors,
                    'bandwidth_utilization_pct': round(bw_util, 1),
                }

                # Occasional data quality issues
                if random.random() < 0.002:
                    row['throughput_mbps'] = 'N/A'  # Non-numeric
                if random.random() < 0.003:
                    row['timestamp'] = ''  # Missing timestamp
                if random.random() < 0.001:
                    row['cpu_utilization_pct'] = 150.0  # Out-of-range

                writer.writerow(row)
                measurement_id += 1
                row_count += 1

    print(f"  Generated {row_count:,} performance metrics -> {filepath}")


def generate_outage_events(output_dir: str, nodes: list):
    """Generate network_outages.json — outage events in JSON Lines format."""
    filepath = os.path.join(output_dir, 'network_outages.json')

    active_nodes = [n for n in nodes if n['status'] in ('active', 'degraded')]
    outages = []

    for i in range(3000):
        node = random.choice(active_nodes)

        # Outage timing — concentrate in recent days so "Recent Outages (7 Days)" has data
        r = random.random()
        if r < 0.40:
            days_ago = random.randint(0, 7)     # 40% in last week
        elif r < 0.70:
            days_ago = random.randint(0, 30)    # 30% in last month
        else:
            days_ago = random.randint(0, 365)   # 30% in last year
        start_time = datetime.now() - timedelta(
            days=days_ago,
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )

        # Duration depends on severity
        severity = random.choices(
            OUTAGE_SEVERITIES,
            weights=[0.10, 0.25, 0.40, 0.25]
        )[0]

        if severity == 'critical':
            duration_min = random.randint(60, 1440)  # 1-24 hours
        elif severity == 'major':
            duration_min = random.randint(30, 480)    # 30min-8 hours
        elif severity == 'minor':
            duration_min = random.randint(10, 120)    # 10min-2 hours
        else:
            duration_min = random.randint(5, 60)      # 5-60 min

        end_time = start_time + timedelta(minutes=duration_min)

        cause = random.choice(OUTAGE_CAUSES)

        # Affected customers based on node capacity
        max_cap = int(node['max_capacity_mbps'])
        affected_customers = int(max_cap * random.uniform(0.1, 0.8))

        # Nested structure (realistic for JSON from monitoring APIs)
        outage = {
            'outage_id': f'OUT-{i+1:06d}',
            'node_id': node['node_id'],
            'node_type': node['node_type'],
            'region_code': node['region_code'],
            'severity': severity,
            'root_cause': cause,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat() if days_ago > 0 or random.random() > 0.05 else None,  # Some ongoing
            'duration_minutes': duration_min if end_time else None,
            'affected_customers': affected_customers,
            'impact': {
                'service_degraded': severity in ('minor', 'warning'),
                'service_down': severity in ('critical', 'major'),
                'estimated_revenue_impact': round(affected_customers * duration_min * 0.002, 2),
            },
            'resolution': {
                'resolved_by': random.choice(['auto_recovery', 'noc_remote', 'field_dispatch', 'vendor_support']) if end_time else None,
                'dispatch_required': random.random() < 0.4,
                'work_order_created': random.random() < 0.35,
            },
            'reported_at': start_time.isoformat(),
            'detected_by': random.choice(['monitoring_alert', 'customer_report', 'noc_visual', 'automated_test']),
        }

        # Occasional data quality issues
        if random.random() < 0.01:
            outage['severity'] = 'CRITICAL'  # Wrong case
        if random.random() < 0.005:
            outage['affected_customers'] = 'unknown'  # Wrong type

        outages.append(outage)

    # Write as JSON Lines (one JSON object per line — standard for streaming ingestion)
    with open(filepath, 'w') as f:
        for outage in outages:
            f.write(json.dumps(outage) + '\n')

    print(f"  Generated {len(outages):,} outage events -> {filepath}")


def main():
    parser = argparse.ArgumentParser(description='Generate raw network data files for FSM demo pipeline')
    parser.add_argument('--output', default=os.path.dirname(os.path.abspath(__file__)),
                       help='Output directory (default: same as script)')
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    print(f"\nGenerating telco network raw data files...")
    print(f"Output: {args.output}\n")

    nodes = generate_network_nodes(args.output)
    generate_performance_metrics(args.output, nodes)
    generate_outage_events(args.output, nodes)

    print(f"\nDone. Files ready for upload to UC Volume.\n")


if __name__ == '__main__':
    main()
