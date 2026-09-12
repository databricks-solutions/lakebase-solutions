#!/usr/bin/env python3
"""
Real-Time Field Service Data Generator for Lakebase FSM Demo
Simulates a live telco field service operation writing to Lakebase

Generates realistic patterns:
  - Time-of-day work order volume (peaks at 8-11am, drops at night)
  - Regional volume weighted by population
  - Weather/outage events that cause correlated spikes
  - Technician shift schedules (day/evening/on-call)
  - Realistic status progression with time delays
  - SLA countdown and breach tracking
  - Equipment failure patterns correlated with age
  - Customer satisfaction correlated with resolution speed
  - Escalation patterns when SLA is about to breach

Usage:
    # From command line with config.yaml:
    python realtime_data_generator.py

    # From command line with explicit parameters:
    python realtime_data_generator.py --host <host> --database <db> --user <user> --password <pass>

    # In Databricks notebook (see notebooks/run_data_generator.py):
    # Uses LakebaseConnectionFactory for auto-credentials + token refresh
"""

import argparse
import psycopg2
import random
import time
import signal
import sys
import os
import json
import base64
import math
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import threading

# ── Terminal colors ─────────────────────────────────────────────────────
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    UNDERLINE = '\033[4m'
    ORANGE = '\033[38;5;208m'


# ── Region-to-city mapping (avoids re-creation per call) ──────────────
REGION_CITIES = {
    'PNW': [('Seattle', 'WA'), ('Portland', 'OR'), ('Boise', 'ID'), ('Tacoma', 'WA')],
    'SW':  [('Phoenix', 'AZ'), ('Las Vegas', 'NV'), ('Albuquerque', 'NM'), ('Salt Lake City', 'UT')],
    'SC':  [('Dallas', 'TX'), ('Houston', 'TX'), ('Austin', 'TX'), ('Oklahoma City', 'OK')],
    'SE':  [('Atlanta', 'GA'), ('Charlotte', 'NC'), ('Miami', 'FL'), ('Tampa', 'FL')],
    'MW':  [('Chicago', 'IL'), ('Detroit', 'MI'), ('Columbus', 'OH'), ('Indianapolis', 'IN')],
    'NE':  [('New York', 'NY'), ('Philadelphia', 'PA'), ('Boston', 'MA'), ('Newark', 'NJ')],
}

# ── Realistic telco field service data ──────────────────────────────────

# Work order templates: (category, subcategory, title, reported_issue, required_skill_category, typical_priority_weights)
WO_TEMPLATES = {
    'install': [
        ('install', 'fiber_install', 'New Fiber Installation',
         'Customer signed up for fiber internet service, needs ONT and router installed',
         'installation', {'low': 0.4, 'medium': 0.5, 'high': 0.1, 'critical': 0.0}),
        ('install', 'copper_install', 'New Copper/DSL Installation',
         'New DSL service activation, need to install modem and verify line quality',
         'installation', {'low': 0.5, 'medium': 0.4, 'high': 0.1, 'critical': 0.0}),
        ('install', 'satellite_install', 'Satellite Dish Installation',
         'New satellite TV customer, need dish mount and alignment plus receiver setup',
         'installation', {'low': 0.3, 'medium': 0.5, 'high': 0.2, 'critical': 0.0}),
        ('install', 'fixed_wireless_install', 'Fixed Wireless CPE Installation',
         'Install 5G fixed wireless unit on exterior of building, configure indoor router',
         'network', {'low': 0.3, 'medium': 0.5, 'high': 0.2, 'critical': 0.0}),
        ('install', 'business_install', 'Business Service Installation',
         'Enterprise fiber installation with managed router, VLAN configuration required',
         'network', {'low': 0.1, 'medium': 0.3, 'high': 0.4, 'critical': 0.2}),
    ],
    'repair': [
        ('repair', 'no_signal', 'No Signal - Complete Service Outage',
         'My internet has been completely down since this morning, all lights on modem are off',
         'repair', {'low': 0.0, 'medium': 0.2, 'high': 0.5, 'critical': 0.3}),
        ('repair', 'slow_speed', 'Slow Speed - Below Subscribed Tier',
         'Speed test shows 10mbps but I am paying for 500mbps, this has been going on for days',
         'repair', {'low': 0.2, 'medium': 0.5, 'high': 0.3, 'critical': 0.0}),
        ('repair', 'intermittent_connection', 'Intermittent Connection Drops',
         'Connection keeps dropping every 15-20 minutes, have to restart the router each time',
         'repair', {'low': 0.1, 'medium': 0.5, 'high': 0.3, 'critical': 0.1}),
        ('repair', 'equipment_failure', 'Customer Equipment Malfunction',
         'The ONT box is making a clicking noise and the power light is flashing red',
         'repair', {'low': 0.1, 'medium': 0.4, 'high': 0.4, 'critical': 0.1}),
        ('repair', 'line_damage', 'Damaged Outside Plant Line',
         'After the storm last night there is a cable hanging from the pole in my backyard',
         'repair', {'low': 0.0, 'medium': 0.1, 'high': 0.4, 'critical': 0.5}),
        ('repair', 'noise_on_line', 'Noise/Static on Phone Line',
         'Terrible static noise on my phone line, can barely hear anything on calls',
         'repair', {'low': 0.3, 'medium': 0.5, 'high': 0.2, 'critical': 0.0}),
        ('repair', 'wifi_issues', 'WiFi Coverage Problems',
         'WiFi signal drops in back rooms of the house, need better coverage throughout',
         'network', {'low': 0.4, 'medium': 0.5, 'high': 0.1, 'critical': 0.0}),
        ('repair', 'tv_pixelation', 'TV Service Pixelation and Audio Issues',
         'TV service pixelating badly on all channels, audio cutting in and out',
         'repair', {'low': 0.2, 'medium': 0.5, 'high': 0.3, 'critical': 0.0}),
    ],
    'maintenance': [
        ('maintenance', 'scheduled_maintenance', 'Scheduled Preventive Maintenance',
         'Annual inspection of outside plant facilities in service area',
         'maintenance', {'low': 0.7, 'medium': 0.3, 'high': 0.0, 'critical': 0.0}),
        ('maintenance', 'equipment_refresh', 'CPE Equipment Refresh',
         'Customer eligible for equipment upgrade, swap out end-of-life ONT/router',
         'maintenance', {'low': 0.6, 'medium': 0.4, 'high': 0.0, 'critical': 0.0}),
        ('maintenance', 'line_inspection', 'Aerial/Underground Line Inspection',
         'Routine inspection of aerial or buried plant for degradation',
         'maintenance', {'low': 0.8, 'medium': 0.2, 'high': 0.0, 'critical': 0.0}),
    ],
    'upgrade': [
        ('upgrade', 'speed_upgrade', 'Speed Tier Upgrade',
         'Customer upgrading from 200mbps to 1gbps tier, may need ONT swap',
         'network', {'low': 0.3, 'medium': 0.5, 'high': 0.2, 'critical': 0.0}),
        ('upgrade', 'equipment_upgrade', 'Equipment Upgrade to WiFi 6E',
         'Upgrading customer to latest WiFi 6E router and mesh extenders',
         'network', {'low': 0.4, 'medium': 0.5, 'high': 0.1, 'critical': 0.0}),
        ('upgrade', 'service_tier_change', 'Service Tier Change',
         'Customer changing service package, needs reconfiguration of CPE',
         'network', {'low': 0.5, 'medium': 0.4, 'high': 0.1, 'critical': 0.0}),
    ],
    'disconnect': [
        ('disconnect', 'service_disconnect', 'Service Disconnection',
         'Customer requested service cancellation, need to retrieve CPE equipment',
         'maintenance', {'low': 0.7, 'medium': 0.3, 'high': 0.0, 'critical': 0.0}),
    ],
}

# Category distribution weights (what a real telco sees)
CATEGORY_WEIGHTS = {
    'install': 0.22,
    'repair': 0.40,
    'maintenance': 0.18,
    'upgrade': 0.15,
    'disconnect': 0.05,
}

# Tech notes templates
TECH_NOTES = [
    "Arrived on site. Customer showed me the issue. Running initial diagnostics on the line.",
    "Signal levels at the ONT are {signal_level}dBm — {signal_assessment}. Checking upstream.",
    "Replaced faulty ONT (SN: {old_sn}). New unit provisioned and tested, signal restored.",
    "Found damaged drop cable at the service entrance. Cable shows {damage_type}. Replacing.",
    "Completed speed test: DL {dl_speed}mbps / UL {ul_speed}mbps. Within spec for tier.",
    "Customer not home on arrival. Left door tag with callback number. Rescheduling.",
    "Identified failing splitter at the {location}. Replaced with new unit, all ports verified.",
    "Power cycled CPE, issue resolved. Monitoring for 15 minutes before closing.",
    "Firmware on router was 2 versions behind. Updated to {firmware_ver}. Customer confirmed improvement.",
    "Ran new {cable_type} drop cable from {from_loc} to {to_loc}. Old cable showed {loss}dB signal loss.",
    "Equipment swap completed. Collected old {old_equip} for RMA. New {new_equip} installed and tested.",
    "Spliced fiber at the {splice_loc}. OTDR test shows {otdr_loss}dB loss — within acceptable range.",
    "Traced issue to corroded connector at the {connector_loc}. Cleaned and re-terminated.",
    "Installed WiFi 6E mesh extender in {room}. Signal strength improved from {old_signal} to {new_signal}.",
    "Business customer — configured VLAN {vlan_id} for voice traffic. QoS policies verified.",
    "Underground locate completed. Marked fiber path with orange flags. Ready for excavation.",
    "Tower site inspection complete. All equipment within normal operating parameters.",
    "Generator test completed. Fuel level at {fuel_pct}%. Runtime test: {runtime_min} minutes. PASS.",
    "Escalating to Tier 2 — issue appears to be upstream of the serving terminal. Reference: {ref_id}.",
    "Customer satisfaction check: customer confirmed all services working. Rating: {rating}/5.",
]

# Weather event templates that cause correlated work order spikes
WEATHER_EVENTS = [
    {'name': 'Thunderstorm', 'affected_categories': ['repair'], 'subcategories': ['line_damage', 'no_signal', 'equipment_failure'],
     'multiplier': 3.0, 'duration_hours': 4, 'priority_boost': True},
    {'name': 'Ice Storm', 'affected_categories': ['repair'], 'subcategories': ['line_damage', 'no_signal'],
     'multiplier': 5.0, 'duration_hours': 8, 'priority_boost': True},
    {'name': 'High Winds', 'affected_categories': ['repair'], 'subcategories': ['line_damage', 'tv_pixelation'],
     'multiplier': 2.5, 'duration_hours': 6, 'priority_boost': True},
    {'name': 'Heat Wave', 'affected_categories': ['repair', 'maintenance'], 'subcategories': ['equipment_failure', 'slow_speed'],
     'multiplier': 1.8, 'duration_hours': 12, 'priority_boost': False},
    {'name': 'Construction Accident', 'affected_categories': ['repair'], 'subcategories': ['line_damage', 'no_signal'],
     'multiplier': 4.0, 'duration_hours': 3, 'priority_boost': True},
]

# SLA hours by priority (simplified — matches sla_policies table residential tier)
SLA_HOURS = {'low': 120, 'medium': 72, 'high': 24, 'critical': 12}


def load_config_if_available():
    """Try to load configuration from deployment/config.yaml and connect via Databricks SDK."""
    try:
        deployment_dir = str(Path(__file__).parent.parent / "deployment")
        if deployment_dir not in sys.path:
            sys.path.insert(0, deployment_dir)

        from config import load_config, get_workspace_client

        cfg = load_config()
        w = get_workspace_client(cfg)

        instance = w.api_client.do(
            "GET", f"/api/2.0/database/instances/{cfg['instance_name']}"
        )
        host = instance['read_write_dns']

        cred = w.database.generate_database_credential(
            instance_names=[cfg['instance_name']]
        )
        token_parts = cred.token.split('.')
        payload = token_parts[1] + '=' * (4 - len(token_parts[1]) % 4)
        payload_data = json.loads(base64.urlsafe_b64decode(payload))

        return {
            'host': host,
            'database': cfg.get('database', 'databricks_postgres'),
            'user': payload_data['sub'],
            'password': cred.token,
            'port': 5432,
            'schema': cfg.get('schema', 'field_service'),
        }
    except Exception:
        return None


class LakebaseConnectionFactory:
    """Creates and refreshes psycopg2 connections using Databricks Lakebase credentials."""

    TOKEN_REFRESH_MINUTES = 25

    def __init__(self, cfg: dict, workspace_client):
        self.cfg = cfg
        self.w = workspace_client
        self._host = None
        self._last_token_time = None

    def _resolve_host(self):
        if self._host is None:
            instance = self.w.api_client.do(
                "GET", f"/api/2.0/database/instances/{self.cfg['instance_name']}"
            )
            self._host = instance['read_write_dns']
        return self._host

    def _generate_credential(self):
        cred = self.w.database.generate_database_credential(
            instance_names=[self.cfg['instance_name']]
        )
        token_parts = cred.token.split('.')
        payload = token_parts[1] + '=' * (4 - len(token_parts[1]) % 4)
        payload_data = json.loads(base64.urlsafe_b64decode(payload))
        self._last_token_time = time.time()
        return payload_data['sub'], cred.token

    def create_connection(self):
        host = self._resolve_host()
        user, password = self._generate_credential()
        return psycopg2.connect(
            host=host, port=5432, user=user, password=password,
            database=self.cfg.get('database', 'databricks_postgres'),
            sslmode='require'
        )

    def needs_refresh(self):
        if self._last_token_time is None:
            return True
        elapsed = (time.time() - self._last_token_time) / 60
        return elapsed >= self.TOKEN_REFRESH_MINUTES


class FieldServiceGenerator:
    """Simulates a live telco field service operation with realistic data patterns."""

    def __init__(self, host: str = None, database: str = None, user: str = None,
                 password: str = None, port: int = 5432, schema: str = 'field_service',
                 connection_factory: LakebaseConnectionFactory = None,
                 notebook_mode: bool = False):
        self.schema = schema
        self.notebook_mode = notebook_mode
        self.connection_factory = connection_factory
        self._stop_event = threading.Event()

        if connection_factory:
            self.conn = connection_factory.create_connection()
        else:
            self.conn = psycopg2.connect(
                host=host, database=database, user=user,
                password=password, port=port, sslmode='require'
            )
        self.conn.autocommit = True

        self.running = True
        self.stats = {
            'work_orders_created': 0,
            'status_transitions': 0,
            'appointments_created': 0,
            'appointments_completed': 0,
            'parts_consumed': 0,
            'notes_added': 0,
            'new_customers': 0,
            'sla_breaches': 0,
            'weather_events': 0,
            'escalations': 0,
            'errors': 0,
        }
        self.start_time = datetime.now()

        # Reference data caches
        self.customer_ids = []
        self.technician_data = {}  # tech_id -> {region_id, status, shift, name, skills}
        self.region_data = {}      # region_id -> {code, weight, timezone}
        self.sla_data = {}         # sla_id -> {priority, response_hours, resolution_hours}
        self.equipment_types = []
        self.wo_counter = 0
        self.active_weather = None  # Current weather event if any

        # Track open work orders for status progression
        self.open_wo_ids = []
        self.assigned_wo_ids = []
        self.enroute_wo_ids = []
        self.inprogress_wo_ids = []

        self.load_reference_data()

        if not notebook_mode:
            signal.signal(signal.SIGINT, self.signal_handler)
        else:
            try:
                signal.signal(signal.SIGINT, self.signal_handler)
            except (OSError, ValueError):
                pass

    def stop(self):
        self._stop_event.set()
        self.running = False

    def signal_handler(self, sig, frame):
        if self.notebook_mode:
            self.stop()
        else:
            print(f"\n\n{Colors.YELLOW}{'='*80}{Colors.ENDC}")
            print(f"{Colors.YELLOW}Shutting down gracefully...{Colors.ENDC}")
            self.running = False

    # ── Reference Data Loading ──────────────────────────────────────────

    def load_reference_data(self):
        """Load existing data for realistic generation."""
        cursor = self.conn.cursor()
        msg = lambda m: print(f"  {m}") if self.notebook_mode else print(f"{Colors.CYAN}  {m}{Colors.ENDC}")

        msg("Loading reference data...")

        # Customers
        cursor.execute(f"SELECT customer_id FROM {self.schema}.customers WHERE account_status = 'active' LIMIT 20000")
        self.customer_ids = [r[0] for r in cursor.fetchall()]
        msg(f"  {len(self.customer_ids)} active customers")

        # Regions
        cursor.execute(f"SELECT region_id, region_code, population_weight, timezone FROM {self.schema}.service_regions")
        for rid, code, weight, tz in cursor.fetchall():
            self.region_data[rid] = {'code': code, 'weight': float(weight), 'timezone': tz}

        # Technicians with their regions and skills
        cursor.execute(f"""
            SELECT t.technician_id, t.first_name || ' ' || t.last_name, t.region_id, t.status, t.shift,
                   ARRAY_AGG(DISTINCT st.skill_category) FILTER (WHERE st.skill_category IS NOT NULL) as skill_cats
            FROM {self.schema}.technicians t
            LEFT JOIN {self.schema}.technician_skills ts ON t.technician_id = ts.technician_id
            LEFT JOIN {self.schema}.skill_types st ON ts.skill_id = st.skill_id
            WHERE t.is_active = TRUE
            GROUP BY t.technician_id, t.first_name, t.last_name, t.region_id, t.status, t.shift
        """)
        for tid, name, rid, status, shift, skills in cursor.fetchall():
            self.technician_data[tid] = {
                'name': name, 'region_id': rid, 'status': status,
                'shift': shift, 'skills': skills or []
            }
        msg(f"  {len(self.technician_data)} active technicians")

        # SLA policies
        cursor.execute(f"SELECT sla_id, priority, response_hours, resolution_hours, customer_tier FROM {self.schema}.sla_policies")
        for sid, pri, resp, res, tier in cursor.fetchall():
            self.sla_data[sid] = {'priority': pri, 'response_hours': resp, 'resolution_hours': res, 'tier': tier}

        # Equipment types
        cursor.execute(f"SELECT equipment_type_id, equipment_name, category FROM {self.schema}.equipment_catalog WHERE is_active = TRUE")
        self.equipment_types = [{'id': r[0], 'name': r[1], 'category': r[2]} for r in cursor.fetchall()]

        # Get current max WO number for unique generation
        cursor.execute(f"SELECT COUNT(*) FROM {self.schema}.work_orders")
        self.wo_counter = cursor.fetchone()[0]

        # Load active work orders for status progression
        cursor.execute(f"SELECT work_order_id FROM {self.schema}.work_orders WHERE status = 'open' ORDER BY RANDOM() LIMIT 20000")
        self.open_wo_ids = [r[0] for r in cursor.fetchall()]
        cursor.execute(f"SELECT work_order_id FROM {self.schema}.work_orders WHERE status = 'assigned' ORDER BY RANDOM() LIMIT 12000")
        self.assigned_wo_ids = [r[0] for r in cursor.fetchall()]
        cursor.execute(f"SELECT work_order_id FROM {self.schema}.work_orders WHERE status = 'en_route' ORDER BY RANDOM() LIMIT 8000")
        self.enroute_wo_ids = [r[0] for r in cursor.fetchall()]
        cursor.execute(f"SELECT work_order_id FROM {self.schema}.work_orders WHERE status = 'in_progress' ORDER BY RANDOM() LIMIT 8000")
        self.inprogress_wo_ids = [r[0] for r in cursor.fetchall()]

        msg(f"  {len(self.open_wo_ids)} open, {len(self.assigned_wo_ids)} assigned, "
            f"{len(self.enroute_wo_ids)} en_route, {len(self.inprogress_wo_ids)} in_progress WOs loaded")

        cursor.close()

    # ── Time-of-Day Volume Modeling ─────────────────────────────────────

    def _time_of_day_multiplier(self) -> float:
        """Returns a multiplier (0.1 - 2.0) based on current hour.
        Models real telco patterns: ramp up at 7am, peak 9-11am, steady afternoon, taper at 5pm, quiet overnight."""
        hour = datetime.now().hour
        # Hourly multipliers: index = hour of day
        hourly = [
            0.10, 0.08, 0.05, 0.05, 0.05, 0.08,  # 0-5am: very quiet
            0.15, 0.40, 0.70, 1.00, 1.00, 0.90,  # 6-11am: ramp to peak
            0.85, 0.90, 0.95, 0.85, 0.70, 0.50,  # 12-5pm: steady then taper
            0.35, 0.25, 0.20, 0.15, 0.12, 0.10,  # 6-11pm: evening decline
        ]
        return hourly[hour]

    def _day_of_week_multiplier(self) -> float:
        """Weekdays are busier than weekends for field service."""
        dow = datetime.now().weekday()  # 0=Mon, 6=Sun
        if dow < 5:
            return 1.0  # Weekday
        elif dow == 5:
            return 0.4  # Saturday (some emergency work)
        else:
            return 0.15  # Sunday (emergencies only)

    def _pick_region(self) -> int:
        """Pick a region weighted by population."""
        regions = list(self.region_data.keys())
        weights = [self.region_data[r]['weight'] for r in regions]
        return random.choices(regions, weights=weights)[0]

    def _pick_priority(self, priority_weights: dict) -> str:
        """Pick a priority based on template weights, boosted during weather events."""
        priorities = list(priority_weights.keys())
        weights = list(priority_weights.values())

        if self.active_weather and self.active_weather.get('priority_boost'):
            # Shift weight toward higher priorities during weather events
            weights = [w * 0.5 for w in weights]  # reduce all
            weights[2] = min(weights[2] + 0.3, 1.0)  # boost high
            weights[3] = min(weights[3] + 0.2, 1.0)  # boost critical

        return random.choices(priorities, weights=weights)[0]

    def _get_sla_id(self, priority: str, customer_tier: str = 'residential') -> int:
        """Find matching SLA policy."""
        for sid, sla in self.sla_data.items():
            if sla['priority'] == priority and sla['tier'] == customer_tier:
                return sid
        # Fallback to residential
        for sid, sla in self.sla_data.items():
            if sla['priority'] == priority and sla['tier'] == 'residential':
                return sid
        return 1

    def _find_technician(self, region_id: int, required_skill: str = None) -> Optional[int]:
        """Find an available technician in the region with matching skills."""
        candidates = [
            tid for tid, data in self.technician_data.items()
            if data['region_id'] == region_id
            and data['status'] in ('available',)
            and (required_skill is None or required_skill in data.get('skills', []))
        ]
        if not candidates:
            # Fallback: any available tech in region regardless of skill
            candidates = [
                tid for tid, data in self.technician_data.items()
                if data['region_id'] == region_id and data['status'] in ('available',)
            ]
        return random.choice(candidates) if candidates else None

    # ── Event Generators ────────────────────────────────────────────────

    def create_work_order(self) -> Optional[Dict]:
        """Create a new work order with realistic attributes."""
        cursor = self.conn.cursor()
        try:
            # Pick category based on weights (adjusted for weather)
            categories = list(CATEGORY_WEIGHTS.keys())
            weights = list(CATEGORY_WEIGHTS.values())

            if self.active_weather:
                for i, cat in enumerate(categories):
                    if cat in self.active_weather['affected_categories']:
                        weights[i] *= self.active_weather['multiplier']

            category = random.choices(categories, weights=weights)[0]

            # Pick template
            templates = WO_TEMPLATES[category]

            # During weather, prefer specific subcategories
            if self.active_weather and category in self.active_weather['affected_categories']:
                weather_templates = [t for t in templates if t[1] in self.active_weather['subcategories']]
                if weather_templates:
                    templates = weather_templates

            template = random.choice(templates)
            cat, subcat, title, reported_issue, req_skill, priority_weights = template

            # Pick region and priority
            region_id = self._pick_region()
            priority = self._pick_priority(priority_weights)

            # Pick customer from same region
            cursor.execute(f"""
                SELECT customer_id, customer_tier, address_line1, city, state_province, postal_code
                FROM {self.schema}.customers
                WHERE region_id = %s AND account_status = 'active'
                ORDER BY RANDOM() LIMIT 1
            """, (region_id,))
            cust = cursor.fetchone()
            if not cust:
                return None

            customer_id, tier, addr, city, state, postal = cust
            sla_id = self._get_sla_id(priority, tier)
            sla_hours = SLA_HOURS.get(priority, 72)

            self.wo_counter += 1
            wo_number = f"WO-{datetime.now().strftime('%Y')}-{self.wo_counter:06d}"

            # Add weather context to description if applicable
            description = f"Work order created via " + random.choice([
                'customer call center', 'online self-service portal',
                'mobile app report', 'proactive network monitoring',
                'field technician referral', 'automated alert system'
            ])
            if self.active_weather:
                description += f" [Related to {self.active_weather['name']} event in {self.region_data[region_id]['code']}]"

            cursor.execute(f"""
                INSERT INTO {self.schema}.work_orders (
                    work_order_number, customer_id, sla_id, category, subcategory,
                    priority, status, title, description, reported_issue,
                    address_line1, city, state_province, postal_code, region_id,
                    created_at, sla_due_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, 'open', %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP + INTERVAL '{sla_hours} hours'
                ) RETURNING work_order_id
            """, (wo_number, customer_id, sla_id, cat, subcat,
                  priority, title, description, reported_issue,
                  addr, city, state, postal, region_id))

            wo_id = cursor.fetchone()[0]
            self.open_wo_ids.append(wo_id)

            # Add creation note
            cursor.execute(f"""
                INSERT INTO {self.schema}.work_order_notes (work_order_id, author, note_type, content)
                VALUES (%s, 'System', 'status_change', %s)
            """, (wo_id, f"Work order {wo_number} created. Priority: {priority.upper()}. SLA due in {sla_hours}h."))

            self.stats['work_orders_created'] += 1
            self.stats['notes_added'] += 1

            return {
                'type': 'work_order',
                'wo_number': wo_number,
                'category': cat,
                'subcategory': subcat,
                'priority': priority,
                'region': self.region_data[region_id]['code'],
                'title': title,
                'weather_related': self.active_weather is not None,
            }

        except Exception as e:
            self.stats['errors'] += 1
            return {'type': 'error', 'source': 'create_wo', 'message': str(e)[:200]}
        finally:
            cursor.close()

    def transition_work_order(self) -> Optional[Dict]:
        """Advance a work order through its lifecycle: open -> assigned -> en_route -> in_progress -> completed."""
        cursor = self.conn.cursor()
        try:
            # Pick a transition based on what's available
            transitions = []
            if self.open_wo_ids:
                transitions.append(('open', 'assigned', self.open_wo_ids))
            if self.assigned_wo_ids:
                transitions.append(('assigned', 'en_route', self.assigned_wo_ids))
            if self.enroute_wo_ids:
                transitions.append(('en_route', 'in_progress', self.enroute_wo_ids))
            if self.inprogress_wo_ids:
                transitions.append(('in_progress', 'completed', self.inprogress_wo_ids))

            if not transitions:
                return None

            # Weight toward completing existing work
            weights = [0.25, 0.25, 0.25, 0.25][:len(transitions)]
            from_status, to_status, wo_list = random.choices(transitions, weights=weights)[0]

            wo_id = wo_list.pop(random.randrange(len(wo_list)))

            # Get WO details
            cursor.execute(f"""
                SELECT work_order_number, region_id, priority, category
                FROM {self.schema}.work_orders WHERE work_order_id = %s
            """, (wo_id,))
            row = cursor.fetchone()
            if not row:
                return None
            wo_number, region_id, priority, category = row

            note_content = None
            tech_name = None

            if to_status == 'assigned':
                # Find and assign a technician
                tech_id = self._find_technician(region_id)
                if tech_id:
                    cursor.execute(f"""
                        UPDATE {self.schema}.work_orders
                        SET status = 'assigned', assigned_technician_id = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE work_order_id = %s
                    """, (tech_id, wo_id))
                    tech_name = self.technician_data.get(tech_id, {}).get('name', 'Unknown')
                    self.technician_data[tech_id]['status'] = 'en_route'
                    note_content = f"Assigned to technician {tech_name} (ID: {tech_id})"
                    self.assigned_wo_ids.append(wo_id)

                    # Create appointment
                    travel_min = random.randint(15, 60)
                    onsite_min = random.randint(30, 180)
                    cursor.execute(f"""
                        INSERT INTO {self.schema}.appointments (
                            work_order_id, technician_id,
                            scheduled_start, scheduled_end, status, travel_time_min
                        ) VALUES (
                            %s, %s,
                            CURRENT_TIMESTAMP + INTERVAL '{travel_min} minutes',
                            CURRENT_TIMESTAMP + INTERVAL '{travel_min + onsite_min} minutes',
                            'confirmed', %s
                        )
                    """, (wo_id, tech_id, travel_min))
                    self.stats['appointments_created'] += 1
                else:
                    self.open_wo_ids.append(wo_id)  # Put back if no tech available
                    return None

            elif to_status == 'en_route':
                cursor.execute(f"""
                    UPDATE {self.schema}.work_orders
                    SET status = 'en_route', first_response_at = COALESCE(first_response_at, CURRENT_TIMESTAMP), updated_at = CURRENT_TIMESTAMP
                    WHERE work_order_id = %s RETURNING assigned_technician_id
                """, (wo_id,))
                tech_id = cursor.fetchone()[0]
                tech_name = self.technician_data.get(tech_id, {}).get('name', 'Unknown') if tech_id else 'Unknown'
                note_content = f"Technician {tech_name} en route to site"
                # Update appointment
                cursor.execute(f"""
                    UPDATE {self.schema}.appointments SET status = 'en_route', updated_at = CURRENT_TIMESTAMP
                    WHERE work_order_id = %s AND status IN ('confirmed', 'scheduled') ORDER BY appointment_id DESC LIMIT 1
                """, (wo_id,))
                self.enroute_wo_ids.append(wo_id)

            elif to_status == 'in_progress':
                cursor.execute(f"""
                    UPDATE {self.schema}.work_orders
                    SET status = 'in_progress', updated_at = CURRENT_TIMESTAMP
                    WHERE work_order_id = %s RETURNING assigned_technician_id
                """, (wo_id,))
                tech_id = cursor.fetchone()[0]
                tech_name = self.technician_data.get(tech_id, {}).get('name', 'Unknown') if tech_id else 'Unknown'
                note_content = f"Technician {tech_name} on site, work in progress"
                # Update appointment
                cursor.execute(f"""
                    UPDATE {self.schema}.appointments
                    SET status = 'in_progress', actual_start = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                    WHERE work_order_id = %s AND status IN ('en_route', 'confirmed') ORDER BY appointment_id DESC LIMIT 1
                """, (wo_id,))
                if tech_id and tech_id in self.technician_data:
                    self.technician_data[tech_id]['status'] = 'on_site'
                self.inprogress_wo_ids.append(wo_id)

            elif to_status == 'completed':
                # Check SLA
                cursor.execute(f"""
                    SELECT sla_due_at FROM {self.schema}.work_orders WHERE work_order_id = %s
                """, (wo_id,))
                sla_due = cursor.fetchone()[0]
                sla_met = sla_due is None or datetime.now() <= sla_due if sla_due else True

                cursor.execute(f"""
                    UPDATE {self.schema}.work_orders
                    SET status = 'completed', resolved_at = CURRENT_TIMESTAMP, closed_at = CURRENT_TIMESTAMP,
                        sla_met = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE work_order_id = %s RETURNING assigned_technician_id
                """, (sla_met, wo_id))
                result = cursor.fetchone()
                tech_id = result[0] if result else None

                if not sla_met:
                    self.stats['sla_breaches'] += 1

                # Complete appointment with rating
                rating = random.choices([3, 4, 5], weights=[0.15, 0.35, 0.50])[0]
                onsite_min = random.randint(20, 150)
                cursor.execute(f"""
                    UPDATE {self.schema}.appointments
                    SET status = 'completed', actual_end = CURRENT_TIMESTAMP,
                        on_site_time_min = %s, customer_rating = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE work_order_id = %s AND status IN ('in_progress', 'en_route')
                    ORDER BY appointment_id DESC LIMIT 1
                """, (onsite_min, rating, wo_id))
                self.stats['appointments_completed'] += 1

                # Free up technician
                if tech_id and tech_id in self.technician_data:
                    self.technician_data[tech_id]['status'] = 'available'

                tech_name = self.technician_data.get(tech_id, {}).get('name', 'Unknown') if tech_id else 'Unknown'
                sla_text = 'MET' if sla_met else 'BREACHED'
                note_content = f"Work order completed by {tech_name}. SLA {sla_text}. Customer rating: {rating}/5."

                # Add tech note
                tech_note = random.choice(TECH_NOTES)
                # Fill in some placeholders
                tech_note = tech_note.replace('{signal_level}', str(round(random.uniform(-25, -8), 1)))
                tech_note = tech_note.replace('{signal_assessment}', random.choice(['within spec', 'marginal', 'below threshold']))
                tech_note = tech_note.replace('{old_sn}', f"SN-{random.randint(10000000, 99999999)}")
                tech_note = tech_note.replace('{damage_type}', random.choice(['water ingress', 'rodent damage', 'UV degradation', 'physical break']))
                tech_note = tech_note.replace('{dl_speed}', str(random.randint(400, 980)))
                tech_note = tech_note.replace('{ul_speed}', str(random.randint(100, 500)))
                tech_note = tech_note.replace('{location}', random.choice(['service entrance', 'pedestal', 'distribution panel', 'aerial tap']))
                tech_note = tech_note.replace('{firmware_ver}', f"v{random.randint(3,5)}.{random.randint(0,9)}.{random.randint(1,20)}")
                tech_note = tech_note.replace('{cable_type}', random.choice(['fiber', 'Cat6A', 'coax']))
                tech_note = tech_note.replace('{from_loc}', random.choice(['pole', 'pedestal', 'building entry']))
                tech_note = tech_note.replace('{to_loc}', random.choice(['customer demarc', 'ONT location', 'MDF room']))
                tech_note = tech_note.replace('{loss}', str(round(random.uniform(1.5, 12.0), 1)))
                tech_note = tech_note.replace('{old_equip}', random.choice(['ONT-G844', 'Router-AX4200', 'Modem-VMG4005']))
                tech_note = tech_note.replace('{new_equip}', random.choice(['ONT-G844G-v2', 'Router-AX6600', 'CPE-FWA-5G']))
                tech_note = tech_note.replace('{splice_loc}', random.choice(['splice closure #42', 'handhole B-17', 'vault entrance']))
                tech_note = tech_note.replace('{otdr_loss}', str(round(random.uniform(0.1, 0.5), 2)))
                tech_note = tech_note.replace('{connector_loc}', random.choice(['patch panel', 'outside drop', 'splice tray']))
                tech_note = tech_note.replace('{room}', random.choice(['living room', 'home office', 'master bedroom', 'basement']))
                tech_note = tech_note.replace('{old_signal}', random.choice(['-72dBm', '-68dBm', '-75dBm']))
                tech_note = tech_note.replace('{new_signal}', random.choice(['-45dBm', '-38dBm', '-42dBm']))
                tech_note = tech_note.replace('{vlan_id}', str(random.randint(100, 999)))
                tech_note = tech_note.replace('{fuel_pct}', str(random.randint(70, 100)))
                tech_note = tech_note.replace('{runtime_min}', str(random.randint(15, 60)))
                tech_note = tech_note.replace('{ref_id}', f"ESC-{random.randint(100000, 999999)}")
                tech_note = tech_note.replace('{rating}', str(rating))

                cursor.execute(f"""
                    INSERT INTO {self.schema}.work_order_notes (work_order_id, author, note_type, content)
                    VALUES (%s, %s, 'tech_note', %s)
                """, (wo_id, tech_name, tech_note))
                self.stats['notes_added'] += 1

                # Consume parts for install/repair
                if category in ('install', 'repair', 'upgrade'):
                    cpe_types = [et for et in self.equipment_types if et['category'] == 'CPE']
                    if cpe_types:
                        equip = random.choice(cpe_types)
                        cursor.execute(f"""
                            INSERT INTO {self.schema}.work_order_parts (work_order_id, inventory_id, quantity, action)
                            SELECT %s, inventory_id, 1,
                                CASE WHEN %s = 'install' THEN 'installed' ELSE 'replaced' END
                            FROM {self.schema}.equipment_inventory
                            WHERE equipment_type_id = %s AND status = 'in_stock' AND region_id = %s
                            ORDER BY RANDOM() LIMIT 1
                        """, (wo_id, category, equip['id'], region_id))
                        if cursor.rowcount > 0:
                            self.stats['parts_consumed'] += 1

            # Add status change note
            if note_content:
                cursor.execute(f"""
                    INSERT INTO {self.schema}.work_order_notes (work_order_id, author, note_type, content)
                    VALUES (%s, 'System', 'status_change', %s)
                """, (wo_id, note_content))
                self.stats['notes_added'] += 1

            self.stats['status_transitions'] += 1

            return {
                'type': 'status_transition',
                'wo_number': wo_number,
                'from_status': from_status,
                'to_status': to_status,
                'priority': priority,
                'region': self.region_data.get(region_id, {}).get('code', '?'),
                'tech': tech_name,
            }

        except Exception as e:
            self.stats['errors'] += 1
            return {'type': 'error', 'source': 'transition_wo', 'message': str(e)[:200]}
        finally:
            cursor.close()

    def trigger_weather_event(self) -> Optional[Dict]:
        """Randomly trigger a weather event that causes a spike of correlated work orders."""
        if self.active_weather:
            return None  # Only one at a time

        event = random.choice(WEATHER_EVENTS)
        region_id = self._pick_region()
        region_code = self.region_data[region_id]['code']

        self.active_weather = {
            **event,
            'region_id': region_id,
            'started_at': datetime.now(),
            'expires_at': datetime.now() + timedelta(hours=event['duration_hours']),
        }
        self.stats['weather_events'] += 1

        return {
            'type': 'weather_event',
            'name': event['name'],
            'region': region_code,
            'duration_hours': event['duration_hours'],
            'multiplier': event['multiplier'],
        }

    def check_sla_breaches(self) -> Optional[Dict]:
        """Check for work orders about to breach SLA and escalate."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"""
                SELECT work_order_id, work_order_number, priority, region_id,
                       EXTRACT(EPOCH FROM (sla_due_at - CURRENT_TIMESTAMP))/3600 as hours_remaining
                FROM {self.schema}.work_orders
                WHERE status NOT IN ('completed', 'cancelled')
                AND sla_due_at IS NOT NULL
                AND sla_due_at < CURRENT_TIMESTAMP + INTERVAL '2 hours'
                AND sla_due_at > CURRENT_TIMESTAMP
                ORDER BY sla_due_at ASC LIMIT 3
            """)
            rows = cursor.fetchall()
            if not rows:
                return None

            wo_id, wo_number, priority, region_id, hours_remaining = rows[0]
            hours_remaining = round(float(hours_remaining), 1)

            cursor.execute(f"""
                INSERT INTO {self.schema}.work_order_notes (work_order_id, author, note_type, content)
                VALUES (%s, 'System', 'escalation', %s)
            """, (wo_id, f"SLA BREACH WARNING: Only {hours_remaining}h remaining. Priority: {priority.upper()}. Auto-escalating."))

            self.stats['escalations'] += 1
            self.stats['notes_added'] += 1

            return {
                'type': 'sla_warning',
                'wo_number': wo_number,
                'priority': priority,
                'hours_remaining': hours_remaining,
                'region': self.region_data.get(region_id, {}).get('code', '?'),
            }
        except Exception as e:
            self.stats['errors'] += 1
            return None
        finally:
            cursor.close()

    def create_new_customer(self) -> Optional[Dict]:
        """Simulate a new customer signup."""
        cursor = self.conn.cursor()
        try:
            ts = int(time.time() * 1000)
            region_id = self._pick_region()
            first_names = ['Alex', 'Sam', 'Jordan', 'Taylor', 'Morgan', 'Casey', 'Riley', 'Avery',
                          'Quinn', 'Dakota', 'Reese', 'Cameron', 'Hayden', 'Skyler', 'Drew', 'Blake']
            last_names = ['Smith', 'Johnson', 'Chen', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
                         'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Wilson', 'Anderson', 'Thomas', 'Lee']
            fn = random.choice(first_names)
            ln = random.choice(last_names)
            email_providers = ['gmail.com', 'yahoo.com', 'outlook.com', 'icloud.com',
                               'hotmail.com', 'protonmail.com', 'aol.com', 'comcast.net']
            email = f"{fn.lower()}.{ln.lower()}.{ts % 10000}@{random.choice(email_providers)}"
            tier = random.choices(['residential', 'small_business', 'enterprise'], weights=[0.75, 0.17, 0.08])[0]
            revenue = {'residential': round(random.uniform(49, 149), 2),
                      'small_business': round(random.uniform(149, 499), 2),
                      'enterprise': round(random.uniform(499, 2999), 2)}[tier]

            cursor.execute(f"""
                INSERT INTO {self.schema}.customers (
                    account_number, first_name, last_name, email, phone,
                    address_line1, city, state_province, postal_code, region_id,
                    customer_tier, account_status, service_type, contract_start, monthly_revenue
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, 'active', 'fiber', CURRENT_DATE, %s
                ) RETURNING customer_id
            """, (f'ACCT-{ts % 10000000:07d}', fn, ln, email,
                  f'+1{random.randint(2000000000, 9999999999)}',
                  f'{random.randint(100, 9999)} {random.choice(["Oak", "Maple", "Pine", "Cedar", "Elm", "Birch", "Walnut", "Main", "Park", "Lake", "River", "Summit"])} {random.choice(["St", "Ave", "Blvd", "Dr", "Ln", "Way"])}',
                  *random.choice(REGION_CITIES.get(self.region_data.get(region_id, {}).get('code', 'NE'), REGION_CITIES['NE'])),
                  f'{random.randint(10000, 99999)}', region_id,
                  tier, revenue))

            cid = cursor.fetchone()[0]
            self.customer_ids.append(cid)
            self.stats['new_customers'] += 1

            return {
                'type': 'new_customer',
                'name': f"{fn} {ln}",
                'tier': tier,
                'region': self.region_data[region_id]['code'],
                'revenue': revenue,
            }
        except Exception as e:
            self.stats['errors'] += 1
            return {'type': 'error', 'source': 'new_customer', 'message': str(e)[:200]}
        finally:
            cursor.close()

    # ── Dashboard Output ────────────────────────────────────────────────

    def print_dashboard(self, recent_events: List[Dict]):
        """Print a live terminal dashboard (CLI mode)."""
        print("\033[2J\033[H", end='')

        elapsed = datetime.now() - self.start_time
        elapsed_str = str(elapsed).split('.')[0]
        C = Colors

        print(f"{C.BOLD}{C.BLUE}{'='*90}{C.ENDC}")
        print(f"{C.BOLD}{C.BLUE}  FieldOps Real-Time Data Generator{C.ENDC}")
        print(f"{C.BLUE}{'='*90}{C.ENDC}\n")

        tod = self._time_of_day_multiplier()
        dow = self._day_of_week_multiplier()
        volume_pct = int(tod * dow * 100)
        weather_str = f" | {C.RED}WEATHER: {self.active_weather['name']} ({self.region_data[self.active_weather['region_id']]['code']}){C.ENDC}" if self.active_weather else ""

        print(f"  {C.GREEN}● LIVE{C.ENDC}  |  Running: {elapsed_str}  |  Volume: {volume_pct}%{weather_str}\n")

        print(f"  {C.BOLD}Activity:{C.ENDC}")
        print(f"    📋 Work Orders Created:     {C.CYAN}{self.stats['work_orders_created']:,}{C.ENDC}")
        print(f"    🔄 Status Transitions:      {C.CYAN}{self.stats['status_transitions']:,}{C.ENDC}")
        print(f"    📅 Appointments Created:    {C.GREEN}{self.stats['appointments_created']:,}{C.ENDC}")
        print(f"    ✅ Appointments Completed:  {C.GREEN}{self.stats['appointments_completed']:,}{C.ENDC}")
        print(f"    🔧 Parts Consumed:          {C.YELLOW}{self.stats['parts_consumed']:,}{C.ENDC}")
        print(f"    📝 Notes Added:             {C.DIM}{self.stats['notes_added']:,}{C.ENDC}")
        print(f"    👤 New Customers:           {C.GREEN}{self.stats['new_customers']:,}{C.ENDC}")
        print(f"    ⚠️  SLA Breaches:            {C.RED}{self.stats['sla_breaches']:,}{C.ENDC}")
        print(f"    🌩️  Weather Events:          {C.ORANGE}{self.stats['weather_events']:,}{C.ENDC}")
        print(f"    🔺 Escalations:             {C.RED}{self.stats['escalations']:,}{C.ENDC}")
        if self.stats['errors'] > 0:
            print(f"    ❌ Errors:                  {C.RED}{self.stats['errors']:,}{C.ENDC}")

        if elapsed.total_seconds() > 0:
            rate = (self.stats['work_orders_created'] + self.stats['status_transitions']) / elapsed.total_seconds()
            print(f"\n    📊 Event Rate: {C.BOLD}{rate:.1f}{C.ENDC} events/sec")

        print(f"\n  {C.BOLD}Recent Activity:{C.ENDC}")
        print(f"  {C.BLUE}{'─'*86}{C.ENDC}")

        for event in recent_events[-12:]:
            ts = event.get('_timestamp', datetime.now().strftime("%H:%M:%S"))
            etype = event.get('type', 'unknown')

            if etype == 'work_order':
                pri_color = C.RED if event['priority'] == 'critical' else C.ORANGE if event['priority'] == 'high' else C.YELLOW if event['priority'] == 'medium' else C.ENDC
                weather_tag = f" {C.RED}⛈{C.ENDC}" if event.get('weather_related') else ""
                print(f"  {C.CYAN}[{ts}]{C.ENDC} 📋 NEW: {event['wo_number']} {pri_color}[{event['priority'].upper()}]{C.ENDC} {event['title'][:45]} ({event['region']}){weather_tag}")

            elif etype == 'status_transition':
                arrow = f"{event['from_status']} → {event['to_status']}"
                icon = '✅' if event['to_status'] == 'completed' else '🔄'
                print(f"  {C.GREEN}[{ts}]{C.ENDC} {icon} {event['wo_number']}: {arrow} ({event.get('tech', 'System')})")

            elif etype == 'weather_event':
                print(f"  {C.RED}[{ts}]{C.ENDC} 🌩️  WEATHER: {event['name']} in {event['region']} — {event['multiplier']}x volume for {event['duration_hours']}h")

            elif etype == 'sla_warning':
                print(f"  {C.RED}[{ts}]{C.ENDC} ⚠️  SLA BREACH WARNING: {event['wo_number']} [{event['priority'].upper()}] — {event['hours_remaining']}h remaining!")

            elif etype == 'new_customer':
                print(f"  {C.GREEN}[{ts}]{C.ENDC} 👤 New {event['tier']} customer: {event['name']} ({event['region']}) ${event['revenue']}/mo")

            elif etype == 'error':
                print(f"  {C.RED}[{ts}]{C.ENDC} ❌ Error ({event.get('source', '?')}): {event.get('message', '')[:60]}")

        print(f"  {C.BLUE}{'─'*86}{C.ENDC}\n")

    def print_notebook_dashboard(self, recent_events: List[Dict]):
        """Print dashboard for Databricks notebook (ANSI colors, throttled)."""
        now = time.time()
        if hasattr(self, '_last_display_time') and (now - self._last_display_time) < 2.0:
            return
        self._last_display_time = now

        elapsed = datetime.now() - self.start_time
        elapsed_str = str(elapsed).split('.')[0]
        C = Colors
        lines = []
        a = lines.append

        a(f"{C.BOLD}{C.BLUE}{'='*90}{C.ENDC}")
        a(f"{C.BOLD}{C.BLUE}  🔧 FieldOps Real-Time Data Generator{C.ENDC}")
        a(f"{C.BLUE}{'='*90}{C.ENDC}")
        a("")

        token_info = ""
        if self.connection_factory and self.connection_factory._last_token_time:
            token_age = int((time.time() - self.connection_factory._last_token_time) / 60)
            token_color = C.GREEN if token_age < 20 else C.YELLOW if token_age < 25 else C.RED
            token_info = f" | {token_color}🔑 Token: {token_age}m{C.ENDC}"

        weather_info = ""
        if self.active_weather:
            weather_info = f" | {C.RED}⛈ {self.active_weather['name']}{C.ENDC}"

        a(f"  {C.GREEN}● LIVE{C.ENDC}  |  Running: {elapsed_str}{token_info}{weather_info}")
        a("")
        a(f"  {C.BOLD}Activity:{C.ENDC}")
        a(f"    📋 Work Orders:    {C.CYAN}{self.stats['work_orders_created']:,}{C.ENDC}  |  🔄 Transitions: {C.CYAN}{self.stats['status_transitions']:,}{C.ENDC}  |  ✅ Completed: {C.GREEN}{self.stats['appointments_completed']:,}{C.ENDC}")
        a(f"    🔧 Parts:          {C.YELLOW}{self.stats['parts_consumed']:,}{C.ENDC}  |  👤 Customers:   {C.GREEN}{self.stats['new_customers']:,}{C.ENDC}  |  ⚠️  SLA Breach: {C.RED}{self.stats['sla_breaches']:,}{C.ENDC}")

        if elapsed.total_seconds() > 0:
            rate = (self.stats['work_orders_created'] + self.stats['status_transitions']) / elapsed.total_seconds()
            a(f"\n    📊 Event Rate: {C.BOLD}{rate:.1f}{C.ENDC} events/sec")

        a(f"\n  {C.BOLD}Recent Activity:{C.ENDC}")
        a(f"  {C.BLUE}{'─'*86}{C.ENDC}")

        for event in recent_events[-10:]:
            ts = event.get('_timestamp', datetime.now().strftime("%H:%M:%S"))
            etype = event.get('type', 'unknown')
            if etype == 'work_order':
                pri_color = C.RED if event['priority'] == 'critical' else C.ORANGE if event['priority'] == 'high' else C.YELLOW
                a(f"  {C.CYAN}[{ts}]{C.ENDC} 📋 {event['wo_number']} {pri_color}[{event['priority'].upper()}]{C.ENDC} {event['title'][:40]} ({event['region']})")
            elif etype == 'status_transition':
                icon = '✅' if event['to_status'] == 'completed' else '🔄'
                a(f"  {C.GREEN}[{ts}]{C.ENDC} {icon} {event['wo_number']}: {event['from_status']} → {event['to_status']}")
            elif etype == 'weather_event':
                a(f"  {C.RED}[{ts}]{C.ENDC} 🌩️  WEATHER: {event['name']} in {event['region']}")
            elif etype == 'sla_warning':
                a(f"  {C.RED}[{ts}]{C.ENDC} ⚠️  SLA: {event['wo_number']} — {event['hours_remaining']}h left!")
            elif etype == 'new_customer':
                a(f"  {C.GREEN}[{ts}]{C.ENDC} 👤 {event['name']} ({event['tier']}, {event['region']})")
            elif etype == 'error':
                a(f"  {C.RED}[{ts}]{C.ENDC} ❌ {event.get('message', '')[:60]}")

        a(f"  {C.BLUE}{'─'*86}{C.ENDC}")

        try:
            from IPython.display import clear_output
            clear_output(wait=True)
        except ImportError:
            pass
        print("\n".join(lines), flush=True)

    # ── Token Refresh ───────────────────────────────────────────────────

    def _refresh_connection(self):
        try:
            self.conn.close()
        except Exception:
            pass
        self.conn = self.connection_factory.create_connection()
        self.conn.autocommit = True

    # ── Main Run Loop ───────────────────────────────────────────────────

    def run(self, duration_minutes: int = 15, speed_factor: float = 1.0):
        """Run the field service data generator with realistic patterns."""
        if self.notebook_mode:
            print(f"Starting field service simulation for {duration_minutes} minutes (speed: {speed_factor}x)...")
        else:
            print(f"\n{Colors.GREEN}Starting field service simulation...{Colors.ENDC}\n")
            print(f"Duration: {duration_minutes} min | Speed: {speed_factor}x | Press Ctrl+C to stop\n")
            time.sleep(2)

        end_time = datetime.now() + timedelta(minutes=duration_minutes)
        recent_events = []
        sf = max(speed_factor, 0.1)

        while self.running and not self._stop_event.is_set() and datetime.now() < end_time:
            # Token refresh
            if self.connection_factory and self.connection_factory.needs_refresh():
                try:
                    self._refresh_connection()
                except Exception as e:
                    msg = f"Token refresh failed: {e}"
                    if self.notebook_mode:
                        print(f"Warning: {msg}")
                    else:
                        print(f"{Colors.RED}{msg}{Colors.ENDC}")

            # Check and expire weather events
            if self.active_weather and datetime.now() > self.active_weather['expires_at']:
                self.active_weather = None

            # Volume multiplier based on time of day and day of week
            volume = self._time_of_day_multiplier() * self._day_of_week_multiplier()

            # Event selection with realistic distribution
            rand = random.random()
            event = None

            if rand < 0.35 * volume:
                # New work order (35% base, modulated by volume)
                event = self.create_work_order()
                time.sleep(random.uniform(1.0, 4.0) / sf)

            elif rand < 0.70:
                # Status transition (35% — always processing the queue)
                event = self.transition_work_order()
                time.sleep(random.uniform(0.5, 2.0) / sf)

            elif rand < 0.80:
                # SLA breach check (10%)
                event = self.check_sla_breaches()
                time.sleep(random.uniform(2.0, 5.0) / sf)

            elif rand < 0.90:
                # New customer (10%)
                event = self.create_new_customer()
                time.sleep(random.uniform(3.0, 8.0) / sf)

            elif rand < 0.98:
                # Idle / wait cycle (8%)
                time.sleep(random.uniform(1.0, 3.0) / sf)
                continue

            else:
                # Weather event trigger (2% chance per cycle)
                event = self.trigger_weather_event()
                time.sleep(random.uniform(0.5, 1.0) / sf)

            if event:
                event['_timestamp'] = datetime.now().strftime("%H:%M:%S")
                recent_events.append(event)

            # Update display
            if self.notebook_mode:
                self.print_notebook_dashboard(recent_events)
            else:
                self.print_dashboard(recent_events)

        # Final summary
        self._print_final_summary()

        try:
            self.conn.close()
        except Exception:
            pass

    def _print_final_summary(self):
        C = Colors
        elapsed = datetime.now() - self.start_time
        elapsed_str = str(elapsed).split('.')[0]
        total = sum(v for k, v in self.stats.items() if k != 'errors')

        if self.notebook_mode:
            try:
                from IPython.display import clear_output
                clear_output(wait=True)
            except ImportError:
                pass

        print(f"\n{C.GREEN}{'='*90}{C.ENDC}")
        print(f"{C.GREEN}  ✅ Field Service Simulation Complete!{C.ENDC}")
        print(f"{C.GREEN}{'='*90}{C.ENDC}\n")
        print(f"  Duration: {elapsed_str}  |  {total:,} total events\n")
        print(f"  {C.BOLD}Final Statistics:{C.ENDC}")
        print(f"    📋 Work Orders Created:     {self.stats['work_orders_created']:,}")
        print(f"    🔄 Status Transitions:      {self.stats['status_transitions']:,}")
        print(f"    📅 Appointments Created:    {self.stats['appointments_created']:,}")
        print(f"    ✅ Appointments Completed:  {self.stats['appointments_completed']:,}")
        print(f"    🔧 Parts Consumed:          {self.stats['parts_consumed']:,}")
        print(f"    📝 Notes Added:             {self.stats['notes_added']:,}")
        print(f"    👤 New Customers:           {self.stats['new_customers']:,}")
        print(f"    ⚠️  SLA Breaches:            {self.stats['sla_breaches']:,}")
        print(f"    🌩️  Weather Events:          {self.stats['weather_events']:,}")
        print(f"    🔺 Escalations:             {self.stats['escalations']:,}")
        print(f"    ❌ Errors:                  {self.stats['errors']:,}")
        print(f"\n{'='*90}\n")


def main():
    config_params = load_config_if_available()

    parser = argparse.ArgumentParser(
        description='Real-Time Field Service Data Generator for Lakebase FSM Demo'
    )

    required = not bool(config_params)

    parser.add_argument('--host', required=required,
                       default=config_params.get('host') if config_params else None)
    parser.add_argument('--database', required=required,
                       default=config_params.get('database') if config_params else None)
    parser.add_argument('--user', required=required,
                       default=config_params.get('user') if config_params else None)
    parser.add_argument('--password', required=required,
                       default=config_params.get('password') if config_params else None)
    parser.add_argument('--port', type=int,
                       default=config_params.get('port', 5432) if config_params else 5432)
    parser.add_argument('--schema',
                       default=config_params.get('schema', 'field_service') if config_params else 'field_service')
    parser.add_argument('--duration', type=int, default=15,
                       help='Duration in minutes (default: 15)')
    parser.add_argument('--speed', type=float, default=1.0,
                       help='Speed multiplier (default: 1.0, use 2.0 for 2x speed)')

    args = parser.parse_args()

    try:
        print(f"\n{Colors.BOLD}{Colors.BLUE}FieldOps Real-Time Data Generator{Colors.ENDC}")
        print(f"{Colors.BLUE}{'=' * 90}{Colors.ENDC}\n")

        if config_params:
            print(f"{Colors.GREEN}✓ Loaded configuration from config.yaml{Colors.ENDC}")

        print(f"Connection: {args.host}")
        print(f"Database:   {args.database}")
        print(f"Schema:     {args.schema}")
        print(f"Duration:   {args.duration} minutes")
        print(f"Speed:      {args.speed}x\n")

        generator = FieldServiceGenerator(
            host=args.host, database=args.database,
            user=args.user, password=args.password,
            port=args.port, schema=args.schema
        )
        generator.run(duration_minutes=args.duration, speed_factor=args.speed)

    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Interrupted by user{Colors.ENDC}\n")
    except Exception as e:
        print(f"\n{Colors.RED}Error: {e}{Colors.ENDC}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
