-- ============================================================================
-- TELCO FIELD SERVICE MANAGEMENT - OPERATIONAL DATABASE
-- For Lakebase -> Unity Catalog -> Genie / DBSQL / AI|BI Integration
-- ============================================================================
--
-- Use Case: Telco Field Service Operations
--   Technician dispatch, work orders, equipment inventory, SLA tracking
--
-- Architecture:
--   Lakebase (PostgreSQL OLTP) -> Unity Catalog -> Foreign Catalog -> DBSQL
--   Databricks App (Flask) R/W -> Lakebase
--   AI/BI Genie Spaces for natural language queries
--   Foundation Model Serving for ticket classification
--
-- Business Questions This Answers:
--   - How many open work orders by priority and region?
--   - What is our SLA compliance rate this week?
--   - Which technicians have the highest first-fix rate?
--   - Are we running low on equipment in any region?
--   - What is the average time to resolution by category?
--   - Which regions have the most unassigned critical work orders?
--
-- Idempotent: Uses DROP IF EXISTS + CREATE for clean re-runs.
-- ============================================================================

SET client_min_messages = NOTICE;

-- Create schema
CREATE SCHEMA IF NOT EXISTS field_service;

COMMENT ON SCHEMA field_service IS 'Telco field service operations - technician dispatch, work orders, equipment, SLA tracking. Exposed to Unity Catalog for Genie and DBSQL analytics.';

-- ============================================================================
-- REFERENCE / LOOKUP TABLES
-- ============================================================================

-- Service Regions
DROP TABLE IF EXISTS field_service.service_regions CASCADE;

CREATE TABLE field_service.service_regions (
    region_id       SERIAL PRIMARY KEY,
    region_name     VARCHAR(100) NOT NULL,
    region_code     VARCHAR(10) UNIQUE NOT NULL,
    state_list      TEXT,
    timezone        VARCHAR(50) NOT NULL,
    manager_name    VARCHAR(100),
    population_weight NUMERIC(3,2) DEFAULT 1.0,
    is_urban        BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE field_service.service_regions IS 'Geographic service regions for field operations. Each technician and customer belongs to exactly one region.';

INSERT INTO field_service.service_regions (region_name, region_code, state_list, timezone, manager_name, population_weight, is_urban) VALUES
('Pacific Northwest',  'PNW', 'WA,OR,ID',       'America/Los_Angeles',  'Sarah Mitchell',    0.85, TRUE),
('Southwest',          'SW',  'AZ,NM,NV,UT',    'America/Phoenix',      'Carlos Rodriguez',  0.90, TRUE),
('South Central',      'SC',  'TX,OK,AR,LA',     'America/Chicago',      'James Parker',      1.20, TRUE),
('Southeast',          'SE',  'FL,GA,NC,SC,VA',  'America/New_York',     'Angela Foster',     1.15, TRUE),
('Midwest',            'MW',  'IL,OH,MI,IN,WI',  'America/Chicago',      'David Kowalski',    1.00, TRUE),
('Northeast',          'NE',  'NY,NJ,PA,MA,CT',  'America/New_York',     'Patricia Chen',     1.10, TRUE);


-- Metro Territories — sub-regions within each service region for realistic GPS clustering.
-- Each territory is a metro area with a center lat/lng and radius.
-- Technicians and work orders cluster around these centers (±5km).
DROP TABLE IF EXISTS field_service.metro_territories CASCADE;

CREATE TABLE field_service.metro_territories (
    territory_id    SERIAL PRIMARY KEY,
    territory_name  VARCHAR(100) NOT NULL,
    territory_code  VARCHAR(10) UNIQUE NOT NULL,
    region_id       INTEGER REFERENCES field_service.service_regions(region_id),
    center_latitude  NUMERIC(10,6) NOT NULL,
    center_longitude NUMERIC(10,6) NOT NULL,
    radius_km       NUMERIC(5,1) DEFAULT 10.0,
    tech_weight     INTEGER DEFAULT 1,
    is_urban        BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE field_service.metro_territories IS 'Metro-level territories within service regions. Used for realistic GPS clustering of technicians and work orders. Each territory represents a metro area where field crews operate.';

INSERT INTO field_service.metro_territories (territory_name, territory_code, region_id, center_latitude, center_longitude, radius_km, tech_weight, is_urban) VALUES
-- Pacific Northwest (region_id = 1)
('Seattle',       'SEA', 1, 47.6062, -122.3321, 12.0, 3, TRUE),
('Portland',      'PDX', 1, 45.5152, -122.6784, 10.0, 2, TRUE),
('Boise',         'BOI', 1, 43.6150, -116.2023,  8.0, 1, FALSE),
('Tacoma',        'TAC', 1, 47.2529, -122.4443,  8.0, 1, TRUE),
('Spokane',       'SPO', 1, 47.6588, -117.4260,  8.0, 1, FALSE),
-- Southwest (region_id = 2)
('Phoenix',       'PHX', 2, 33.4484, -112.0740, 15.0, 3, TRUE),
('Las Vegas',     'LVG', 2, 36.1699, -115.1398, 12.0, 2, TRUE),
('Salt Lake City','SLC', 2, 40.7608, -111.8910, 10.0, 1, TRUE),
('Albuquerque',   'ABQ', 2, 35.0844, -106.6504,  8.0, 1, FALSE),
('Tucson',        'TUC', 2, 32.2226, -110.9747,  8.0, 1, FALSE),
-- South Central (region_id = 3)
('Dallas',        'DAL', 3, 32.7767,  -96.7970, 15.0, 3, TRUE),
('Houston',       'HOU', 3, 29.7604,  -95.3698, 15.0, 3, TRUE),
('Austin',        'AUS', 3, 30.2672,  -97.7431, 10.0, 1, TRUE),
('Oklahoma City', 'OKC', 3, 35.4676,  -97.5164,  8.0, 1, FALSE),
-- Southeast (region_id = 4)
('Atlanta',       'ATL', 4, 33.7490,  -84.3880, 15.0, 2, TRUE),
('Miami',         'MIA', 4, 25.7617,  -80.1918, 12.0, 2, TRUE),
('Tampa',         'TPA', 4, 27.9506,  -82.4572, 10.0, 1, TRUE),
('Charlotte',     'CLT', 4, 35.2271,  -80.8431, 10.0, 1, TRUE),
('Orlando',       'ORL', 4, 28.5383,  -81.3792, 10.0, 1, TRUE),
('Nashville',     'NSH', 4, 36.1627,  -86.7816,  8.0, 1, FALSE),
-- Midwest (region_id = 5)
('Chicago',       'CHI', 5, 41.8781,  -87.6298, 15.0, 3, TRUE),
('Detroit',       'DET', 5, 42.3314,  -83.0458, 10.0, 1, TRUE),
('Columbus',      'COL', 5, 39.9612,  -82.9988, 10.0, 1, TRUE),
('Indianapolis',  'IND', 5, 39.7684,  -86.1581, 10.0, 1, TRUE),
('Minneapolis',   'MSP', 5, 44.9778,  -93.2650, 10.0, 1, TRUE),
('Milwaukee',     'MKE', 5, 43.0389,  -87.9065,  8.0, 1, TRUE),
-- Northeast (region_id = 6)
('New York',      'NYC', 6, 40.7484,  -73.9857, 12.0, 3, TRUE),
('Philadelphia',  'PHL', 6, 39.9526,  -75.1652, 10.0, 1, TRUE),
('Boston',        'BOS', 6, 42.3601,  -71.0589, 10.0, 1, TRUE),
('Washington DC', 'DCA', 6, 38.9072,  -77.0369, 12.0, 2, TRUE),
('Baltimore',     'BWI', 6, 39.2904,  -76.6122,  8.0, 1, TRUE);


-- Skill Types
DROP TABLE IF EXISTS field_service.skill_types CASCADE;

CREATE TABLE field_service.skill_types (
    skill_id               SERIAL PRIMARY KEY,
    skill_name             VARCHAR(100) NOT NULL,
    skill_category         VARCHAR(50) NOT NULL,
    certification_required BOOLEAN DEFAULT FALSE,
    description            TEXT,
    created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE field_service.skill_types IS 'Certifications and skills that technicians can hold. Work orders require specific skills for assignment.';

INSERT INTO field_service.skill_types (skill_name, skill_category, certification_required, description) VALUES
-- Installation skills
('Fiber Optic Installation',    'installation', TRUE,  'Install and terminate single/multi-mode fiber including ONTs'),
('Copper Line Installation',    'installation', FALSE, 'Install and test copper twisted-pair for DSL and POTS service'),
('Coaxial Cable Installation',  'installation', FALSE, 'Install and terminate coaxial for HFC network service'),
('Satellite Dish Installation', 'installation', TRUE,  'Mount and align satellite dishes for DBS service'),
('5G Small Cell Installation',  'installation', TRUE,  'Install and configure 5G small cell equipment on poles and structures'),
-- Repair skills
('Fiber Optic Repair',         'repair',       TRUE,  'Splice, test and repair fiber optic cables and ONT equipment'),
('Copper Line Repair',         'repair',       FALSE, 'Diagnose and repair copper plant issues including DSLAM-side'),
('Network Troubleshooting',    'repair',       FALSE, 'End-to-end network issue diagnosis using signal meters and analyzers'),
('CPE Troubleshooting',        'repair',       FALSE, 'Customer premises equipment diagnostics and replacement'),
('Aerial Line Repair',         'repair',       TRUE,  'Pole-mounted and aerial cable repair requiring bucket truck and safety cert'),
-- Maintenance skills
('Preventive Maintenance',     'maintenance',  FALSE, 'Scheduled inspection and maintenance of outside plant'),
('Tower Climbing',             'maintenance',  TRUE,  'Cell tower climbing for inspection and equipment maintenance'),
('Generator Maintenance',      'maintenance',  FALSE, 'Backup generator testing and maintenance at cell sites'),
('Splicing and Termination',   'maintenance',  TRUE,  'Fiber and copper splicing for plant maintenance and upgrades'),
-- Network skills
('ONT Configuration',          'network',      TRUE,  'Configure and provision Optical Network Terminals'),
('Router Configuration',       'network',      FALSE, 'Configure customer-facing routers and WiFi APs'),
('VLAN Configuration',         'network',      TRUE,  'Configure VLANs and managed switches for business customers'),
('RF Testing and Alignment',   'network',      TRUE,  'Radio frequency testing for wireless and satellite services'),
('Spectrum Analysis',          'network',      TRUE,  'Advanced RF spectrum analysis for interference troubleshooting'),
-- Safety
('Confined Space Entry',       'safety',       TRUE,  'Certified for confined space work in manholes and vaults');


-- Equipment Catalog
DROP TABLE IF EXISTS field_service.equipment_catalog CASCADE;

CREATE TABLE field_service.equipment_catalog (
    equipment_type_id   SERIAL PRIMARY KEY,
    equipment_name      VARCHAR(150) NOT NULL,
    category            VARCHAR(50) NOT NULL,
    manufacturer        VARCHAR(100),
    model_number        VARCHAR(100),
    unit_cost           NUMERIC(10,2),
    avg_lifespan_months INTEGER,
    is_serialized       BOOLEAN DEFAULT TRUE,
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE field_service.equipment_catalog IS 'Master catalog of equipment types available for field service operations.';

INSERT INTO field_service.equipment_catalog (equipment_name, category, manufacturer, model_number, unit_cost, avg_lifespan_months, is_serialized) VALUES
-- Customer Premises Equipment (CPE)
('Fiber ONT - Residential',         'CPE',              'Calix',       'GigaPoint 844G',    189.00, 60, TRUE),
('Fiber ONT - Business',            'CPE',              'Calix',       'GigaPro 854G',      349.00, 60, TRUE),
('WiFi 6E Router',                  'CPE',              'CommScope',   'AX6600',             129.00, 36, TRUE),
('WiFi 6E Mesh Extender',           'CPE',              'CommScope',   'AX6600-EXT',          89.00, 36, TRUE),
('Coax Set-Top Box',                'CPE',              'EchoStar',    'Hopper 4',           249.00, 48, TRUE),
('DSL Modem',                       'CPE',              'Zyxel',       'VMG4005-B50A',        79.00, 36, TRUE),
('Business VoIP Gateway',           'CPE',              'Grandstream', 'GXW4248',            399.00, 48, TRUE),
('Fixed Wireless CPE',              'CPE',              'Ericsson',    'FWA-5G-CPE',         449.00, 48, TRUE),
-- Tools
('Fiber Splice Kit',                'tool',             'Fujikura',    'FSM-90S+',          8500.00, 96, TRUE),
('OTDR Fiber Tester',               'tool',             'EXFO',        'MaxTester 730C',    5200.00, 72, TRUE),
('Signal Level Meter',              'tool',             'Trilithic',   'CT-6',              1200.00, 60, TRUE),
('Cable Tone Generator & Probe',    'tool',             'Fluke',       'IntelliTone Pro',    249.00, 60, TRUE),
('Ethernet Cable Tester',           'tool',             'Fluke',       'LinkIQ',             799.00, 60, TRUE),
('Power Meter & Light Source',      'tool',             'AFL',         'OPM5-2D',            890.00, 60, TRUE),
('Crimping Tool Set',               'tool',             'Klein',       'VDV226-110',          89.00, 36, FALSE),
-- Network Components
('Fiber Patch Panel - 24 Port',     'network_component','Corning',     'CCH-024',            320.00, 120, TRUE),
('Ethernet Switch - 24 Port PoE',   'network_component','Cisco',       'CBS350-24P',         650.00, 84, TRUE),
('UPS Battery Backup',              'network_component','APC',         'SMT1500RM2U',        890.00, 48, TRUE),
('Outdoor Fiber Enclosure',         'network_component','Corning',     'FDC-024',            185.00, 120, TRUE),
('5G Small Cell Radio',             'network_component','Ericsson',    'Dot 6402',          2800.00, 84, TRUE),
-- Consumables (not serialized)
('Fiber Optic Cable - 500ft',       'consumable',       'Corning',     'SMF-28e+',           245.00, NULL, FALSE),
('Cat6A Ethernet Cable - 1000ft',   'consumable',       'Belden',      '10GXW12',            389.00, NULL, FALSE),
('Coaxial Cable RG6 - 500ft',       'consumable',       'Commscope',   'F677TSVV',            89.00, NULL, FALSE),
('Fiber Splice Protectors - 100pk', 'consumable',       'Fujikura',    'FP-03',               45.00, NULL, FALSE),
('Cable Ties Assorted - 1000pk',    'consumable',       'Panduit',     'PLT-M',               22.00, NULL, FALSE),
('Weatherproof Connectors - 50pk',  'consumable',       'PPC',         'EX6XL PLUS',          65.00, NULL, FALSE);


-- SLA Policies
DROP TABLE IF EXISTS field_service.sla_policies CASCADE;

CREATE TABLE field_service.sla_policies (
    sla_id           SERIAL PRIMARY KEY,
    sla_name         VARCHAR(100) NOT NULL,
    priority         VARCHAR(20) NOT NULL CHECK (priority IN ('low', 'medium', 'high', 'critical')),
    response_hours   INTEGER NOT NULL,
    resolution_hours INTEGER NOT NULL,
    customer_tier    VARCHAR(50),
    penalty_per_hour NUMERIC(10,2) DEFAULT 0,
    is_active        BOOLEAN DEFAULT TRUE,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE field_service.sla_policies IS 'Service Level Agreement policies defining response and resolution time targets by priority and customer tier.';

INSERT INTO field_service.sla_policies (sla_name, priority, response_hours, resolution_hours, customer_tier, penalty_per_hour) VALUES
-- Residential SLAs
('Residential - Low',        'low',      48, 120, 'residential',     0.00),
('Residential - Medium',     'medium',   24,  72, 'residential',     0.00),
('Residential - High',       'high',      8,  24, 'residential',     0.00),
('Residential - Critical',   'critical',  4,  12, 'residential',    25.00),
-- Small Business SLAs
('Business - Low',           'low',      24,  72, 'small_business', 10.00),
('Business - Medium',        'medium',   12,  48, 'small_business', 25.00),
('Business - High',          'high',      4,  16, 'small_business', 50.00),
('Business - Critical',      'critical',  2,   8, 'small_business',100.00),
-- Enterprise SLAs
('Enterprise - Low',         'low',      12,  48, 'enterprise',     50.00),
('Enterprise - Medium',      'medium',    8,  24, 'enterprise',    100.00),
('Enterprise - High',        'high',      2,   8, 'enterprise',    250.00),
('Enterprise - Critical',    'critical',  1,   4, 'enterprise',    500.00);


-- ============================================================================
-- CORE TRANSACTIONAL TABLES
-- ============================================================================

-- Customers
DROP TABLE IF EXISTS field_service.customers CASCADE;

CREATE TABLE field_service.customers (
    customer_id       BIGSERIAL PRIMARY KEY,
    account_number    VARCHAR(20) UNIQUE NOT NULL,
    first_name        VARCHAR(100) NOT NULL,
    last_name         VARCHAR(100) NOT NULL,
    email             VARCHAR(200) NOT NULL,
    phone             VARCHAR(20),
    address_line1     VARCHAR(200),
    address_line2     VARCHAR(200),
    city              VARCHAR(100),
    state_province    VARCHAR(50),
    postal_code       VARCHAR(20),
    region_id         INTEGER REFERENCES field_service.service_regions(region_id),
    customer_tier     VARCHAR(50) DEFAULT 'residential' CHECK (customer_tier IN ('residential', 'small_business', 'enterprise')),
    account_status    VARCHAR(20) DEFAULT 'active' CHECK (account_status IN ('active', 'suspended', 'cancelled', 'pending')),
    service_type      VARCHAR(50) DEFAULT 'fiber',
    contract_start    DATE,
    contract_end      DATE,
    monthly_revenue   NUMERIC(10,2),
    lifetime_value    NUMERIC(12,2),
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE field_service.customers IS 'Customer accounts with service addresses and contract details. PII columns (email, phone, address) should be masked for non-admin roles.';

-- Generate 50,000 customers across 6 regions
INSERT INTO field_service.customers (
    account_number, first_name, last_name, email, phone,
    address_line1, city, state_province, postal_code, region_id,
    customer_tier, account_status, service_type,
    contract_start, contract_end, monthly_revenue, lifetime_value
)
SELECT
    'ACCT-' || LPAD(i::TEXT, 7, '0') AS account_number,
    (ARRAY[
        'James','Maria','Robert','Jennifer','Michael','Linda','David','Patricia','William','Elizabeth',
        'Richard','Barbara','Joseph','Susan','Thomas','Jessica','Daniel','Sarah','Matthew','Karen',
        'Anthony','Lisa','Mark','Nancy','Steven','Betty','Andrew','Margaret','Joshua','Sandra',
        'Christopher','Ashley','Brian','Kimberly','Kevin','Donna','Jason','Emily','Ryan','Carol',
        'Jacob','Michelle','Gary','Amanda','Timothy','Dorothy','Jose','Melissa','Larry','Deborah',
        'Jeffrey','Stephanie','Frank','Rebecca','Scott','Sharon','Eric','Laura','Raymond','Cynthia',
        'Gregory','Kathleen','Samuel','Amy','Benjamin','Angela','Patrick','Shirley','Jack','Anna',
        'Henry','Brenda','Walter','Pamela','Dennis','Emma','Jerry','Nicole','Tyler','Helen',
        'Aaron','Samantha','Jose','Katherine','Adam','Christine','Nathan','Debra','Henry','Rachel',
        'Douglas','Carolyn','Zachary','Janet','Peter','Catherine','Kyle','Maria','Noah','Heather',
        'Ethan','Diane','Jeremy','Ruth','Roger','Julie','Keith','Olivia','Terry','Joyce',
        'Austin','Virginia','Sean','Victoria','Christian','Kelly','Albert','Lauren','Joe','Christina',
        'Juan','Joan','Gerald','Evelyn','Clarence','Judith','Philip','Megan','Bobby','Andrea',
        'Russell','Cheryl','Randy','Hannah','Howard','Jacqueline','Carlos','Martha','Eugene','Gloria',
        'Harry','Teresa','Wayne','Ann','Elijah','Sara','Arthur','Madison','Alexander','Frances',
        'Aiden','Janice','Alejandro','Kathryn','Aaliyah','Abigail','Ada','Adeline','Adrian','Adriana',
        'Aisha','Alan','Alana','Aleksei','Alexandra','Ali','Alina','Amara','Amari','Amelia',
        'Amos','Ananya','Andre','Angelo','Anil','Anita','Ankur','Arjun','Arlo','Ash',
        'Astrid','Ayana','Ayumi','Bao','Beatrice','Benedict','Bianca','Bjorn','Boris','Bria',
        'Caleb','Camila','Carmen','Cedric','Chiara','Clara','Colette','Conrad','Damian','Dante',
        'Dara','Darren','Deepa','Devi','Diego','Dmitri','Elena','Elias','Emiko','Emilio',
        'Enrique','Esther','Eva','Farah','Fatima','Felix','Fiona','Gabriel','Gita','Grace',
        'Hana','Haruki','Hassan','Hector','Hiroshi','Hugo','Idris','Ingrid','Irene','Isaiah',
        'Isla','Ivan','Ivy','Jabari','Jade','Javier','Jin','Joaquin','Juno','Kai',
        'Kaia','Kamila','Kaori','Kenji','Khalid','Kiara','Kofi','Lakshmi','Leila','Leo'
    ])[((i * 347 + 131) % 250) + 1] AS first_name,
    (ARRAY[
        'Smith','Johnson','Chen','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez',
        'Hernandez','Lopez','Gonzalez','Wilson','Anderson','Thomas','Taylor','Moore','Park','Martin',
        'Lee','Perez','Thompson','White','Harris','Sanchez','Clark','Ramirez','Lewis','Robinson',
        'Walker','Young','Allen','King','Wright','Scott','Torres','Nguyen','Hill','Flores',
        'Green','Adams','Nelson','Baker','Hall','Rivera','Campbell','Mitchell','Carter','Roberts',
        'Gomez','Phillips','Evans','Turner','Diaz','Parker','Cruz','Edwards','Collins','Reyes',
        'Stewart','Morris','Morales','Murphy','Cook','Rogers','Gutierrez','Ortiz','Morgan','Cooper',
        'Peterson','Bailey','Reed','Kelly','Howard','Ramos','Kim','Cox','Ward','Richardson',
        'Watson','Brooks','Chavez','Wood','James','Bennett','Gray','Mendoza','Ruiz','Hughes',
        'Price','Alvarez','Castillo','Sanders','Patel','Myers','Long','Ross','Foster','Jimenez',
        'Powell','Jenkins','Perry','Russell','Sullivan','Bell','Coleman','Butler','Henderson','Barnes',
        'Gonzales','Fisher','Vasquez','Simmons','Graham','Murray','Ford','Castro','Yamamoto','Tanaka',
        'Nakamura','Watanabe','Suzuki','Takahashi','Sato','Kobayashi','Johansson','Eriksson','Lindqvist','Bergstrom',
        'Muller','Schmidt','Becker','Fischer','Weber','Meyer','Wagner','Schulz','Hoffmann','Richter',
        'Bianchi','Romano','Colombo','Ferrari','Esposito','Rizzo','Greco','Moreau','Dubois','Laurent',
        'Lefevre','Petit','Roux','Bernard','Okonkwo','Adeyemi','Okafor','Mensah','Asante','Toure',
        'Diallo','Kamara','Sow','Keita','Cisse','Ibrahim','Khalil','Hassan','Ahmad','Rashid',
        'Sharma','Gupta','Patel','Singh','Kumar','Agarwal','Joshi','Deshmukh','Iyer','Nair',
        'Ochieng','Mwangi','Kariuki','Wanjiku','Kamau','Andrade','Oliveira','Pereira','Costa','Silva',
        'Fernandes','Souza','Santos','Lima','Almeida','Kowalski','Nowak','Wojcik','Zielinski','Lewandowski',
        'Novak','Horvat','Kovacs','Szabo','Nagy','Petrov','Ivanov','Popov','Kuznetsov','Volkov',
        'Chang','Liu','Wang','Zhang','Wu','Yang','Huang','Zhou','Xu','Sun',
        'Kwon','Yoon','Shin','Han','Kang','Choi','Medina','Vargas','Solis','Padilla',
        'Contreras','Lara','Espinoza','Delgado','Aguilar','Cabrera','Acosta','Rojas','Herrera','Navarro',
        'Virtanen','Korhonen','Makinen','Nieminen','Laine','Heikkinen','Koskinen','Bakken','Holm','Dahl',
        'Strand'
    ])[((i * 409 + 79) % 251) + 1] AS last_name,
    'customer' || i || '@' ||
        CASE ((i * 11) % 8)
            WHEN 0 THEN 'gmail.com' WHEN 1 THEN 'yahoo.com' WHEN 2 THEN 'outlook.com'
            WHEN 3 THEN 'icloud.com' WHEN 4 THEN 'hotmail.com' WHEN 5 THEN 'comcast.net'
            WHEN 6 THEN 'att.net' WHEN 7 THEN 'protonmail.com'
        END AS email,
    '+1' || LPAD(((i::BIGINT * 123456789) % 9000000000 + 1000000000)::TEXT, 10, '0') AS phone,
    (100 + (i % 9900))::TEXT || ' ' ||
        CASE ((i * 3) % 12)
            WHEN 0 THEN 'Oak St' WHEN 1 THEN 'Maple Ave' WHEN 2 THEN 'Cedar Blvd'
            WHEN 3 THEN 'Pine Dr' WHEN 4 THEN 'Elm Way' WHEN 5 THEN 'Birch Ln'
            WHEN 6 THEN 'Walnut Ct' WHEN 7 THEN 'Main St' WHEN 8 THEN 'Park Ave'
            WHEN 9 THEN 'Lake Dr' WHEN 10 THEN 'River Ln' WHEN 11 THEN 'Summit Way'
        END AS address_line1,
    -- City and state tied to metro territories within each region (8 weighted slots)
    (ARRAY[
        'Seattle','Seattle','Seattle','Portland','Portland','Boise','Tacoma','Spokane',
        'Phoenix','Phoenix','Phoenix','Las Vegas','Las Vegas','Salt Lake City','Albuquerque','Tucson',
        'Dallas','Dallas','Dallas','Houston','Houston','Houston','Austin','Oklahoma City',
        'Atlanta','Atlanta','Miami','Miami','Tampa','Charlotte','Orlando','Nashville',
        'Chicago','Chicago','Chicago','Detroit','Columbus','Indianapolis','Minneapolis','Milwaukee',
        'New York','New York','New York','Philadelphia','Boston','Washington','Washington','Baltimore'
    ])[((i * 7) % 6) * 8 + ((i * 11) % 8) + 1] AS city,
    (ARRAY[
        'WA','WA','WA','OR','OR','ID','WA','WA',
        'AZ','AZ','AZ','NV','NV','UT','NM','AZ',
        'TX','TX','TX','TX','TX','TX','TX','OK',
        'GA','GA','FL','FL','FL','NC','FL','TN',
        'IL','IL','IL','MI','OH','IN','MN','WI',
        'NY','NY','NY','PA','MA','DC','DC','MD'
    ])[((i * 7) % 6) * 8 + ((i * 11) % 8) + 1] AS state_province,
    LPAD((10000 + (i * 17) % 89999)::TEXT, 5, '0') AS postal_code,
    ((i * 7) % 6) + 1 AS region_id,
    CASE
        WHEN (i % 100) < 75 THEN 'residential'
        WHEN (i % 100) < 92 THEN 'small_business'
        ELSE 'enterprise'
    END AS customer_tier,
    CASE
        WHEN (i % 100) < 90 THEN 'active'
        WHEN (i % 100) < 95 THEN 'suspended'
        WHEN (i % 100) < 98 THEN 'cancelled'
        ELSE 'pending'
    END AS account_status,
    CASE ((i * 3) % 5)
        WHEN 0 THEN 'fiber' WHEN 1 THEN 'fiber' WHEN 2 THEN 'copper_dsl'
        WHEN 3 THEN 'coaxial' WHEN 4 THEN 'fixed_wireless'
    END AS service_type,
    CURRENT_DATE - ((i * 17) % 1825)::INTEGER AS contract_start,
    CURRENT_DATE - ((i * 17) % 1825)::INTEGER + 730 AS contract_end,
    CASE
        WHEN (i % 100) < 75 THEN 49.99 + (i % 10) * 10
        WHEN (i % 100) < 92 THEN 149.99 + (i % 10) * 25
        ELSE 499.99 + (i % 10) * 100
    END AS monthly_revenue,
    CASE
        WHEN (i % 100) < 75 THEN 500.00 + (i % 100) * 50
        WHEN (i % 100) < 92 THEN 5000.00 + (i % 100) * 200
        ELSE 25000.00 + (i % 100) * 1000
    END AS lifetime_value
FROM generate_series(1, {customers}) i;


-- Technicians
DROP TABLE IF EXISTS field_service.technicians CASCADE;

CREATE TABLE field_service.technicians (
    technician_id       BIGSERIAL PRIMARY KEY,
    employee_id         VARCHAR(20) UNIQUE NOT NULL,
    first_name          VARCHAR(100) NOT NULL,
    last_name           VARCHAR(100) NOT NULL,
    email               VARCHAR(200),
    phone               VARCHAR(20),
    region_id           INTEGER REFERENCES field_service.service_regions(region_id),
    status              VARCHAR(20) DEFAULT 'available' CHECK (status IN ('available', 'en_route', 'on_site', 'off_duty', 'on_leave', 'training')),
    shift               VARCHAR(20) DEFAULT 'day' CHECK (shift IN ('day', 'evening', 'on_call')),
    hire_date           DATE,
    certification_level VARCHAR(50) CHECK (certification_level IN ('junior', 'standard', 'senior', 'lead')),
    vehicle_id          VARCHAR(50),
    current_latitude    NUMERIC(10,6),
    current_longitude   NUMERIC(10,6),
    avg_rating          NUMERIC(3,2) DEFAULT 4.00,
    jobs_completed_mtd  INTEGER DEFAULT 0,
    jobs_completed_ytd  INTEGER DEFAULT 0,
    first_fix_rate      NUMERIC(5,2) DEFAULT 85.00,
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE field_service.technicians IS 'Field technicians with skills, certifications, and real-time status. Key persona for the dispatch board.';

-- Generate technicians across 6 regions
-- Uses 500 first names x 500 last names = 250,000 unique combos.
-- Hash uses coprime multipliers with modulus 500 so every technician
-- gets a near-unique full name even at scale preset (2,500 techs).
INSERT INTO field_service.technicians (
    employee_id, first_name, last_name, email, phone, region_id,
    status, shift, hire_date, certification_level, vehicle_id,
    current_latitude, current_longitude, avg_rating,
    jobs_completed_mtd, jobs_completed_ytd, first_fix_rate
)
SELECT
    'TECH-' || LPAD(i::TEXT, 4, '0') AS employee_id,
    (ARRAY[
        'Jake','Maya','Tyler','Aisha','Brandon','Sophia','Derek','Yuki','Marcus','Elena',
        'Ryan','Priya','Kevin','Nicole','Omar','Ashley','Travis','Carmen','Liam','Destiny',
        'James','Maria','Andre','Keiko','David','Lucia','Tyrone','Anita','Scott','Mei',
        'Chris','Fatima','Darnell','Rosa','Patrick','Suki','Jason','Diana','Raj','Tanya',
        'Miguel','Hana','Vincent','Zara','Darius','Leah','Santos','Nadia','Bo','Ingrid',
        'Caleb','Amara','Felix','Yelena','Terrance','Jia','Emmett','Lina','Hugo','Miriam',
        'Isaiah','Ayumi','Grant','Serena','Diego','Freya','Martin','Celeste','Aaron','Iris',
        'Kendrick','Vera','Saul','Noor','Dean','Paloma','Colton','Remi','Ivan','Ada',
        'Malachi','Sana','Reid','Xiomara','Desmond','Harley','Nico','Brynn','Elias','Alma',
        'Cedric','Thea','Rowan','Kira','Dante','Mira','Tobias','Sage','Ezra','Luna',
        'Abel','Tessa','Kai','Isla','Silas','Wren','Nash','Cleo','Orion','Jade',
        'Beckett','Esme','Atlas','Ivy','Knox','Aria','Hayes','Olive','Quinn','Lyra',
        'Axel','Nyla','Cruz','Zuri','Lane','Pearl','River','Skye','Zane','Stella',
        'Archer','Faye','Brooks','Eden','Finn','Dahlia','Reed','Marlowe','Cole','Hazel',
        'Dallas','Lennox','Ellis','Petra','Heath','Greta','Blake','Darcy','Carter','Maeve',
        'Jordan','Sloane','Casey','Milan','Devin','Senna','Morgan','Tatum','Riley','Arden',
        'Drew','Haven','Emery','Simone','Harper','Teagan','Avery','Reese','Skyler','Sutton',
        'Taylor','Blair','Logan','Finley','Parker','Dylan','Peyton','Rory','Hayden','Marlow',
        'Cameron','Bailey','Sawyer','Lennon','Jesse','Robin','Chandler','Phoenix','Caspian','Winter',
        'Alex','Dakota','Shea','Kendall','Aubrey','Jamie','Jules','Remy','Frankie','Sam',
        'Mateo','Lilith','Owen','Amira','Jace','Naomi','Ezekiel','Valentina','Bennett','Athena',
        'Santiago','Camila','Asher','Penelope','Leo','Willow','Grayson','Emilia','Jack','Violet',
        'Theodore','Scarlett','Aiden','Katya','Julian','Paisley','Hudson','Savannah','Nolan','Audrey',
        'Lincoln','Brooklyn','Elijah','Bella','Mason','Aurora','Conrad','Chloe','Sebastian','Zoey',
        'Henry','Nora','Harrison','Lily','Levi','Ellie','Josiah','Mila','Oliver','Nova',
        'Wyatt','Aaliyah','Gabriel','Sienna','Jaxon','Bianca','Kellan','Marisol','Nathaniel','Ariana',
        'Dominic','Gianna','Connor','Thalia','Malcolm','Selma','Austin','Hailey','Cooper','Ruby',
        'Xavier','Kinsley','Weston','Mackenzie','Damian','Madelyn','Roman','Caroline','Miles','Piper',
        'Tristan','Rylee','Victor','Sadie','Maxwell','Serenity','Dorian','Everly','Maddox','Laila',
        'Chance','Vivian','Griffin','Josephine','Everett','Nevaeh','Micah','Gabriella','Remington','Adeline',
        'Waylon','Raelynn','Wesley','Catalina','Tucker','Emerson','Jasper','Makayla','Brody','Mariana',
        'Marshall','Juliana','Holden','Adalynn','Quinton','Kenzie','August','Harmony','Clay','Elsie',
        'Sullivan','Alaina','Brock','Ramona','Barrett','Liliana','Rhett','Daisy','Garrett','Meadow',
        'Callum','Rosalie','Alaric','Leilani','Beau','Juliette','Kade','Raegan','Landon','Selena',
        'Gage','Alina','Jett','Braelyn','Sterling','Aniyah','Tate','Noelle','Zander','Raven',
        'Preston','Londyn','Colby','Mckenna','Denver','Hadley','Arlo','Tamsin','Fintan','Amaya',
        'Ryder','Liora','Bodhi','Gracie','Sirius','Zinnia','Otto','Journee','Gideon','Ruth',
        'Tanner','Marley','Corbin','Adalyn','Shane','Palmer','Cash','Corinne','Theron','Ondine',
        'Marcel','Lola','Ace','Margot','Cade','Gemma','Cyrus','Elliot','Lewis','Miette',
        'Kian','Briar','Ford','Cosette','Hendrix','Brielle','Crew','Oakley','Wells','Linnea',
        'Anders','Willa','Kane','Harlow','Luca','Stevie','Fritz','Odette','Torsten','Holland',
        'Magnus','Cora','Soren','Talia','Lars','Annika','Viggo','Signe','Bjorn','Astrid',
        'Aksel','Solveig','Eero','Elsa','Leif','Anja','Stellan','Maren','Alvar','Tuva',
        'Kieran','Maura','Ronan','Niamh','Cillian','Saoirse','Lorcan','Aoife','Eamon','Roisin',
        'Ravi','Meera','Arjun','Pooja','Vikram','Ananya','Nikhil','Kavya','Aarav','Diya',
        'Hiroshi','Sakura','Kenji','Haruki','Takumi','Fumiko','Ren','Hina','Kaito','Aoi',
        'Jin','Sooyun','Minho','Jisoo','Taehyung','Chaewon','Joon','Nari','Seung','Eunji',
        'Wei','Lixue','Hao','Xiuying','Jian','Shu','Liang','Xia','Bowen','Yue',
        'Tariq','Zahra','Idris','Yasmin','Omari','Safiya','Farid','Amina','Jabari','Zainab',
        'Kofi','Abena','Kwame','Efua','Yaw','Akua','Kojo','Ama','Binta','Sekou'
    ])[((i * 347 + 131) % 500) + 1] AS first_name,
    (ARRAY[
        'Morgan','Santos','Okafor','Patel','Nakamura','Kim','Murphy','Tanaka','Carter','Vasquez',
        'Foster','Nguyen','Brooks','Reyes','Hassan','Cooper','Walsh','Rivera','Bennett','Washington',
        'Adeyemi','Johansson','Fitzgerald','Kowalski','DeSilva','Marchetti','Yamamoto','Svensson',
        'Oduya','Chakraborty','Petrov','Gonzalez','MacLeod','Abadi','Lindqvist','Toure','Ramos',
        'Okamura','Henriksen','Achebe','Molina','Takahashi','Bergstrom','Osei','Guerrero','Watanabe',
        'Kristiansen','Diallo','Herrera','Suzuki','Andersen','Mensah','Castillo','Hayashi','Eriksson',
        'Asante','Delgado','Saito','Larsson','Koffi','Mendoza','Mori','Nilsson','Boateng','Aguilar',
        'Inoue','Persson','Lemaire','Sandoval','Ishida','Jonasson','Ferreira','Rojas','Kimura','Lund',
        'Ouattara','Fuentes','Sato','Dahl','Adjei','Barrera','Hashimoto','Holm','Bamba','Espinoza',
        'Kobayashi','Strand','Coulibaly','Valdez','Aoki','Hauge','Konare','Salazar','Abe','Berge',
        'Keita','Acosta','Taniguchi','Vang','Cisse','Paredes','Ogawa','Haugen','Fofana','Trujillo',
        'Nishimura','Solheim','Sy','Navarrete','Fujita','Moen','Kamara','Bautista','Ueda','Lindgren',
        'Savane','Montoya','Matsuda','Aas','Sangare','Carrillo','Endo','Bakken','Dembele','Zavala',
        'Hasegawa','Lien','Sidibe','Ponce','Goto','Dale','Kone','Estrada','Hirano','Vik',
        'Bagayoko','Galvan','Nishida','Brekke','Doumbia','Cisneros','Morimoto','Hoie','Sissoko',
        'Ochoa','Shimizu','Nordby','Drabo','Rosales','Tsuda','Foss','Maiga','Iglesias','Hori',
        'Camara','Villarreal','Ozawa','Nygaard','Sow','Coronado','Kondo','Eide','Niasse','Partida',
        'Hamada','Borg','Sakho','Sedano','Arai','Falk','Ouedraogo','Camacho','Ikeda','Helland',
        'Koroma','Ornelas','Wada','Roen','Tounkara','Huerta','Miura','Gundersen','Koulibaly',
        'Beltran','Ota','Ellingsen','Dabo','Renteria','Ogata','Hetland','Samake','Alvarado',
        'Thorne','Visser','Okonkwo','Janssen','Reuter','Lindholm','Machado','Hofmann',
        'Alekseev','Baranov','Volkov','Gusev','Dmitriev','Yegorov','Zhukov','Ivanov','Kuznetsov',
        'Lebedev','Morozov','Novikov','Orlov','Popov','Romanov','Sokolov','Fedorov','Yakovlev',
        'Bergman','Carlsson','Dahlberg','Ekström','Fransson','Gustafsson','Hedlund','Isaksson',
        'Jakobsson','Karlsson','Lundin','Magnusson','Nyström','Olofsson','Petersson','Ringström',
        'Sjöberg','Thorsson','Vikström','Wahlström','Björk','Engström','Holmgren','Sandström',
        'Ahmad','Bakir','Darwish','Farouk','Habib','Ibrahim','Jaber','Khalil','Mansour','Nasser',
        'Qasim','Rashid','Saleh','Taha','Wazir','Yousef','Zahran','Almasi','Boutros','Dagher',
        'Ochieng','Mwangi','Wanjiku','Kamau','Ndungu','Wainaina','Kariuki','Njoroge','Muthoni','Gitau',
        'Bhandari','Choudhury','Deshmukh','Ghosh','Iyer','Joshi','Kulkarni','Menon','Nair','Pillai',
        'Raghavan','Sharma','Thakur','Upadhyay','Venkatesh','Yadav','Acharya','Banerjee','Dutta','Gupta',
        'Andrade','Barbosa','Carvalho','Dias','Fernandes','Gomes','Henriques','Lima','Marques','Oliveira',
        'Pereira','Ribeiro','Sousa','Teixeira','Vieira','Almeida','Batista','Costa','Duarte','Fonseca',
        'Garcia','Hawkins','Ingram','Jensen','Keller','Lambert','Mitchell','Norton','Palmer','Quinn',
        'Rhodes','Spencer','Turner','Underwood','Vaughn','Watson','Young','Zimmerman','Abbott','Baldwin',
        'Chambers','Donovan','Ellison','Fletcher','Gibson','Hamilton','Irwin','Jacobs','Kemp','Lawson',
        'Manning','Neal','Osborne','Price','Ramsey','Sinclair','Thornton','Upton','Valentine','Whitman',
        'Blackwell','Crawford','Dalton','Emerson','Gallagher','Harding','Kendall','Lancaster','Mercer',
        'Nicholson','Payne','Randall','Shelton','Townsend','Whitfield','Archer','Boyd','Cross','Dixon',
        'Eaton','Floyd','Grant','Hull','Kent','Lane','Moss','Page','Ross','Stone',
        'Webb','York','Adler','Braun','Fischer','Hartmann','Kaiser','Lehmann','Meyer','Richter',
        'Schneider','Wagner','Weber','Becker','Decker','Engel','Fuchs','Grüber','Huber','Klein',
        'Bianchi','Conti','DeLuca','Esposito','Ferrari','Greco','Leone','Marino','Pellegrini','Romano',
        'Moreau','Dubois','Laurent','Lefevre','Martin','Petit','Roux','Simon','Vincent','Bernard',
        'Virtanen','Korhonen','Mäkinen','Nieminen','Hämäläinen','Laine','Heikkinen','Koskinen','Järvinen',
        'Coppola','Mancini','Rizzo','Santoro','Vitale','Colombo','Galli','Grassi','Longo','Neri',
        'Takeda','Morita','Okada','Kawai','Sugiyama','Matsumoto','Sakamoto','Yoshida','Nakagawa','Ueno',
        'Park','Choi','Kang','Yoon','Jang','Lim','Han','Shin','Kwon','Seo',
        'Diaz','Munoz','Ortega','Vargas','Cruz','Padilla','Contreras','Lara','Medina','Solis',
        'Bowen','Marsh','Barker','Prescott','Sharp','Watts','Burke','Sutton','Frost','Hale',
        'Lindberg','Forsberg','Stenberg','Wickström','Edlund','Söderberg','Malmberg','Hallberg','Östlund','Blom',
        'Toussaint','Beaulieu','Charpentier','Deschamps','Girard','Marchand','Pelletier','Renard','Thibault'
    ])[((i * 409 + 79) % 499) + 1] AS last_name,
    'tech' || i || '@fieldops.com' AS email,
    '+1' || LPAD(((i::BIGINT * 987654321) % 9000000000 + 1000000000)::TEXT, 10, '0') AS phone,
    ((i - 1) % 6) + 1 AS region_id,
    CASE
        WHEN (i % 10) < 6 THEN 'available'
        WHEN (i % 10) < 7 THEN 'en_route'
        WHEN (i % 10) < 8 THEN 'on_site'
        WHEN (i % 10) < 9 THEN 'off_duty'
        ELSE 'on_leave'
    END AS status,
    CASE
        WHEN (i % 5) < 3 THEN 'day'
        WHEN (i % 5) < 4 THEN 'evening'
        ELSE 'on_call'
    END AS shift,
    CURRENT_DATE - (90 + (i * 23) % 3650)::INTEGER AS hire_date,
    CASE
        WHEN (i * 23) % 3650 < 365 THEN 'junior'
        WHEN (i * 23) % 3650 < 1460 THEN 'standard'
        WHEN (i * 23) % 3650 < 2920 THEN 'senior'
        ELSE 'lead'
    END AS certification_level,
    'VAN-' || LPAD(i::TEXT, 4, '0') AS vehicle_id,
    -- Metro-clustered GPS: 8 weighted slots per region (big cities repeated for weight)
    -- Spread: ±0.05° ≈ ±5km — techs cluster tightly around their assigned metro
    -- Slot index: region * 8 + (tech_within_region % 8) + 1
    (ARRAY[
        -- PNW: Seattle×3, Portland×2, Boise, Tacoma, Spokane
        47.6062, 47.6062, 47.6062, 45.5152, 45.5152, 43.6150, 47.2529, 47.6588,
        -- SW: Phoenix×3, Las Vegas×2, SLC, Albuquerque, Tucson
        33.4484, 33.4484, 33.4484, 36.1699, 36.1699, 40.7608, 35.0844, 32.2226,
        -- SC: Dallas×3, Houston×3, Austin, OKC
        32.7767, 32.7767, 32.7767, 29.7604, 29.7604, 29.7604, 30.2672, 35.4676,
        -- SE: Atlanta×2, Miami×2, Tampa, Charlotte, Orlando, Nashville
        33.7490, 33.7490, 25.7617, 25.7617, 27.9506, 35.2271, 28.5383, 36.1627,
        -- MW: Chicago×3, Detroit, Columbus, Indianapolis, Minneapolis, Milwaukee
        41.8781, 41.8781, 41.8781, 42.3314, 39.9612, 39.7684, 44.9778, 43.0389,
        -- NE: NYC×3, Philadelphia, Boston, DC×2, Baltimore
        40.7484, 40.7484, 40.7484, 39.9526, 42.3601, 38.9072, 38.9072, 39.2904
    ])[((i - 1) % 6) * 8 + ((i - 1) / 6 % 8) + 1]
    + (RANDOM() * 0.10 - 0.05) AS current_latitude,
    (ARRAY[
        -122.3321,-122.3321,-122.3321,-122.6784,-122.6784,-116.2023,-122.4443,-117.4260,
        -112.0740,-112.0740,-112.0740,-115.1398,-115.1398,-111.8910,-106.6504,-110.9747,
         -96.7970, -96.7970, -96.7970, -95.3698, -95.3698, -95.3698, -97.7431, -97.5164,
         -84.3880, -84.3880, -80.1918, -80.1918, -82.4572, -80.8431, -81.3792, -86.7816,
         -87.6298, -87.6298, -87.6298, -83.0458, -82.9988, -86.1581, -93.2650, -87.9065,
         -73.9857, -73.9857, -73.9857, -75.1652, -71.0589, -77.0369, -77.0369, -76.6122
    ])[((i - 1) % 6) * 8 + ((i - 1) / 6 % 8) + 1]
    + (RANDOM() * 0.10 - 0.05) AS current_longitude,
    3.50 + (RANDOM() * 1.50) AS avg_rating,
    5 + (RANDOM() * 30)::INTEGER AS jobs_completed_mtd,
    50 + (RANDOM() * 300)::INTEGER AS jobs_completed_ytd,
    70.0 + (RANDOM() * 25.0) AS first_fix_rate
FROM generate_series(1, {technicians}) i;


-- Technician Skills (junction table)
DROP TABLE IF EXISTS field_service.technician_skills CASCADE;

CREATE TABLE field_service.technician_skills (
    technician_id     BIGINT REFERENCES field_service.technicians(technician_id),
    skill_id          INTEGER REFERENCES field_service.skill_types(skill_id),
    proficiency_level VARCHAR(20) CHECK (proficiency_level IN ('basic', 'intermediate', 'expert')),
    certified_at      DATE,
    expires_at        DATE,
    PRIMARY KEY (technician_id, skill_id)
);

COMMENT ON TABLE field_service.technician_skills IS 'Maps technicians to their skills and certifications. Used for intelligent dispatch assignment.';

-- Assign technicians realistic skill specializations.
-- Each tech gets a primary specialty based on their ID, then mostly skills in
-- that category with a smaller chance of cross-training in other categories.
-- Leads are cross-trained across all categories; juniors know only 2-3 skills.
INSERT INTO field_service.technician_skills (technician_id, skill_id, proficiency_level, certified_at, expires_at)
SELECT
    t.technician_id,
    s.skill_id,
    -- Proficiency: higher in primary specialty, lower outside
    -- Only 3 levels allowed: basic, intermediate, expert
    CASE
        WHEN s.skill_category = primary_cat THEN
            CASE
                WHEN t.certification_level = 'lead' THEN 'expert'
                WHEN t.certification_level = 'senior' THEN
                    CASE WHEN (t.technician_id * 7 + s.skill_id * 13) % 10 < 7 THEN 'expert' ELSE 'intermediate' END
                WHEN t.certification_level = 'standard' THEN
                    CASE WHEN (t.technician_id * 11 + s.skill_id * 3) % 10 < 3 THEN 'expert' ELSE 'intermediate' END
                ELSE 'basic'
            END
        ELSE  -- secondary category
            CASE
                WHEN t.certification_level = 'lead' THEN
                    CASE WHEN (t.technician_id * 3 + s.skill_id * 17) % 10 < 4 THEN 'expert' ELSE 'intermediate' END
                WHEN t.certification_level = 'senior' THEN 'intermediate'
                ELSE 'basic'
            END
    END AS proficiency_level,
    -- Stagger certification dates deterministically
    CURRENT_DATE - ((t.technician_id * 17 + s.skill_id * 31) % 730)::INTEGER AS certified_at,
    CASE WHEN s.certification_required
         THEN CURRENT_DATE + ((t.technician_id * 13 + s.skill_id * 29) % 365 - 30)::INTEGER
         ELSE NULL END AS expires_at
FROM field_service.technicians t
CROSS JOIN field_service.skill_types s
CROSS JOIN LATERAL (
    -- Map each technician to a primary specialty category
    SELECT CASE (t.technician_id % 5)
        WHEN 0 THEN 'installation'
        WHEN 1 THEN 'repair'
        WHEN 2 THEN 'maintenance'
        WHEN 3 THEN 'network'
        WHEN 4 THEN 'installation'  -- installation-heavy workforce
    END AS primary_cat
) pc
WHERE
    -- Primary specialty: high chance of assignment
    (s.skill_category = primary_cat AND (
        (t.certification_level = 'lead'     AND (t.technician_id * 7 + s.skill_id * 11) % 100 < 90) OR
        (t.certification_level = 'senior'   AND (t.technician_id * 7 + s.skill_id * 11) % 100 < 75) OR
        (t.certification_level = 'standard' AND (t.technician_id * 7 + s.skill_id * 11) % 100 < 55) OR
        (t.certification_level = 'junior'   AND (t.technician_id * 7 + s.skill_id * 11) % 100 < 35)
    ))
    OR
    -- Secondary categories: much lower chance
    (s.skill_category <> primary_cat AND s.skill_category <> 'safety' AND (
        (t.certification_level = 'lead'     AND (t.technician_id * 13 + s.skill_id * 23) % 100 < 30) OR
        (t.certification_level = 'senior'   AND (t.technician_id * 13 + s.skill_id * 23) % 100 < 12) OR
        (t.certification_level = 'standard' AND (t.technician_id * 13 + s.skill_id * 23) % 100 < 5)
        -- juniors: no secondary skills
    ))
    OR
    -- Safety cert: leads and some seniors only
    (s.skill_category = 'safety' AND (
        (t.certification_level = 'lead'     AND (t.technician_id * 19) % 100 < 60) OR
        (t.certification_level = 'senior'   AND (t.technician_id * 19) % 100 < 20)
    ));


-- Work Orders
DROP TABLE IF EXISTS field_service.work_orders CASCADE;

CREATE TABLE field_service.work_orders (
    work_order_id       BIGSERIAL PRIMARY KEY,
    work_order_number   VARCHAR(20) UNIQUE NOT NULL,
    customer_id         BIGINT REFERENCES field_service.customers(customer_id),
    sla_id              INTEGER REFERENCES field_service.sla_policies(sla_id),
    category            VARCHAR(50) NOT NULL CHECK (category IN ('install', 'repair', 'maintenance', 'upgrade', 'disconnect')),
    subcategory         VARCHAR(100),
    priority            VARCHAR(20) NOT NULL DEFAULT 'medium' CHECK (priority IN ('low', 'medium', 'high', 'critical')),
    status              VARCHAR(30) NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'assigned', 'en_route', 'in_progress', 'on_hold', 'completed', 'cancelled')),
    title               TEXT NOT NULL,
    description         TEXT,
    reported_issue      TEXT,
    predicted_category  VARCHAR(50),
    predicted_priority  VARCHAR(20),
    confidence_score    NUMERIC(5,4),
    address_line1       VARCHAR(200),
    city                VARCHAR(100),
    state_province      VARCHAR(50),
    postal_code         VARCHAR(20),
    region_id           INTEGER REFERENCES field_service.service_regions(region_id),
    latitude            NUMERIC(10,6),
    longitude           NUMERIC(10,6),
    assigned_technician_id BIGINT REFERENCES field_service.technicians(technician_id),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sla_due_at          TIMESTAMP,
    first_response_at   TIMESTAMP,
    resolved_at         TIMESTAMP,
    closed_at           TIMESTAMP,
    sla_met             BOOLEAN,
    resolution_notes    TEXT,
    required_skill_id   INTEGER REFERENCES field_service.skill_types(skill_id),
    -- Fleet predictive maintenance: links fleet_maintenance work orders to a vehicle.
    -- Defined here (not via a later ALTER) so it exists before the bulk load and the
    -- fleet schema step never has to ALTER work_orders while it is being loaded.
    vehicle_id          VARCHAR(50)
);

COMMENT ON TABLE field_service.work_orders IS 'Core transactional table for all field service requests. Tracks full lifecycle from creation through SLA-tracked resolution.';

-- Generate 100,000 historical work orders over the past 12 months
INSERT INTO field_service.work_orders (
    work_order_number, customer_id, sla_id, category, subcategory,
    priority, status, title, description, reported_issue,
    address_line1, city, state_province, postal_code, region_id,
    latitude, longitude, assigned_technician_id,
    created_at, updated_at, sla_due_at, first_response_at, resolved_at, closed_at, sla_met
)
SELECT
    'WO-' || TO_CHAR(CURRENT_DATE - ((i * 3) % 365)::INTEGER, 'YYYY') || '-' || LPAD(i::TEXT, 8, '0') AS work_order_number,
    -- Random customer
    (1 + (i * 7) % {customers})::BIGINT AS customer_id,
    -- SLA: match priority (using same formula as priority column) + customer tier
    CASE
        WHEN (i * 31) % 100 < 10 THEN CASE WHEN (i % 3) = 0 THEN 4 WHEN (i % 3) = 1 THEN 8 ELSE 12 END   -- critical
        WHEN (i * 31) % 100 < 30 THEN CASE WHEN (i % 3) = 0 THEN 3 WHEN (i % 3) = 1 THEN 7 ELSE 11 END   -- high
        WHEN (i * 31) % 100 < 70 THEN CASE WHEN (i % 3) = 0 THEN 2 WHEN (i % 3) = 1 THEN 6 ELSE 10 END   -- medium
        ELSE CASE WHEN (i % 3) = 0 THEN 1 WHEN (i % 3) = 1 THEN 5 ELSE 9 END                               -- low
    END AS sla_id,
    -- Category distribution: 25% install, 35% repair, 20% maintenance, 15% upgrade, 5% disconnect
    CASE
        WHEN (i * 13) % 100 < 25 THEN 'install'
        WHEN (i * 13) % 100 < 60 THEN 'repair'
        WHEN (i * 13) % 100 < 80 THEN 'maintenance'
        WHEN (i * 13) % 100 < 95 THEN 'upgrade'
        ELSE 'disconnect'
    END AS category,
    -- Subcategory based on category
    CASE
        WHEN (i * 13) % 100 < 25 THEN
            CASE ((i * 17) % 4) WHEN 0 THEN 'fiber_install' WHEN 1 THEN 'copper_install' WHEN 2 THEN 'satellite_install' ELSE 'fixed_wireless_install' END
        WHEN (i * 13) % 100 < 60 THEN
            CASE ((i * 17) % 6) WHEN 0 THEN 'no_signal' WHEN 1 THEN 'slow_speed' WHEN 2 THEN 'intermittent_connection' WHEN 3 THEN 'equipment_failure' WHEN 4 THEN 'line_damage' ELSE 'noise_on_line' END
        WHEN (i * 13) % 100 < 80 THEN
            CASE ((i * 17) % 3) WHEN 0 THEN 'scheduled_maintenance' WHEN 1 THEN 'equipment_refresh' ELSE 'line_inspection' END
        WHEN (i * 13) % 100 < 95 THEN
            CASE ((i * 17) % 3) WHEN 0 THEN 'speed_upgrade' WHEN 1 THEN 'equipment_upgrade' ELSE 'service_tier_change' END
        ELSE 'service_disconnect'
    END AS subcategory,
    -- Priority distribution: 30% low, 40% medium, 20% high, 10% critical
    CASE
        WHEN (i * 31) % 100 < 10 THEN 'critical'
        WHEN (i * 31) % 100 < 30 THEN 'high'
        WHEN (i * 31) % 100 < 70 THEN 'medium'
        ELSE 'low'
    END AS priority,
    -- Status: 96% completed, 1.5% cancelled, 2.5% active (realistic for field ops)
    CASE
        WHEN (i * 19) % 1000 < 960 THEN 'completed'
        WHEN (i * 19) % 1000 < 975 THEN 'cancelled'
        WHEN (i * 19) % 1000 < 983 THEN 'open'
        WHEN (i * 19) % 1000 < 990 THEN 'assigned'
        WHEN (i * 19) % 1000 < 995 THEN 'en_route'
        WHEN (i * 19) % 1000 < 998 THEN 'in_progress'
        ELSE 'on_hold'
    END AS status,
    -- Title based on category and subcategory
    CASE
        WHEN (i * 13) % 100 < 25 THEN 'New ' ||
            CASE ((i * 17) % 4) WHEN 0 THEN 'Fiber' WHEN 1 THEN 'Copper/DSL' WHEN 2 THEN 'Satellite' ELSE 'Fixed Wireless' END || ' Installation'
        WHEN (i * 13) % 100 < 60 THEN
            CASE ((i * 17) % 6) WHEN 0 THEN 'No Signal - Service Outage' WHEN 1 THEN 'Slow Speed - Below Tier' WHEN 2 THEN 'Intermittent Connection Drops'
                WHEN 3 THEN 'Equipment Malfunction' WHEN 4 THEN 'Damaged Line Reported' ELSE 'Noise/Static on Line' END
        WHEN (i * 13) % 100 < 80 THEN 'Scheduled ' ||
            CASE ((i * 17) % 3) WHEN 0 THEN 'Preventive Maintenance' WHEN 1 THEN 'Equipment Refresh' ELSE 'Line Inspection' END
        WHEN (i * 13) % 100 < 95 THEN
            CASE ((i * 17) % 3) WHEN 0 THEN 'Speed Tier Upgrade' WHEN 1 THEN 'Equipment Upgrade' ELSE 'Service Tier Change' END
        ELSE 'Service Disconnection Request'
    END AS title,
    'Work order created via ' ||
        CASE ((i * 11) % 4) WHEN 0 THEN 'customer call center' WHEN 1 THEN 'online portal' WHEN 2 THEN 'mobile app' ELSE 'field technician report' END
    AS description,
    CASE
        WHEN (i * 13) % 100 < 60 THEN
            CASE ((i * 17) % 8)
                WHEN 0 THEN 'My internet has been down since this morning, all lights on the modem are off'
                WHEN 1 THEN 'Speed test shows 10mbps but I am paying for 500mbps, this has been going on for days'
                WHEN 2 THEN 'Connection keeps dropping every 15-20 minutes, have to restart the router each time'
                WHEN 3 THEN 'The ONT box is making a clicking noise and the power light is flashing red'
                WHEN 4 THEN 'After the storm last night there is a cable hanging from the pole in my backyard'
                WHEN 5 THEN 'Terrible static noise on my phone line, can barely hear anything'
                WHEN 6 THEN 'WiFi signal is great but no internet, tried rebooting everything multiple times'
                WHEN 7 THEN 'TV service pixelating badly on all channels, audio cutting in and out'
            END
        ELSE NULL
    END AS reported_issue,
    -- Address, city, state, GPS — all metro-clustered to match technician territories
    (100 + (i % 9900))::TEXT || ' ' ||
        (ARRAY['Oak St','Maple Ave','Cedar Blvd','Pine Dr','Elm Way','Birch Ln',
               'Walnut Ct','Main St','Park Ave','Lake Dr','River Ln','Summit Way'
        ])[(i * 3 % 12) + 1] AS address_line1,
    -- City matches metro slot (same distribution as GPS below)
    (ARRAY[
        'Seattle','Seattle','Seattle','Portland','Portland','Boise','Tacoma','Spokane',
        'Phoenix','Phoenix','Phoenix','Las Vegas','Las Vegas','Salt Lake City','Albuquerque','Tucson',
        'Dallas','Dallas','Dallas','Houston','Houston','Houston','Austin','Oklahoma City',
        'Atlanta','Atlanta','Miami','Miami','Tampa','Charlotte','Orlando','Nashville',
        'Chicago','Chicago','Chicago','Detroit','Columbus','Indianapolis','Minneapolis','Milwaukee',
        'New York','New York','New York','Philadelphia','Boston','Washington','Washington','Baltimore'
    ])[((i * 7) % 6) * 8 + ((i * 11) % 8) + 1] AS city,
    (ARRAY[
        'WA','WA','WA','OR','OR','ID','WA','WA',
        'AZ','AZ','AZ','NV','NV','UT','NM','AZ',
        'TX','TX','TX','TX','TX','TX','TX','OK',
        'GA','GA','FL','FL','FL','NC','FL','TN',
        'IL','IL','IL','MI','OH','IN','MN','WI',
        'NY','NY','NY','PA','MA','DC','DC','MD'
    ])[((i * 7) % 6) * 8 + ((i * 11) % 8) + 1] AS state_province,
    LPAD((10000 + (i * 23) % 89999)::TEXT, 5, '0') AS postal_code,
    ((i * 7) % 6) + 1 AS region_id,
    -- GPS tightly clustered around metro centers (±3km for customer locations)
    (ARRAY[
        47.6062, 47.6062, 47.6062, 45.5152, 45.5152, 43.6150, 47.2529, 47.6588,
        33.4484, 33.4484, 33.4484, 36.1699, 36.1699, 40.7608, 35.0844, 32.2226,
        32.7767, 32.7767, 32.7767, 29.7604, 29.7604, 29.7604, 30.2672, 35.4676,
        33.7490, 33.7490, 25.7617, 25.7617, 27.9506, 35.2271, 28.5383, 36.1627,
        41.8781, 41.8781, 41.8781, 42.3314, 39.9612, 39.7684, 44.9778, 43.0389,
        40.7484, 40.7484, 40.7484, 39.9526, 42.3601, 38.9072, 38.9072, 39.2904
    ])[((i * 7) % 6) * 8 + ((i * 11) % 8) + 1]
    + (RANDOM() * 0.06 - 0.03) AS latitude,
    (ARRAY[
        -122.3321,-122.3321,-122.3321,-122.6784,-122.6784,-116.2023,-122.4443,-117.4260,
        -112.0740,-112.0740,-112.0740,-115.1398,-115.1398,-111.8910,-106.6504,-110.9747,
         -96.7970, -96.7970, -96.7970, -95.3698, -95.3698, -95.3698, -97.7431, -97.5164,
         -84.3880, -84.3880, -80.1918, -80.1918, -82.4572, -80.8431, -81.3792, -86.7816,
         -87.6298, -87.6298, -87.6298, -83.0458, -82.9988, -86.1581, -93.2650, -87.9065,
         -73.9857, -73.9857, -73.9857, -75.1652, -71.0589, -77.0369, -77.0369, -76.6122
    ])[((i * 7) % 6) * 8 + ((i * 11) % 8) + 1]
    + (RANDOM() * 0.06 - 0.03) AS longitude,
    -- Assign technician: only truly 'open' WOs have no tech assigned
    CASE
        WHEN (i * 19) % 1000 >= 975 AND (i * 19) % 1000 < 983 THEN NULL  -- open WOs only
        ELSE ((((i * 7) % 6) * {techs_per_region}) + 1 + (i % {techs_per_region}))::BIGINT  -- tech from same region
    END AS assigned_technician_id,
    -- Created: historical WOs spread over past year, active WOs within past 3 days (realistic)
    CASE
        WHEN (i * 19) % 1000 < 975 THEN  -- completed/cancelled: past 12 months
            CURRENT_TIMESTAMP - ((i * 3) % 365 || ' days')::INTERVAL - ((i * 7) % 24 || ' hours')::INTERVAL
        ELSE  -- active: past 3 days
            CURRENT_TIMESTAMP - ((i * 3) % 3 || ' days')::INTERVAL - ((i * 7) % 24 || ' hours')::INTERVAL
    END AS created_at,
    CASE
        WHEN (i * 19) % 1000 < 975 THEN
            CURRENT_TIMESTAMP - ((i * 3) % 365 || ' days')::INTERVAL - ((i * 7) % 24 || ' hours')::INTERVAL + ((2 + (i * 11) % 48) || ' hours')::INTERVAL
        ELSE
            CURRENT_TIMESTAMP - ((i * 3) % 3 || ' days')::INTERVAL - ((i * 7) % 24 || ' hours')::INTERVAL + ((2 + (i * 11) % 48) || ' hours')::INTERVAL
    END AS updated_at,
    -- SLA due: base on created_at + SLA window
    CASE
        WHEN (i * 19) % 1000 < 975 THEN
            CURRENT_TIMESTAMP - ((i * 3) % 365 || ' days')::INTERVAL - ((i * 7) % 24 || ' hours')::INTERVAL
        ELSE
            CURRENT_TIMESTAMP - ((i * 3) % 3 || ' days')::INTERVAL - ((i * 7) % 24 || ' hours')::INTERVAL
    END +
        CASE
            WHEN (i * 31) % 100 < 10 THEN '12 hours'::INTERVAL    -- critical
            WHEN (i * 31) % 100 < 30 THEN '24 hours'::INTERVAL    -- high
            WHEN (i * 31) % 100 < 70 THEN '72 hours'::INTERVAL    -- medium
            ELSE '120 hours'::INTERVAL                              -- low
        END AS sla_due_at,
    -- First response: all WOs except truly 'open' and 'on_hold' have a response
    CASE
        WHEN (i * 19) % 1000 < 975 THEN  -- completed/cancelled
            CURRENT_TIMESTAMP - ((i * 3) % 365 || ' days')::INTERVAL + ((1 + (i * 3) % 6) || ' hours')::INTERVAL
        WHEN (i * 19) % 1000 >= 983 AND (i * 19) % 1000 < 998 THEN  -- assigned/en_route/in_progress (active, responded)
            CURRENT_TIMESTAMP - ((i * 3) % 3 || ' days')::INTERVAL + ((1 + (i * 3) % 6) || ' hours')::INTERVAL
        ELSE NULL  -- open/on_hold: no response yet
    END AS first_response_at,
    -- Resolved (completed WOs only)
    CASE
        WHEN (i * 19) % 1000 < 960 THEN
            CURRENT_TIMESTAMP - ((i * 3) % 365 || ' days')::INTERVAL + ((4 + (i * 11) % 48) || ' hours')::INTERVAL
        ELSE NULL
    END AS resolved_at,
    -- Closed (completed WOs only)
    CASE
        WHEN (i * 19) % 1000 < 960 THEN
            CURRENT_TIMESTAMP - ((i * 3) % 365 || ' days')::INTERVAL + ((5 + (i * 11) % 48) || ' hours')::INTERVAL
        ELSE NULL
    END AS closed_at,
    -- SLA met: varies by priority (critical hardest, low easiest)
    -- Uses large-prime double-mod to decorrelate from priority expression (i*31)%100
    CASE
        WHEN (i * 19) % 1000 < 960 THEN
            CASE
                WHEN (i * 31) % 100 < 10 THEN ((i::bigint * 104729 + 37) % 99991) % 100 < 75  -- critical: 75% met
                WHEN (i * 31) % 100 < 30 THEN ((i::bigint * 104729 + 37) % 99991) % 100 < 82  -- high: 82% met
                WHEN (i * 31) % 100 < 70 THEN ((i::bigint * 104729 + 37) % 99991) % 100 < 93  -- medium: 93% met
                ELSE ((i::bigint * 104729 + 37) % 99991) % 100 < 98                             -- low: 98% met
            END
        ELSE NULL
    END AS sla_met
FROM generate_series(1, {work_orders}) i;

-- (Indexes created in bulk after all tables are loaded — see INDEXES section below)
ANALYZE field_service.work_orders;


-- Appointments
DROP TABLE IF EXISTS field_service.appointments CASCADE;

CREATE TABLE field_service.appointments (
    appointment_id    BIGSERIAL PRIMARY KEY,
    work_order_id     BIGINT REFERENCES field_service.work_orders(work_order_id),
    technician_id     BIGINT REFERENCES field_service.technicians(technician_id),
    scheduled_start   TIMESTAMP NOT NULL,
    scheduled_end     TIMESTAMP NOT NULL,
    actual_start      TIMESTAMP,
    actual_end        TIMESTAMP,
    status            VARCHAR(20) DEFAULT 'scheduled' CHECK (status IN ('scheduled', 'confirmed', 'en_route', 'in_progress', 'completed', 'missed', 'rescheduled', 'cancelled')),
    travel_time_min   INTEGER,
    on_site_time_min  INTEGER,
    customer_rating   INTEGER CHECK (customer_rating BETWEEN 1 AND 5),
    customer_feedback TEXT,
    notes             TEXT,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE field_service.appointments IS 'Scheduled and completed technician visits for work orders. One work order may have multiple appointments (e.g., rescheduled or follow-up).';

-- Generate appointments for completed work orders
-- Uses deterministic CTE join instead of correlated subquery for scale
INSERT INTO field_service.appointments (
    work_order_id, technician_id,
    scheduled_start, scheduled_end, actual_start, actual_end,
    status, travel_time_min, on_site_time_min,
    customer_rating, customer_feedback, created_at
)
WITH region_techs AS (
    SELECT technician_id, region_id,
           ROW_NUMBER() OVER (PARTITION BY region_id ORDER BY technician_id) AS rn,
           COUNT(*) OVER (PARTITION BY region_id) AS cnt
    FROM field_service.technicians
)
SELECT
    wo.work_order_id,
    rt.technician_id,
    wo.created_at + ((2 + (wo.work_order_id * 7) % 24) || ' hours')::INTERVAL AS scheduled_start,
    wo.created_at + ((4 + (wo.work_order_id * 7) % 24) || ' hours')::INTERVAL AS scheduled_end,
    CASE WHEN wo.status IN ('completed', 'in_progress') THEN
        wo.created_at + ((2 + (wo.work_order_id * 7) % 24) || ' hours')::INTERVAL + ((wo.work_order_id * 13 % 30) || ' minutes')::INTERVAL
    ELSE NULL END AS actual_start,
    CASE WHEN wo.status = 'completed' THEN
        wo.created_at + ((3 + (wo.work_order_id * 7) % 24) || ' hours')::INTERVAL + ((wo.work_order_id * 17 % 60) || ' minutes')::INTERVAL
    ELSE NULL END AS actual_end,
    CASE
        WHEN wo.status = 'completed' THEN 'completed'
        WHEN wo.status = 'in_progress' THEN 'in_progress'
        WHEN wo.status = 'assigned' THEN 'confirmed'
        WHEN wo.status = 'en_route' THEN 'en_route'
        WHEN wo.status = 'cancelled' THEN 'cancelled'
        ELSE 'scheduled'
    END AS status,
    15 + (wo.work_order_id * 11 % 45) AS travel_time_min,
    30 + (wo.work_order_id * 23 % 120) AS on_site_time_min,
    CASE WHEN wo.status = 'completed' THEN
        3 + (wo.work_order_id * 7 % 3)
    ELSE NULL END AS customer_rating,
    CASE WHEN wo.status = 'completed' AND (wo.work_order_id * 31 % 10) < 3 THEN
        CASE ((wo.work_order_id * 11) % 6)
            WHEN 0 THEN 'Technician was very professional and resolved the issue quickly'
            WHEN 1 THEN 'Great service, explained everything clearly'
            WHEN 2 THEN 'Fixed the problem but took longer than expected'
            WHEN 3 THEN 'Arrived on time and was very helpful'
            WHEN 4 THEN 'Had to come back a second time but eventually fixed it'
            WHEN 5 THEN 'Excellent service from start to finish'
        END
    ELSE NULL END AS customer_feedback,
    wo.created_at AS created_at
FROM field_service.work_orders wo
JOIN region_techs rt ON rt.region_id = wo.region_id
    AND rt.rn = 1 + (wo.work_order_id % rt.cnt)
WHERE wo.status IN ('completed', 'in_progress', 'assigned', 'en_route', 'cancelled')
LIMIT {appointments};


-- Equipment Inventory
DROP TABLE IF EXISTS field_service.equipment_inventory CASCADE;

CREATE TABLE field_service.equipment_inventory (
    inventory_id            BIGSERIAL PRIMARY KEY,
    equipment_type_id       INTEGER REFERENCES field_service.equipment_catalog(equipment_type_id),
    serial_number           VARCHAR(100),
    status                  VARCHAR(20) DEFAULT 'in_stock' CHECK (status IN ('in_stock', 'assigned', 'installed', 'defective', 'returned', 'retired')),
    warehouse_location      VARCHAR(100),
    region_id               INTEGER REFERENCES field_service.service_regions(region_id),
    assigned_technician_id  BIGINT REFERENCES field_service.technicians(technician_id),
    installed_customer_id   BIGINT REFERENCES field_service.customers(customer_id),
    purchased_at            DATE,
    installed_at            TIMESTAMP,
    last_serviced_at        TIMESTAMP,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE field_service.equipment_inventory IS 'Physical equipment inventory tracked by serial number. Items flow from warehouse to technician van to customer premises.';

-- Generate 50,000 inventory items across regions
INSERT INTO field_service.equipment_inventory (
    equipment_type_id, serial_number, status, warehouse_location,
    region_id, assigned_technician_id, installed_customer_id,
    purchased_at, installed_at, last_serviced_at
)
SELECT
    -- Equipment type: weighted toward CPE and consumables
    CASE
        WHEN (i % 100) < 40 THEN 1 + (i % 8)    -- CPE (types 1-8)
        WHEN (i % 100) < 55 THEN 9 + (i % 7)    -- Tools (types 9-15)
        WHEN (i % 100) < 70 THEN 16 + (i % 5)   -- Network components (types 16-20)
        ELSE 21 + (i % 6)                         -- Consumables (types 21-26)
    END AS equipment_type_id,
    CASE
        WHEN (i % 100) < 70 THEN 'SN-' || LPAD(i::TEXT, 8, '0') || '-' || UPPER(SUBSTR(MD5(i::TEXT), 1, 4))
        ELSE NULL  -- Consumables don't have serial numbers
    END AS serial_number,
    CASE
        WHEN (i % 100) < 30 THEN 'in_stock'
        WHEN (i % 100) < 45 THEN 'assigned'
        WHEN (i % 100) < 80 THEN 'installed'
        WHEN (i % 100) < 90 THEN 'defective'
        WHEN (i % 100) < 95 THEN 'returned'
        ELSE 'retired'
    END AS status,
    'Warehouse-' || CASE ((i - 1) % 6)
        WHEN 0 THEN 'PNW' WHEN 1 THEN 'SW' WHEN 2 THEN 'SC'
        WHEN 3 THEN 'SE' WHEN 4 THEN 'MW' WHEN 5 THEN 'NE'
    END AS warehouse_location,
    ((i - 1) % 6) + 1 AS region_id,
    CASE WHEN (i % 100) BETWEEN 30 AND 44 THEN
        (((i - 1) % 6) * {techs_per_region} + 1 + (i % {techs_per_region}))::BIGINT
    ELSE NULL END AS assigned_technician_id,
    CASE WHEN (i % 100) BETWEEN 45 AND 79 THEN
        (1 + (i * 7) % {customers})::BIGINT
    ELSE NULL END AS installed_customer_id,
    CURRENT_DATE - (30 + (i * 13) % 730)::INTEGER AS purchased_at,
    CASE WHEN (i % 100) BETWEEN 45 AND 79 THEN
        CURRENT_TIMESTAMP - ((i * 11) % 365 || ' days')::INTERVAL
    ELSE NULL END AS installed_at,
    CASE WHEN (i % 100) BETWEEN 45 AND 89 THEN
        CURRENT_TIMESTAMP - ((i * 7) % 180 || ' days')::INTERVAL
    ELSE NULL END AS last_serviced_at
FROM generate_series(1, {equipment}) i;


-- Work Order Parts
DROP TABLE IF EXISTS field_service.work_order_parts CASCADE;

CREATE TABLE field_service.work_order_parts (
    id              BIGSERIAL PRIMARY KEY,
    work_order_id   BIGINT REFERENCES field_service.work_orders(work_order_id),
    inventory_id    BIGINT REFERENCES field_service.equipment_inventory(inventory_id),
    quantity        INTEGER DEFAULT 1 CHECK (quantity > 0),
    action          VARCHAR(20) CHECK (action IN ('installed', 'replaced', 'returned', 'consumed')),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE field_service.work_order_parts IS 'Parts and equipment consumed or installed per work order.';

-- Generate parts records for completed install and repair work orders
INSERT INTO field_service.work_order_parts (work_order_id, inventory_id, quantity, action, created_at)
SELECT
    wo.work_order_id,
    (1 + (wo.work_order_id * 7) % {equipment})::BIGINT AS inventory_id,
    CASE WHEN wo.category = 'install' THEN 1 + (wo.work_order_id % 3)
         ELSE 1
    END AS quantity,
    CASE
        WHEN wo.category = 'install' THEN 'installed'
        WHEN wo.category = 'repair' AND RANDOM() < 0.6 THEN 'replaced'
        WHEN wo.category = 'repair' THEN 'consumed'
        ELSE 'consumed'
    END AS action,
    wo.resolved_at AS created_at
FROM field_service.work_orders wo
WHERE wo.status = 'completed'
AND wo.category IN ('install', 'repair', 'upgrade')
AND wo.resolved_at IS NOT NULL
LIMIT {work_order_parts};


-- Work Order Notes (activity log)
DROP TABLE IF EXISTS field_service.work_order_notes CASCADE;

CREATE TABLE field_service.work_order_notes (
    note_id         BIGSERIAL PRIMARY KEY,
    work_order_id   BIGINT REFERENCES field_service.work_orders(work_order_id),
    author          VARCHAR(100),
    note_type       VARCHAR(20) CHECK (note_type IN ('status_change', 'tech_note', 'customer_note', 'system', 'escalation')),
    content         TEXT NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE field_service.work_order_notes IS 'Activity log for work orders. Every status change, technician note, and customer interaction is recorded.';

-- Generate notes for work orders
INSERT INTO field_service.work_order_notes (work_order_id, author, note_type, content, created_at)
SELECT
    wo.work_order_id,
    'System',
    'status_change',
    'Work order created with priority: ' || wo.priority,
    wo.created_at
FROM field_service.work_orders wo
LIMIT {notes_creation};

-- Add tech notes for completed WOs
INSERT INTO field_service.work_order_notes (work_order_id, author, note_type, content, created_at)
SELECT
    wo.work_order_id,
    'Field Technician',
    'tech_note',
    CASE ((wo.work_order_id * 11) % 10)
        WHEN 0 THEN 'Arrived on site, customer showed me the issue. Running diagnostics.'
        WHEN 1 THEN 'Replaced faulty ONT, signal levels now within spec. Testing throughput.'
        WHEN 2 THEN 'Found damaged cable at the demarcation point. Spliced and tested, service restored.'
        WHEN 3 THEN 'Customer not home on first attempt. Left door tag, rescheduling.'
        WHEN 4 THEN 'Completed fiber installation. ONT mounted, router configured, speed test passed.'
        WHEN 5 THEN 'Identified issue with upstream splitter. Replaced and verified all ports.'
        WHEN 6 THEN 'Power cycling resolved the issue temporarily. Monitoring for recurrence.'
        WHEN 7 THEN 'Upgraded firmware on CPE. Customer confirmed improvement in connectivity.'
        WHEN 8 THEN 'Ran new drop cable from pole to house. Old cable showed significant signal loss.'
        WHEN 9 THEN 'Equipment swap completed. Old unit collected for RMA processing.'
    END AS content,
    wo.resolved_at - ('30 minutes'::INTERVAL) AS created_at
FROM field_service.work_orders wo
WHERE wo.status = 'completed' AND wo.resolved_at IS NOT NULL
LIMIT {notes_tech};

-- Add resolution notes
INSERT INTO field_service.work_order_notes (work_order_id, author, note_type, content, created_at)
SELECT
    wo.work_order_id,
    'System',
    'status_change',
    'Work order resolved. SLA ' || CASE WHEN wo.sla_met THEN 'MET' ELSE 'BREACHED' END || '.',
    wo.resolved_at
FROM field_service.work_orders wo
WHERE wo.status = 'completed' AND wo.resolved_at IS NOT NULL
LIMIT {notes_resolution};


-- ============================================================================
-- INDEXES for performance
-- ============================================================================

-- Backfill required_skill_id based on category + subcategory
UPDATE field_service.work_orders wo SET required_skill_id = mapping.skill_id
FROM (SELECT skill_id, skill_name FROM field_service.skill_types) mapping
WHERE mapping.skill_name = CASE wo.category
    WHEN 'install' THEN 'Installation'
    WHEN 'repair' THEN 'Repair'
    WHEN 'maintenance' THEN 'Preventive Maintenance'
    WHEN 'upgrade' THEN 'Upgrade'
    WHEN 'disconnect' THEN 'Decommissioning'
    END
AND wo.required_skill_id IS NULL;

-- ---- Work orders ----
-- Composite indexes (these also serve as leading-column indexes for status, customer_id, etc.)
CREATE INDEX IF NOT EXISTS idx_wo_status_priority ON field_service.work_orders(status, priority);
CREATE INDEX IF NOT EXISTS idx_wo_status_region ON field_service.work_orders(status, region_id);
CREATE INDEX IF NOT EXISTS idx_wo_status_created ON field_service.work_orders(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_wo_customer_status ON field_service.work_orders(customer_id, status);
CREATE INDEX IF NOT EXISTS idx_wo_region_status ON field_service.work_orders(region_id, status);
CREATE INDEX IF NOT EXISTS idx_wo_required_skill ON field_service.work_orders(required_skill_id);
CREATE INDEX IF NOT EXISTS idx_wo_created ON field_service.work_orders(created_at);
CREATE INDEX IF NOT EXISTS idx_wo_sla_due ON field_service.work_orders(sla_due_at);
-- Partial indexes: active work orders only (~2.5% of table, most queried by app)
CREATE INDEX IF NOT EXISTS idx_wo_active ON field_service.work_orders(status, priority, sla_due_at) WHERE status NOT IN ('completed', 'cancelled');
CREATE INDEX IF NOT EXISTS idx_wo_active_region ON field_service.work_orders(region_id, priority, sla_due_at) WHERE status NOT IN ('completed', 'cancelled');
CREATE INDEX IF NOT EXISTS idx_wo_tech_active ON field_service.work_orders(assigned_technician_id) WHERE status NOT IN ('completed', 'cancelled');
CREATE INDEX IF NOT EXISTS idx_wo_completed_sla ON field_service.work_orders(region_id, priority, category, sla_met) WHERE status = 'completed';
CREATE INDEX IF NOT EXISTS idx_wo_dispatch_order ON field_service.work_orders (
    (CASE priority WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END),
    sla_due_at NULLS LAST
) WHERE status NOT IN ('completed', 'cancelled');
CREATE INDEX IF NOT EXISTS idx_wo_completed_resolved ON field_service.work_orders(resolved_at DESC) WHERE status = 'completed';
CREATE INDEX IF NOT EXISTS idx_wo_sla_active ON field_service.work_orders(sla_due_at ASC) WHERE status NOT IN ('completed', 'cancelled') AND sla_due_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_wo_status_category ON field_service.work_orders(status, category);
CREATE INDEX IF NOT EXISTS idx_wo_tech_completed ON field_service.work_orders(assigned_technician_id, resolved_at DESC) WHERE status = 'completed';

-- ---- Appointments ----
CREATE INDEX IF NOT EXISTS idx_appt_wo ON field_service.appointments(work_order_id);
CREATE INDEX IF NOT EXISTS idx_appt_tech ON field_service.appointments(technician_id);
CREATE INDEX IF NOT EXISTS idx_appt_status ON field_service.appointments(status);
CREATE INDEX IF NOT EXISTS idx_appt_scheduled ON field_service.appointments(scheduled_start);

-- ---- Technicians ----
CREATE INDEX IF NOT EXISTS idx_tech_region ON field_service.technicians(region_id);
CREATE INDEX IF NOT EXISTS idx_tech_status ON field_service.technicians(status);

-- ---- Metro Territories ----
CREATE INDEX IF NOT EXISTS idx_metro_region ON field_service.metro_territories(region_id);

-- ---- Customers ----
CREATE INDEX IF NOT EXISTS idx_cust_region ON field_service.customers(region_id);
CREATE INDEX IF NOT EXISTS idx_cust_tier ON field_service.customers(customer_tier);
CREATE INDEX IF NOT EXISTS idx_cust_status ON field_service.customers(account_status);

-- ---- Equipment ----
CREATE INDEX IF NOT EXISTS idx_equip_region_status ON field_service.equipment_inventory(region_id, status);
CREATE INDEX IF NOT EXISTS idx_equip_type ON field_service.equipment_inventory(equipment_type_id);
CREATE INDEX IF NOT EXISTS idx_equip_customer ON field_service.equipment_inventory(installed_customer_id);
CREATE INDEX IF NOT EXISTS idx_equip_tech ON field_service.equipment_inventory(assigned_technician_id);

-- ---- Notes ----
CREATE INDEX IF NOT EXISTS idx_notes_wo_created ON field_service.work_order_notes(work_order_id, created_at DESC);

-- ---- Parts ----
CREATE INDEX IF NOT EXISTS idx_parts_wo ON field_service.work_order_parts(work_order_id);
CREATE INDEX IF NOT EXISTS idx_parts_inventory ON field_service.work_order_parts(inventory_id);

-- Run ANALYZE to update planner statistics after bulk load
ANALYZE field_service.work_orders;
ANALYZE field_service.appointments;
ANALYZE field_service.customers;
ANALYZE field_service.equipment_inventory;
ANALYZE field_service.work_order_notes;
ANALYZE field_service.work_order_parts;
ANALYZE field_service.technicians;


-- ============================================================================
-- SUMMARY
-- ============================================================================
-- Row counts are controlled by {placeholder} tokens substituted at deploy time.
-- "demo" preset:  ~671K rows (50K customers, 100K WOs)
-- "scale" preset: ~33M+ rows (2M customers, 5M WOs)
-- ============================================================================
