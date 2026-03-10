# db/seed.py

import csv
import os
import random
import uuid
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]

FIPS_TO_STATE = {
    '01': 'Alabama', '02': 'Alaska', '04': 'Arizona', '05': 'Arkansas',
    '06': 'California', '08': 'Colorado', '09': 'Connecticut', '10': 'Delaware',
    '11': 'Washington DC', '12': 'Florida', '13': 'Georgia', '15': 'Hawaii',
    '16': 'Idaho', '17': 'Illinois', '18': 'Indiana', '19': 'Iowa',
    '20': 'Kansas', '21': 'Kentucky', '22': 'Louisiana', '23': 'Maine',
    '24': 'Maryland', '25': 'Massachusetts', '26': 'Michigan', '27': 'Minnesota',
    '28': 'Mississippi', '29': 'Missouri', '30': 'Montana', '31': 'Nebraska',
    '32': 'Nevada', '33': 'New Hampshire', '34': 'New Jersey', '35': 'New Mexico',
    '36': 'New York', '37': 'North Carolina', '38': 'North Dakota', '39': 'Ohio',
    '40': 'Oklahoma', '41': 'Oregon', '42': 'Pennsylvania', '44': 'Rhode Island',
    '45': 'South Carolina', '46': 'South Dakota', '47': 'Tennessee', '48': 'Texas',
    '49': 'Utah', '50': 'Vermont', '51': 'Virginia', '53': 'Washington',
    '54': 'West Virginia', '55': 'Wisconsin', '56': 'Wyoming'
}

MODE_MAP = {
    '1': 'truck', '2': 'rail', '3': 'water',
    '4': 'air', '5': 'multimodal', '6': 'pipeline', '7': 'other'
}

COMMODITY_MAP = {
    '01': 'Live animals/fish', '02': 'Cereal grains', '03': 'Other ag prods.',
    '04': 'Animal feed', '05': 'Meat/seafood', '06': 'Milled grain prods.',
    '07': 'Other foodstuffs', '08': 'Alcoholic beverages', '09': 'Tobacco prods.',
    '10': 'Building stone', '11': 'Natural sands', '12': 'Gravel',
    '13': 'Nonmetallic minerals', '14': 'Metallic ores', '15': 'Coal',
    '16': 'Crude petroleum', '17': 'Gasoline', '18': 'Fuel oils',
    '19': 'Natural gas', '20': 'Basic chemicals', '21': 'Pharmaceuticals',
    '22': 'Fertilizers', '23': 'Chemical prods.', '24': 'Plastics/rubber',
    '25': 'Logs', '26': 'Wood prods.', '27': 'Newsprint/paper',
    '28': 'Paper articles', '29': 'Printed prods.', '30': 'Textiles/leather',
    '31': 'Nonmetal min. prods.', '32': 'Base metals', '33': 'Articles-base metal',
    '34': 'Machinery', '35': 'Electronics', '36': 'Motorized vehicles',
    '37': 'Transport equip.', '38': 'Precision instruments', '39': 'Furniture',
    '40': 'Misc. mfg. prods.', '41': 'Waste/scrap', '43': 'Mixed freight'
}

US_CITIES = [
    'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia',
    'San Antonio', 'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville',
    'Fort Worth', 'Columbus', 'Indianapolis', 'Charlotte', 'San Francisco',
    'Seattle', 'Denver', 'Nashville', 'Oklahoma City', 'El Paso', 'Washington',
    'Las Vegas', 'Louisville', 'Memphis', 'Portland', 'Baltimore', 'Milwaukee',
    'Albuquerque', 'Tucson', 'Fresno', 'Sacramento', 'Kansas City', 'Mesa',
    'Atlanta', 'Omaha', 'Colorado Springs', 'Raleigh', 'Miami', 'Minneapolis',
    'Cleveland', 'Wichita', 'Arlington', 'New Orleans', 'Bakersfield', 'Tampa',
    'Honolulu', 'Aurora', 'Anaheim', 'Santa Ana', 'Corpus Christi', 'Riverside',
    'St. Louis', 'Lexington', 'Pittsburgh', 'Anchorage', 'Stockton', 'Cincinnati',
]

INDUSTRIES = ['Logistics', 'Manufacturing', 'Distribution', 'Trading', 'Supply Co', 'Imports', 'Exports', 'Industrial']
SUFFIXES = ['LLC', 'Inc', 'Corp', 'Group', 'Partners', 'Co']


def load_carriers():
    carriers = {}
    carrier_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', 'carrier_allwithhistory.txt')
    with open(carrier_path, encoding='latin-1') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 31:
                continue
            dot = row[1].strip()
            authority = row[4].strip()
            name = row[25].strip()
            city = row[29].strip()
            state = row[30].strip()
            if authority != 'A' or not name or not dot:
                continue
            if dot not in carriers:
                carriers[dot] = {'dot': dot, 'name': name, 'city': city, 'state': state}
    all_carriers = list(carriers.values())
    random.seed(42)
    if len(all_carriers) > 2000:
        all_carriers = random.sample(all_carriers, 2000)
    return all_carriers


def load_faf5():
    faf_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', 'FAF5.7.1_State_2018-2024.csv')
    df = pd.read_csv(faf_path, dtype={'dms_origst': str, 'dms_destst': str, 'dms_mode': str, 'sctg2': str})

    # Zero-pad FIPS codes to 2 chars
    df['dms_origst'] = df['dms_origst'].str.zfill(2)
    df['dms_destst'] = df['dms_destst'].str.zfill(2)
    df['dms_mode'] = df['dms_mode'].str.strip()
    df['sctg2'] = df['sctg2'].str.zfill(2)

    df = df[df['value_2024'] > 0]
    df = df[df['tons_2024'] > 0]

    df['origin_state'] = df['dms_origst'].map(FIPS_TO_STATE.get)
    df['destination_state'] = df['dms_destst'].map(FIPS_TO_STATE.get)
    df['mode'] = df['dms_mode'].map(MODE_MAP.get)
    df['commodity'] = df['sctg2'].map(COMMODITY_MAP.get)

    df = df.dropna(subset=['origin_state', 'destination_state', 'mode', 'commodity'])
    df = df[df['origin_state'] != df['destination_state']]

    df = df[['origin_state', 'destination_state', 'mode', 'commodity', 'value_2024', 'tons_2024']].copy()
    df = df.reset_index(drop=True)
    return df


def generate_customers(conn):
    customers = []
    random.seed(42)
    tiers = ['small'] * 60 + ['medium'] * 30 + ['large'] * 10  # 100-item pool
    state_list = list(FIPS_TO_STATE.values())
    for i in range(500):
        tier = tiers[i % 100]
        city = random.choice(US_CITIES)
        industry = random.choice(INDUSTRIES)
        suffix = random.choice(SUFFIXES)
        company_name = f"{city} {industry} {suffix}"
        state = random.choice(state_list)
        customers.append({
            'id': f"CUST-{i+1:04d}",
            'company_name': company_name,
            'tier': tier,
            'industry': industry,
            'state': state,
        })
    return customers


def eta_hours_for_mode(mode):
    if mode == 'truck':
        return random.uniform(4, 72)
    elif mode == 'rail':
        return random.uniform(24, 168)
    elif mode == 'air':
        return random.uniform(2, 24)
    elif mode == 'water':
        return random.uniform(72, 720)
    elif mode == 'multimodal':
        return random.uniform(24, 240)
    else:  # pipeline, other
        return random.uniform(1, 48)


def generate_shipments_and_events(customers, carriers, faf_lanes):
    now = datetime.now(timezone.utc)
    shipments = []
    events = []
    tier_counts = {'small': (1, 5), 'medium': (10, 25), 'large': (50, 100)}

    faf_weights = faf_lanes['value_2024'].values
    faf_total = faf_weights.sum()
    faf_probs = faf_weights / faf_total

    random.seed(42)
    np.random.seed(42)

    shipment_idx = 0
    status_counts = {'normal': 0, 'delayed': 0, 'silent': 0, 'unresponsive': 0, 'delivered': 0}
    mode_counts = {}

    for customer in customers:
        tier = customer['tier']
        min_s, max_s = tier_counts[tier]
        n_shipments = random.randint(min_s, max_s)

        for _ in range(n_shipments):
            lane_idx = np.random.choice(len(faf_lanes), p=faf_probs)
            lane = faf_lanes.iloc[lane_idx]

            carrier = random.choice(carriers)
            freight_value_usd = (lane['value_2024'] * 1_000_000 / 365) * random.uniform(0.001, 0.01)
            weight_tons = (lane['tons_2024'] * 1000 / 365) * random.uniform(0.001, 0.01)
            mode = lane['mode']
            base_eta = eta_hours_for_mode(mode)

            roll = random.random()
            shipment_id = f"SH-{shipment_idx+1:05d}"

            if roll < 0.85:
                # Normal
                status_choice = random.choice(['in_transit', 'picked_up', 'out_for_delivery', 'delivered'])
                carrier_status = 'active'
                created_at = now - timedelta(hours=random.uniform(2, 12))
                last_update = now - timedelta(hours=random.uniform(0, 2))
                hours_since_created = (now - created_at).total_seconds() / 3600
                eta_hours = hours_since_created + random.uniform(4, 24)
                status = status_choice
                if status == 'delivered':
                    status_counts['delivered'] += 1
                else:
                    status_counts['normal'] += 1

                if status == 'delivered':
                    seq = ['shipment_created', 'picked_up', 'in_transit', 'out_for_delivery']
                else:
                    seq_map = {
                        'in_transit': ['shipment_created', 'picked_up', 'in_transit'],
                        'picked_up': ['shipment_created', 'picked_up'],
                        'out_for_delivery': ['shipment_created', 'picked_up', 'in_transit', 'out_for_delivery'],
                    }
                    seq = seq_map.get(status, ['shipment_created', 'picked_up', 'in_transit', 'out_for_delivery'])

            elif roll < 0.92:
                # Delayed
                status = 'delayed'
                carrier_status = 'active'
                created_at = now - timedelta(hours=20)
                last_update = now - timedelta(hours=1)
                eta_hours = 14
                status_counts['delayed'] += 1
                seq = ['shipment_created', 'picked_up', 'in_transit', 'delay_reported']

            elif roll < 0.97:
                # Silent
                status = 'in_transit'
                carrier_status = 'active'
                created_at = now - timedelta(hours=random.uniform(12, 24))
                last_update = now - timedelta(hours=random.uniform(7, 12))
                hours_since_created = (now - created_at).total_seconds() / 3600
                eta_hours = hours_since_created + random.uniform(4, 24)
                status_counts['silent'] += 1
                seq = ['shipment_created', 'picked_up', 'in_transit', 'carrier_silent']

            else:
                # Unresponsive
                status = 'in_transit'
                carrier_status = 'unresponsive'
                created_at = now - timedelta(hours=random.uniform(4, 12))
                last_update = now - timedelta(hours=random.uniform(2, 5))
                hours_since_created = (now - created_at).total_seconds() / 3600
                eta_hours = hours_since_created + random.uniform(4, 24)
                status_counts['unresponsive'] += 1
                seq = ['shipment_created', 'picked_up', 'in_transit']

            mode_counts[mode] = mode_counts.get(mode, 0) + 1

            shipments.append({
                'id': shipment_id,
                'carrier_dot': carrier['dot'],
                'customer_id': customer['id'],
                'origin_state': lane['origin_state'],
                'destination_state': lane['destination_state'],
                'mode': mode,
                'commodity': lane['commodity'],
                'status': status,
                'carrier_status': carrier_status,
                'freight_value_usd': float(freight_value_usd),
                'weight_tons': float(weight_tons),
                'eta_hours': float(eta_hours),
                'created_at': created_at,
                'last_update': last_update,
            })

            # Generate 2-4 events from the sequence
            n_events = min(random.randint(2, 4), len(seq))
            event_seq = seq[:n_events]
            event_time = created_at
            for etype in event_seq:
                event_time = event_time + timedelta(minutes=random.uniform(10, 60))
                events.append({
                    'shipment_id': shipment_id,
                    'event_type': etype,
                    'mode': mode,
                    'location': None,
                    'note': None,
                    'timestamp': event_time,
                    'stream_id': None,
                })

            shipment_idx += 1

    return shipments, events, status_counts, mode_counts


def run_schema(conn):
    schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
    with open(schema_path) as f:
        schema_sql = f.read()
    with conn.cursor() as cur:
        cur.execute(schema_sql)
    conn.commit()


def main():
    conn = psycopg2.connect(DATABASE_URL)

    run_schema(conn)

    # Truncate
    with conn.cursor() as cur:
        cur.execute("TRUNCATE carriers, customers, shipments, events, pipeline_metrics CASCADE")
    conn.commit()

    # Step 1: Load carriers
    carriers = load_carriers()
    print(f"Loaded {len(carriers):,} FMCSA carriers")

    with conn.cursor() as cur:
        for c in carriers:
            cur.execute(
                "INSERT INTO carriers (dot, name, city, state, status) VALUES (%s, %s, %s, %s, %s)",
                (c['dot'], c['name'], c['city'], c['state'], 'active')
            )
    conn.commit()

    # Step 2: Load FAF5
    faf_lanes = load_faf5()
    print(f"Loaded {len(faf_lanes):,} FAF5 freight lanes across all modes")

    # Step 3: Generate customers
    customers = generate_customers(conn)
    with conn.cursor() as cur:
        for c in customers:
            cur.execute(
                "INSERT INTO customers (id, company_name, tier, industry, state) VALUES (%s, %s, %s, %s, %s)",
                (c['id'], c['company_name'], c['tier'], c['industry'], c['state'])
            )
    conn.commit()
    print(f"Generated {len(customers)} customers")

    # Step 4 & 5: Generate shipments + events
    shipments, events, status_counts, mode_counts = generate_shipments_and_events(customers, carriers, faf_lanes)

    with conn.cursor() as cur:
        for s in shipments:
            cur.execute(
                """INSERT INTO shipments
                   (id, carrier_dot, customer_id, origin_state, destination_state,
                    mode, commodity, status, carrier_status,
                    freight_value_usd, weight_tons, eta_hours,
                    created_at, last_update)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (s['id'], s['carrier_dot'], s['customer_id'], s['origin_state'], s['destination_state'],
                 s['mode'], s['commodity'], s['status'], s['carrier_status'],
                 s['freight_value_usd'], s['weight_tons'], s['eta_hours'],
                 s['created_at'], s['last_update'])
            )
    conn.commit()

    with conn.cursor() as cur:
        for e in events:
            cur.execute(
                """INSERT INTO events (shipment_id, event_type, mode, location, note, timestamp, stream_id)
                   VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                (e['shipment_id'], e['event_type'], e['mode'], e['location'], e['note'], e['timestamp'], e['stream_id'])
            )
    conn.commit()

    total = len(shipments)
    n_normal = status_counts['normal']
    n_delayed = status_counts['delayed']
    n_silent = status_counts['silent']
    n_unresponsive = status_counts['unresponsive']
    n_delivered = status_counts['delivered']

    print(f"Seeded {total:,} shipments:")
    print(f"  - {n_normal:,} normal       ({n_normal/total*100:.1f}%)")
    print(f"  - {n_delayed:,} delayed      ({n_delayed/total*100:.1f}%)")
    print(f"  - {n_silent:,} silent        ({n_silent/total*100:.1f}%)")
    print(f"  - {n_unresponsive:,} unresponsive ({n_unresponsive/total*100:.1f}%)")
    print(f"  - {n_delivered:,} delivered    ({n_delivered/total*100:.1f}%)")
    print("Mode breakdown:")
    for m, cnt in sorted(mode_counts.items()):
        print(f"  - {m}: {cnt:,}")
    print(f"Total events: {len(events):,}")

    conn.close()


if __name__ == '__main__':
    main()
