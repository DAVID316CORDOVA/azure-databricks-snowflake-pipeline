"""
Dirty data generator for the Azure + Databricks + Snowflake practice
project -- Fintech domain.

Generates synthetic customer/account records with DELIBERATE, controlled
data quality issues, so downstream tests (dbt-expectations, Great
Expectations) can be demonstrated catching real problems, instead of
depending on an external source happening to have them.

Business framing: a fintech platform's customer/account data --
KYC status, account balances, risk scoring. Chosen over gaming/lottery
(the theme of the companion AWS project) specifically because fintech
justifies richer, more realistic data quality rules: KYC completeness,
balance sanity checks, and risk score ranges, on top of the general
issues (nulls, duplicates, malformed emails, schema drift) already
covered in the AWS project.

Usage:
    python generate_dirty_data.py --rows 5000 --seed 42 --output data/raw_customers.json
"""

import argparse
import json
import random
import string
from datetime import datetime, timedelta

COUNTRIES = ["Peru", "Chile", "Colombia", "Mexico", "Argentina", "Brazil", "Spain"]
DEVICES = ["android", "ios", "web", "unknown"]
LANGUAGES = ["es", "en", "pt"]
ACCOUNT_TYPES = ["checking", "savings", "credit"]
KYC_STATUSES = ["pending", "verified", "rejected"]


def random_email(valid: bool = True) -> str:
    local = "".join(random.choices(string.ascii_lowercase, k=8))
    domain = random.choice(["gmail.com", "hotmail.com", "yahoo.com"])
    if valid:
        return f"{local}@{domain}"
    variant = random.choice(["no_at", "no_domain", "spaces", "double_at"])
    if variant == "no_at":
        return f"{local}{domain}"
    if variant == "no_domain":
        return f"{local}@"
    if variant == "spaces":
        return f" {local}@{domain} "
    return f"{local}@@{domain}"


def random_metadata() -> dict:
    """Nested JSON payload -- practice field for VARIANT/FLATTEN."""
    return {
        "device": random.choice(DEVICES),
        "preferences": {
            "notifications": random.choice([True, False]),
            "language": random.choice(LANGUAGES),
        },
        "last_login_ip": f"{random.randint(1,255)}.{random.randint(0,255)}."
        f"{random.randint(0,255)}.{random.randint(0,255)}",
        "risk_score": round(random.uniform(0, 100), 2),
    }


def random_registration_date() -> str:
    start = datetime(2022, 1, 1)
    days_offset = random.randint(0, 1200)
    return (start + timedelta(days=days_offset)).isoformat()


def make_clean_record(customer_id: int) -> dict:
    return {
        "customer_id": customer_id,
        "customer_name": f"Customer_{customer_id}",
        "age": random.randint(18, 75),
        "country": random.choice(COUNTRIES),
        "email": random_email(valid=True),
        "registration_date": random_registration_date(),
        "account_type": random.choice(ACCOUNT_TYPES),
        "account_balance": round(random.uniform(0, 50000), 2),
        "kyc_status": random.choices(KYC_STATUSES, weights=[0.1, 0.85, 0.05])[0],
        "metadata": random_metadata(),
    }


def inject_null_fields(record: dict) -> dict:
    nullable_fields = ["customer_name", "age", "country", "email"]
    field = random.choice(nullable_fields)
    record[field] = None
    return record


def inject_invalid_age(record: dict) -> dict:
    record["age"] = random.choice([-5, 0, 150, 999, 15])
    return record


def inject_malformed_email(record: dict) -> dict:
    record["email"] = random_email(valid=False)
    return record


def inject_schema_drift(record: dict) -> dict:
    variant = random.choice(["rename_field", "extra_field", "type_change"])
    if variant == "rename_field":
        record["customerName"] = record.pop("customer_name", None)
    elif variant == "extra_field":
        record["experimental_flag"] = True
    elif variant == "type_change":
        record["account_balance"] = str(record.get("account_balance", ""))
    return record


def inject_invalid_balance(record: dict) -> dict:
    record["account_balance"] = random.choice([-500.00, -99999.99, 999999999.99])
    return record


def inject_kyc_violation(record: dict) -> dict:
    record["kyc_status"] = random.choice(["pending", "rejected"])
    record["account_balance"] = round(random.uniform(100, 20000), 2)
    return record


def inject_invalid_risk_score(record: dict) -> dict:
    record["metadata"]["risk_score"] = random.choice([-10.0, 150.0, 999.0])
    return record


def generate_dataset(
    n_rows: int,
    null_rate: float = 0.05,
    duplicate_rate: float = 0.03,
    invalid_age_rate: float = 0.03,
    malformed_email_rate: float = 0.03,
    schema_drift_rate: float = 0.02,
    invalid_balance_rate: float = 0.03,
    kyc_violation_rate: float = 0.04,
    invalid_risk_score_rate: float = 0.03,
) -> list:
    records = []
    for i in range(1, n_rows + 1):
        record = make_clean_record(i)

        if random.random() < null_rate:
            record = inject_null_fields(record)
        if random.random() < invalid_age_rate:
            record = inject_invalid_age(record)
        if random.random() < malformed_email_rate:
            record = inject_malformed_email(record)
        if random.random() < schema_drift_rate:
            record = inject_schema_drift(record)
        if random.random() < invalid_balance_rate:
            record = inject_invalid_balance(record)
        if random.random() < kyc_violation_rate:
            record = inject_kyc_violation(record)
        if random.random() < invalid_risk_score_rate:
            record = inject_invalid_risk_score(record)

        records.append(record)

        if random.random() < duplicate_rate:
            records.append(dict(record))

    random.shuffle(records)
    return records


def main():
    parser = argparse.ArgumentParser(description="Generate dirty fintech customer data")
    parser.add_argument("--rows", type=int, default=5000, help="Base number of clean rows before duplicates")
    parser.add_argument("--output", type=str, default="data/raw_customers.json", help="Output file path")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    dataset = generate_dataset(args.rows)

    with open(args.output, "w", encoding="utf-8") as f:
        for record in dataset:
            f.write(json.dumps(record) + "\n")

    print(f"Generated {len(dataset)} records (base rows requested: {args.rows}) -> {args.output}")
    print(
        "Injected issues: nulls, duplicates, invalid ages, malformed emails, "
        "schema drift, invalid balances, KYC violations, invalid risk scores"
    )


if __name__ == "__main__":
    main()