"""Device-group provisioning.

  python -m app.manage create-group --name fleet-a
  python -m app.manage list-groups

create-group generates a random API key and secret, stores them, and
prints them ONCE — the secret is never retrievable afterwards through
any API. list-groups shows keys and names, never secrets.
"""

import argparse
import secrets

from sqlalchemy import text

from app.database import SessionLocal


def create_group(name: str) -> None:
    api_key = "vg-" + secrets.token_hex(16)
    secret = secrets.token_hex(32)
    with SessionLocal() as session:
        with session.begin():
            session.execute(
                text("INSERT INTO device_groups (api_key, secret, name) "
                     "VALUES (:key, :secret, :name)"),
                {"key": api_key, "secret": secret, "name": name},
            )
    print(f"api_key: {api_key}")
    print(f"secret:  {secret}")
    print("Store the secret now; it cannot be retrieved later.")


def list_groups() -> None:
    with SessionLocal() as session:
        rows = session.execute(
            text("SELECT api_key, name, created_at FROM device_groups ORDER BY id")
        ).all()
    for row in rows:
        print(f"{row.api_key}  {row.name}  {row.created_at.isoformat()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("create-group")
    create.add_argument("--name", required=True)
    sub.add_parser("list-groups")
    args = parser.parse_args()
    if args.command == "create-group":
        create_group(args.name)
    else:
        list_groups()