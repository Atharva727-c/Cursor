import os
from typing import Optional, Dict, Any

import snowflake.connector
from dotenv import load_dotenv


def load_environment(dotenv_path: Optional[str] = None) -> None:
	"""Load environment variables from a .env file if present."""
	load_dotenv(dotenv_path=dotenv_path)


def get_snowflake_connection_params() -> Dict[str, Any]:
	"""Collect Snowflake connection parameters from environment variables."""
	account = os.getenv("SNOWFLAKE_ACCOUNT")
	user = os.getenv("SNOWFLAKE_USER")
	password = os.getenv("SNOWFLAKE_PASSWORD")
	role = os.getenv("SNOWFLAKE_ROLE")
	warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
	database = os.getenv("SNOWFLAKE_DATABASE")
	schema = os.getenv("SNOWFLAKE_SCHEMA")

	missing = [
		name for name, value in [
			("SNOWFLAKE_ACCOUNT", account),
			("SNOWFLAKE_USER", user),
			("SNOWFLAKE_PASSWORD", password),
			("SNOWFLAKE_ROLE", role),
			("SNOWFLAKE_WAREHOUSE", warehouse),
			("SNOWFLAKE_DATABASE", database),
			("SNOWFLAKE_SCHEMA", schema),
		] if not value
	]
	if missing:
		raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

	params: Dict[str, Any] = {
		"account": account,
		"user": user,
		"password": password,
		"role": role,
		"warehouse": warehouse,
		"database": database,
		"schema": schema,
	}
	return params


def connect_snowflake(dotenv_path: Optional[str] = None) -> snowflake.connector.SnowflakeConnection:
	"""Create and return a Snowflake connection using env vars."""
	load_environment(dotenv_path=dotenv_path)
	params = get_snowflake_connection_params()
	return snowflake.connector.connect(**params)


def test_connection(dotenv_path: Optional[str] = None) -> str:
	"""Run a simple query to verify the connection and return the Snowflake version."""
	with connect_snowflake(dotenv_path=dotenv_path) as conn:
		with conn.cursor() as cur:
			cur.execute("SELECT CURRENT_VERSION()")
			row = cur.fetchone()
			return str(row[0]) if row else "Unknown"


if __name__ == "__main__":
	# Optional: pass a custom dotenv path, otherwise it will use default .env lookup
	try:
		version = test_connection()
		print(f"Connected to Snowflake. Current version: {version}")
	except Exception as exc:
		print(f"Snowflake connection failed: {exc}")

