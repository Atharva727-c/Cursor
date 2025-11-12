import sys
from typing import List

from snowflake_connect import connect_snowflake


def read_sql_file(path: str) -> str:
	with open(path, "r", encoding="utf-8") as f:
		return f.read()


def split_statements(sql_text: str) -> List[str]:
	# naive split by semicolon; sufficient for simple statements in our file
	parts = [stmt.strip() for stmt in sql_text.split(";")]
	# Filter out empty strings and statements that are only comments/whitespace
	filtered = []
	for p in parts:
		# Remove comments and check if there's actual SQL content
		lines = [line.strip() for line in p.split("\n") if line.strip() and not line.strip().startswith("--")]
		if lines:
			filtered.append(p)
	return filtered


def main() -> None:
	sql_path = "sql/select_all_products.sql" if len(sys.argv) < 2 else sys.argv[1]
	sql_text = read_sql_file(sql_path)
	statements = split_statements(sql_text)

	with connect_snowflake() as conn:
		with conn.cursor() as cur:
			last_result = None
			for stmt in statements:
				cur.execute(stmt)
				last_result = cur

			if last_result is not None:
				cols = [c[0] for c in last_result.description] if last_result.description else []
				rows = last_result.fetchall() if last_result.description else []

				if cols:
					print(",".join(cols))
					for r in rows:
						print(",".join("" if v is None else str(v) for v in r))
				else:
					print("Statement executed successfully.")


if __name__ == "__main__":
	main()

