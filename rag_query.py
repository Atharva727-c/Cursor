import sys
from typing import List, Tuple

from snowflake_connect import connect_snowflake


def retrieve_context(question: str, k: int = 5) -> List[Tuple[str, str, int, float]]:
	with connect_snowflake() as conn, conn.cursor() as cur:
		# Compute embedding inside SQL to preserve VECTOR type
		query = (
			"WITH q AS ("
			" SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', %s) AS qvec"
			") "
			"SELECT d.DOC_ID, d.FILENAME, d.CHUNK_INDEX, "
			" VECTOR_COSINE_SIMILARITY(d.EMBEDDING, q.qvec) AS SIM, "
			" d.CONTENT "
			"FROM PDF_DOC_CHUNKS d, q "
			"ORDER BY SIM DESC "
			"LIMIT %s"
		)
		cur.execute(query, (question, k))
		rows = cur.fetchall()
		# Return (content, filename, chunk_index, similarity)
		return [(r[4], r[1], r[2], r[3]) for r in rows]


def build_prompt(question: str, contexts: List[Tuple[str, str, int, float]]) -> str:
	context_block = "\n\n".join(
		f"[{i+1}] (file: {fn}, chunk: {ci}, score: {sim:.4f})\n{ctx}"
		for i, (ctx, fn, ci, sim) in enumerate(contexts)
	)
	return (
		"Use the context chunks below to answer the question. "
		"If the answer is not in the context, say you don't know.\n\n"
		f"Context:\n{context_block}\n\n"
		f"Question: {question}\n\n"
		"Answer:"
	)


def ask(question: str) -> None:
	contexts = retrieve_context(question, k=5)
	prompt = build_prompt(question, contexts)

	with connect_snowflake() as conn, conn.cursor() as cur:
		# Try multiple models in order of preference
		models = ['llama3-8b', 'mistral-7b', 'mixtral-8x7b', 'snowflake-arctic']
		answer = None
		last_error = None
		for model in models:
			try:
				cur.execute(
					f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', %s)",
					(prompt,),
				)
				(answer,) = cur.fetchone()
				break
			except Exception as e:
				last_error = str(e)
				continue
		if answer is None:
			raise Exception(f"All models failed. Last error: {last_error}")

	print(answer)
	print("\n---\nSources:")
	for i, (_ctx, fn, ci, sim) in enumerate(contexts, 1):
		print(f"[{i}] file={fn}, chunk={ci}, score={sim:.4f}")


if __name__ == "__main__":
	q = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "What are the key points?"
	ask(q)

