import os
import uuid
from typing import List, Tuple

from pypdf import PdfReader

from snowflake_connect import connect_snowflake


def read_pdf_text(file_path: str) -> str:
	with open(file_path, "rb") as f:
		reader = PdfReader(f)
		text_parts: List[str] = []
		for page in reader.pages:
			try:
				text_parts.append(page.extract_text() or "")
			except Exception:
				text_parts.append("")
		return "\n".join(text_parts)


def chunk_text(text: str, max_len: int = 1200, overlap: int = 150) -> List[str]:
	text = text.replace("\x00", " ").strip()
	if not text:
		return []

	parts: List[str] = []
	start = 0
	while start < len(text):
		end = min(len(text), start + max_len)
		chunk = text[start:end].strip()
		if chunk:
			parts.append(chunk)
		if end == len(text):
			break
		start = end - overlap
		if start < 0:
			start = 0
	return parts


def insert_chunk(conn, doc_id: str, filename: str, idx: int, content: str) -> None:
	with conn.cursor() as cur:
		# Insert row without embedding first
		cur.execute(
			"""
			INSERT INTO PDF_DOC_CHUNKS (DOC_ID, FILENAME, CHUNK_INDEX, CONTENT)
			VALUES (%s, %s, %s, %s)
			""",
			(doc_id, filename, idx, content),
		)
		# Compute embedding in Snowflake and update row
		cur.execute(
			"""
			UPDATE PDF_DOC_CHUNKS
			SET EMBEDDING = SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', CONTENT)
			WHERE DOC_ID = %s AND CHUNK_INDEX = %s
			""",
			(doc_id, idx),
		)


def ingest_folder(folder: str) -> Tuple[int, int]:
	pdf_files = [f for f in os.listdir(folder) if f.lower().endswith(".pdf")]
	total_chunks = 0
	total_files = 0

	with connect_snowflake() as conn:
		for pdf in pdf_files:
			file_path = os.path.join(folder, pdf)
			text = read_pdf_text(file_path)
			chunks = chunk_text(text)
			if not chunks:
				continue

			doc_id = str(uuid.uuid4())
			for idx, chunk in enumerate(chunks):
				insert_chunk(conn, doc_id, pdf, idx, chunk)
				total_chunks += 1
			total_files += 1
	return total_files, total_chunks


if __name__ == "__main__":
	# Support both 'pdf_data' and 'PDF Data'
	cwd = os.getcwd()
	candidates = [
		os.path.join(cwd, "pdf_data"),
		os.path.join(cwd, "PDF Data"),
	]
	target_folder = next((p for p in candidates if os.path.isdir(p)), None)
	if not target_folder:
		print(f"Folder not found. Checked: {', '.join(candidates)}")
	else:
		files, chunks = ingest_folder(target_folder)
		print(f"Ingested {files} PDFs into {chunks} chunks from: {target_folder}")

