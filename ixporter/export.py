from io import BytesIO
from hashlib import file_digest as digest
from pathlib import Path

from pysolr import Solr
import jsonlines


def entries_to_record(entries: Sequence[dict], directory: Path) -> str:
    with BytesIO() as buffer:
        with jsonlines.Writer(buffer) as writer:
            writer.write_all(entries)

        filename = f"{digest(buffer, 'md5').hexdigest()}.jsonl"
        (path / filename).write_bytes(buffer.getbuffer())

    return filename

def record_to_entries(record: Path) -> list[dict]:
    with jsonlines.Reader(record) as reader:
        return [entry for entry in reader]
