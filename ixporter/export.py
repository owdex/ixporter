from io import BytesIO
from hashlib import file_digest as digest
from pathlib import Path

import more_itertools

from pysolr import Solr
import jsonlines


def write_batch(entries: Sequence[dict], directory: Path) -> Path:
    """Write entries to a single JSONlines batch file."""
    with BytesIO() as buffer:
        with jsonlines.Writer(buffer) as writer:
            writer.write_all(entries)

        filename = f"{digest(buffer, 'md5').hexdigest()}.jsonl"
        file = directory / filename
        file.write_bytes(buffer.getbuffer())

    return file

def write_bundle(entries: Sequence[dict], directory: Path, split: int = 100) -> Path:
    """Split entries into batches, and write a bundle of those files with a manifest."""
    for batch in more_itertools.batched(entries, split):
        write_batch(batch, directory)

def record_to_entries(record: Path) -> list[dict]:
    with jsonlines.Reader(record) as reader:
        return [entry for entry in reader]
