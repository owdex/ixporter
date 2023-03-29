from io import StringIO
from hashlib import md5
from pathlib import Path
from typing import Sequence

import more_itertools

from pysolr import Solr
import jsonlines


def write_batch(entries: Sequence[dict], directory: Path) -> Path:
    """Write entries to a single JSONlines batch file."""
    with StringIO() as buffer:
        with jsonlines.Writer(buffer) as writer:
            writer.write(entries)

        buffer_value = buffer.getvalue()
        filename = f"{md5(buffer_value.encode()).hexdigest()}.jsonl"
        file = directory / filename
        file.write_text(buffer_value)

    return file

def write_bundle(entries: Sequence[dict], directory: Path, split: int = 100) -> Path:
    """Split entries into batches, and write a bundle of those files with a manifest."""
    for batch in more_itertools.batched(entries, split):
        write_batch(batch, directory)

def record_to_entries(record: Path) -> list[dict]:
    with jsonlines.Reader(record) as reader:
        return [entry for entry in reader]
