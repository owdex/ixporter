from io import StringIO
from hashlib import md5
from pathlib import Path
from typing import Sequence

import more_itertools

from pysolr import Solr
import jsonlines

class Exporter:
    def __init__(self, path: Path, entry_split: int = 100, db: Solr|None = None):
        self.path = path
        self.entry_split = entry_split
        if db:
            self.load_entries(db)

    
    def load_entries(self, db: Solr):
        self.entries = db.search("*:*").docs
    

    def _write_batch(self, batch: list) -> Path:
        """Write entries to a single JSONlines batch file."""
        with StringIO() as buffer:
            with jsonlines.Writer(buffer) as writer:
                writer.write(batch)

            buffer_value = buffer.getvalue()
            file_path = self.path / f"{md5(buffer_value.encode()).hexdigest()}.jsonl"
            with file_path.open('w+') as file:
                file.write(buffer_value)

        return file_path


    def write_bundle(self):
        """Split entries into batches, and write a bundle of those files with a manifest."""
        for batch in more_itertools.batched(self.entries, self.entry_split):
            self._write_batch(batch)

ex = Exporter(
    Path("."),
    db = Solr("http://localhost:8983/solr/gettingstarted")
)
ex.write_bundle()