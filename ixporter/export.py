from io import StringIO
from hashlib import md5
from pathlib import Path
from typing import Sequence
from more_itertools import batched

from pysolr import Solr
import jsonlines

class Exporter:
    def __init__(self, path: Path, , db: Solr, entry_split: int = 100):
        self.version = "0.1"
        self.path = path
        self.path.mkdir(parents=True, exist_ok=True)
        self.entry_split = entry_split
        self.entries = db.search("*:*").docs

    
    def _get_bundle_checksum(self):
        checksum = md5()
        for batch in self.path.iterdir():
            checksum.update(batch.name.encode())
        return checksum


    def write_bundle(self):
        """Split entries into batches, and write a bundle of those files with a manifest."""
        for batch in batched(self.entries, self.entry_split):
            with StringIO() as buffer:
                with jsonlines.Writer(buffer) as writer:
                    writer.write(batch)

                buffer_value = buffer.getvalue()
                file_path = self.path / f"{md5(buffer_value.encode()).hexdigest()}.jsonl"
                with file_path.open('w+') as file:
                    file.write(buffer_value)

        manifest_path = self.path / "manifest.json"
        with jsonlines.Writer(manifest_path.open(mode = "w+")) as writer:
            writer.write({
            "version": self.version,
            "records": len(self.entries),
            "checksums": {
                "md5": self._get_bundle_checksum().hexdigest(),
            },
            "compression": "None"
        })
