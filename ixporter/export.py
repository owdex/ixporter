import gzip
import hashlib
import json
import os
import random
import shutil
import string
import tempfile

from pathlib import Path
from more_itertools import chunked

from pysolr import Solr
import jsonlines


class Exporter:
    def __init__(self, path: Path, db: Solr, entry_split: int = 100):
        self.version = "0.1"
        self.path = path
        self.path.mkdir(parents=True, exist_ok=True)
        self.tmpdir = tempfile.TemporaryDirectory(prefix="ixporter_")
        self.entry_split = entry_split
        self.entries = db.search("*:*", sort="id ASC", cursorMark="*")

    def _get_bundle_checksums(self, filename):
        sha256 = hashlib.sha256()

        with open(filename, "rb") as file:
            while True:
                data = file.read(sha256.block_size)
                if not data:
                    break
                sha256.update(data)

        return {"sha256": sha256.hexdigest()}

    def _id_generator(self):
        """creates 8 long random sequences of ascii chars. To be used for temporary files"""
        return "".join(random.choices(string.ascii_uppercase + string.digits, k=8))

    def _create_bundle(self, records):
        """Takes a list of records and writes out a bundle file
        Returns information about the bundle file, for inclusion in the manifest
        """

        tmpfilepath = Path(f"{self.tmpdir.name}/tmp_{self._id_generator()}.jsonl.gz")
        tmpfilepath.touch()
        # Todo: consider different compression algorithms
        with gzip.open(tmpfilepath, mode="wb") as file:
            with jsonlines.Writer(file) as writer:
                for record in records:
                    writer.write(record)

        digests = self._get_bundle_checksums(tmpfilepath)

        folder = self.path / digests["sha256"][0:2]
        Path(folder).mkdir(exist_ok=True)

        filepath = folder / f"{digests['sha256'][2:10]}.jsonl.gz"
        shutil.move(tmpfilepath, filepath)

        return {
            "path": filepath.name,
            "checksums": digests,
            "filesize": os.stat(filepath).st_size,
            "fragment_version": 0.1,
            "records": len(records),
            "compression": "gzip",
        }

    def write_bundle(self):
        """Split entries into batches, and write a bundle of those files with a manifest."""

        bundel_files = []
        for batch in chunked(self.entries, 10000):
            bundel = self._create_bundle(batch)
            bundel_files.append(bundel)
            print(bundel)

        total_records: int = 0
        for bundel in bundel_files:
            total_records += bundel["records"]

        manifest_path = f"{self.path}/manifest.json"
        with open(manifest_path, mode="w") as file:
            # Don't need jsonlines here
            file.write(
                json.dumps({
                    "files": bundel_files,
                    "index_version": 1,
                    "records": total_records,
                }))
