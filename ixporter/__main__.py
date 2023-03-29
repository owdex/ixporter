from typer import Typer
from pathlib import Path

from pysolr import Solr

from ixporter import Exporter


app = Typer()


@app.command()
def export(database_url: str, path: str = "./export"):
    ex = Exporter(Path(path))
    ex.load_entries(Solr(database_url))
    ex.write_bundle()

app.command()
def import_(database_url: str):
    pass


if __name__ == "__main__":
    app()
