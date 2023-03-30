from typer import Typer
from pathlib import Path

from pysolr import Solr

from ixporter import Exporter
from ixporter.sample import load_sample_data


app = Typer()


@app.command()
def export(database_url: str, path: Path = Path("./export")):
    ex = Exporter(path, Solr(database_url))
    ex.write_bundle()

@app.command("import")
def import_(database_url: str):
    pass

@app.command()
def sample(database_url: str, lines: int = 25):
    load_sample_data(Solr(database_url), lines)


if __name__ == "__main__":
    app()
