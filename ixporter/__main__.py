from typer import Typer, Argument, Option
import rich
from rich import print
from pathlib import Path

from pysolr import Solr, SolrError

from ixporter import Exporter
from ixporter.sample import load_sample_data
from ixporter.__init__ import STATE


app = Typer()

def error(message: str):
    print(rich.panel.Panel(message, title="Error", title_align="left", border_style=rich.style.Style(color="red")))

@app.command()
def export(database_url: str, path: Path = Path("./export")):
    ex = Exporter(path, Solr(database_url))
    ex.write_bundle()

@app.command("import")
def import_(database_url: str):
    pass

@app.command()
def sample(
    database_url: str,
    lines: int = Argument(25),
    timeout: int = Argument(1),
    threads: int = Argument(150),
):
    load_sample_data(Solr(database_url), lines, timeout, threads)


@app.callback()
def main(verbose: bool = Option(False)):
    if verbose:
        STATE["verbose"] = True


if __name__ == "__main__":
    try:
        app()
    except SolrError as e:
        if "NewConnectionError" in str(e):
            error("Couldn't connect to Solr. Is it running? Is the path you gave accurate?")
        elif "SSLError" in str(e):
            error("Couldn't connect to Solr over HTTPS. Usually, this means you typed HTTPS by mistake, and should replace it with HTTP.")
        else:
            raise