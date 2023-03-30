import csv
import zipfile as zf
from io import BytesIO, TextIOWrapper

from rich.progress import track, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from bs4 import BeautifulSoup as bs
from pysolr import Solr
import requests

def load_sample_data(db: Solr, lines: int):
    with Progress(
        SpinnerColumn(),
        *Progress.get_default_columns(),
        TimeElapsedColumn(),
    ) as progress:

        download_task = progress.add_task(description="Downloading corpus...", total=None)
        corpus_zip = requests.get("https://www.corpusdata.org/iweb/samples/iweb_sources.zip")
        progress.update(download_task, visible=False)

        with zf.ZipFile(BytesIO(corpus_zip.content)) as unzipper:
            with open(unzipper.extract("iweb_sources.txt", path="/tmp"), encoding="latin_1") as corpus_file:
                corpus = corpus_file.readlines()[:lines]
                reader = csv.reader(corpus, delimiter="\t")

                read_task = progress.add_task(description="Adding entries...", total=lines)
                for entry in reader:

                    url = entry[3]
                    title = entry[4]

                    soup = bs(requests.get(url).text, features="html.parser")
                    content = soup.get_text()
                    description = soup.find("meta", attrs={"name": "description"})

                    # if there was a description, set that, otherwise just use content
                    description = description.get("content") if description else content
                    
                    db.add({
                        "url": url,
                        "title": title,
                        "submitter": "sampler",
                        "content": content,
                        "description": description
                    })
                    progress.update(read_task, advance=1)
        
        db.commit()
