import csv
import zipfile as zf
from io import BytesIO, TextIOWrapper
from random import sample as random_sample

from bs4 import BeautifulSoup as bs
from pysolr import Solr
import requests

from concurrent.futures import ThreadPoolExecutor
import itertools

from ixporter.__init__ import STATE


def _import_url(entry, db, timeout):
    url = entry[3]
    title = entry[4]

    try:
        soup = bs(requests.get(url, timeout=timeout).text,
                  features="html.parser")
        content = soup.get_text()
        if not content:
            raise ValueError
        description = soup.find("meta", attrs={"name": "description"})

        # if there was a description, set that, otherwise just use content
        description = (description.get("content") if description
                       and description.get("content") else content)

        if len(description) > 150:
            description = description[:149] + "&hellip;"
    except requests.exceptions.RequestException:
        if STATE["verbose"]:
            print(f"Warning: problem connecting to {url}")
    except ValueError:
        if STATE["verbose"]:
            print(f"Warning: {url} had no content and was ignored")
    else:
        if STATE["verbose"]:
            print(f"Success with {url}")
        db.add({
            "url": url,
            "title": title,
            "submitter": "sampler",
            "content": content,
            "description": description,
            "votes": 1,
        })


def load_sample_data(db: Solr, lines: int, timeout: int, threads: int):
    print("Downloading corpus (this should take less than a minute)...")
    corpus_zip = requests.get(
        "https://www.corpusdata.org/iweb/samples/iweb_sources.zip")

    print("Extracting...")
    with zf.ZipFile(BytesIO(corpus_zip.content)) as unzipper:
        with open(unzipper.extract("iweb_sources.txt", path="/tmp"),
                  encoding="latin_1") as corpus_file:
            print("Reading...")

            if lines == 0:
                corpus = corpus_file.readlines()
            else:
                corpus = random_sample(corpus_file.readlines(), lines)

            reader = csv.reader(corpus, delimiter="\t")

            print("Loading entries...")

            list_of_groups = zip(*(iter(reader), ) * 1000)

            for batch in list_of_groups:
                with ThreadPoolExecutor(max_workers=threads) as executor:
                    executor.map(
                        _import_url,
                        batch,
                        itertools.repeat(db),
                        itertools.repeat(timeout),
                    )

                print("Committing...")
                db.commit()

    print("Committing...")
    db.commit()
    print("Done!")
