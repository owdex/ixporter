import csv
import zipfile as zf
from io import BytesIO, TextIOWrapper
from random import sample as random_sample

from bs4 import BeautifulSoup as bs
from pysolr import Solr
import requests

def load_sample_data(db: Solr, lines: int):
        print("Downloading corpus (this should take less than a minute)...")
        corpus_zip = requests.get("https://www.corpusdata.org/iweb/samples/iweb_sources.zip")

        print("Extracting...")
        with zf.ZipFile(BytesIO(corpus_zip.content)) as unzipper:
            with open(unzipper.extract("iweb_sources.txt", path="/tmp"), encoding="latin_1") as corpus_file:
                print("Reading...")
                corpus = random_sample(corpus_file.readlines(), lines)
                reader = csv.reader(corpus, delimiter="\t")

                print("Loading entries...")
                for entry in reader:

                    url = entry[3]
                    title = entry[4]

                    try:
                        soup = bs(requests.get(url, timeout=5).text, features="html.parser")
                        content = soup.get_text()
                        if not content: raise ValueError
                        description = soup.find("meta", attrs={"name": "description"})

                        # if there was a description, set that, otherwise just use content
                        description = description.get("content") if description else content

                        if len(description) > 150:
                            description = description[:149] + "&hellip;"
                    except requests.exceptions.RequestException:
                        print(f"Warning: problem connecting to {url}")
                    except ValueError:
                        print(f"Warning: {url} had no content and was ignored")
                    else:
                        db.add({
                            "url": url,
                            "title": title,
                            "submitter": "sampler",
                            "content": content,
                            "description": description
                        })
        
        print("Committing...")
        db.commit()
        print("Done!")
