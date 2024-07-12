import csv
import datetime
import time
import urllib.request
from pathlib import Path

import requests
import typer
from bs4 import BeautifulSoup

opener = urllib.request.build_opener()
opener.addheaders = [("User-agent", "Mozilla/5.0")]
urllib.request.install_opener(opener)

BASE_URL = "https://www.financial-ombudsman.org.uk/decisions-case-studies/ombudsman-decisions/search"
BASE_DECISIONS_URL = "https://www.financial-ombudsman.org.uk/"
BASE_PARAMETERS = {
    "Sort": "date",
    "Start": 0,
    }

INDUSTRY_SECTOR_MAPPING = {
    "banking-credit-mortgages": 1,
    "investment-pensions": 2,
    "insurance": 3,
    "payment-protection-insurance": 4,
    "claims-management-ombudsman-decisions": 5,
    "funeral-plans": 6,
}

app = typer.Typer()


def process_entry(entry):
    anchor = entry.find("a")
    decision_url_part = anchor["href"]
    title = anchor.find("h4").text.strip()
    metadata = anchor.find("div", class_="search-result__info-main").text
    tag = anchor.find("span", class_="search-result__tag").text

    metadata = [m.strip() for m in metadata.strip().split("\n") if m.strip()]
    [date, company, decision, *extras] = metadata
    extras = ",".join(extras)

    decision_id = Path(decision_url_part).stem

    return {
        "decision_id": decision_id,
        "location": decision_url_part,
        "title": title,
        "date": date,
        "company": company,
        "decision": decision,
        "extras": extras,
        "tag": tag.strip(),
    }


@app.command()
def get_metadata(
     keyword: str = typer.Option(None, help="Keyword to search for"),
    from_: str = typer.Option(None, "--from", help="The start date for the search"),
    to: str = typer.Option(None, help="The end date for the search"),
    upheld: bool = typer.Option(None, help="Filter by whether the decision was upheld"),
    industry_sector: str = typer.Option(
        None, help="Filter by industry sector, separated by commas. If not provided, all sectors will be included"
    ),
):
    # Calculate vales for the default parameters
    today = datetime.date.today()
    from_ = datetime.datetime.strptime(from_, "%Y-%m-%d") if from_ else today - datetime.timedelta(days=50)
    to = datetime.datetime.strptime(to, "%Y-%m-%d") if to else today
    industry_sectors = industry_sector.split(",") if industry_sector else list(INDUSTRY_SECTOR_MAPPING.keys())

    # Build the url parameters
    parameters = BASE_PARAMETERS.copy()
    for selected_industry_sector in industry_sectors:
        parameters[f"IndustrySectorID[{INDUSTRY_SECTOR_MAPPING[selected_industry_sector]}]"] = INDUSTRY_SECTOR_MAPPING[
            selected_industry_sector
        ]

    if upheld is None:
        parameters["IsUpheld[0]"] = "0"
        parameters["IsUpheld[1]"] = "1"
    elif upheld:
        parameters["IsUpheld[1]"] = "1"
    else:
        parameters["IsUpheld[0]"] = "0"

    parameters["DateFrom"] = from_.strftime("%Y-%m-%d")
    parameters["DateTo"] = to.strftime("%Y-%m-%d")
    if keyword:
        parameters["Keywords"] = keyword

    metadata_entries = []
    for start in range(0, 1_000_000, 10):
        parameters["Start"] = start
        results = requests.get(BASE_URL, params=parameters)

        soup = BeautifulSoup(results.text, "html.parser")

        search_results = soup.find("div", class_="search-results-holder").find("ul", class_="search-results")
        entries = search_results.find_all("li")

        if not entries:
            typer.echo(f"Finished scraping at {start}")
            break

        typer.echo(f"Scraping {len(entries)} entries from page {start}")

        for entry in entries:
            processed_entry = process_entry(entry)
            metadata_entries.append(processed_entry)

    if not metadata_entries:
        typer.echo("No results found")
    else:
        typer.echo(f"Writing {len(metadata_entries)} entries to metadata.csv")
        with open("metadata.csv", "w") as f:
            writer = csv.DictWriter(f, fieldnames=metadata_entries[0].keys())
            writer.writeheader()
            writer.writerows(metadata_entries)


@app.command()
def download_decisions(
    metadata_file: Path = typer.Argument("metadata.csv", help="The path to the metadata file"),
    output_dir: Path = typer.Argument("decisions", help="The path to the output directory"),
):
    output_dir.mkdir(exist_ok=True)

    with open(metadata_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            output_file = output_dir / f"{row['decision_id']}.pdf"
            if output_file.exists():
                typer.echo(f"Skipping {output_file} as it already exists")
                continue

            time.sleep(1)
            decision_url = BASE_DECISIONS_URL + row["location"]
            urllib.request.urlretrieve(decision_url, output_file)


if __name__ == "__main__":
    app()

