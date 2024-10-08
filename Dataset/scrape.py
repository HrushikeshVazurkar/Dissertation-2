import csv
import datetime
import time
import urllib.request
from pathlib import Path

import requests
import typer
from bs4 import BeautifulSoup

import re
import os
import pandas as pd

from extract import create_pdf_df
from sklearn.model_selection import train_test_split
import spacy
nlp = spacy.load('en_core_web_sm') # SpaCy English Language Model

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

def extract_product_info(doc):
    for i, token in enumerate(doc):
        if token.text == 'insurance':
            for j in range(i - 1, -1, -1): # nearest pron/deter on the left
                if doc[j].pos_ in ['PRON', 'DET', 'VERB'] or "’s" in doc[j].text:
                    phrase = doc[j + 1:i] # words between the pron/deter/verb and "insurance"
                    return re.sub('’', '', phrase.text)
    return ""

def process_entry(entry):
    anchor = entry.find("a")
    decision_url_part = anchor["href"]
    title = anchor.find("h4").text.strip()
    metadata = anchor.find("div", class_="search-result__info-main").text
    tag = anchor.find("span", class_="search-result__tag").text
    description = anchor.find("div", class_="search-result__desc").text
    product = extract_product_info(nlp(description))

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
        "product": product,
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
        parameters["Keyword"] = keyword

    metadata_entries = []
    for start in range(0, 1_000_000, 10):
        parameters["Start"] = start

        try:
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
        except:
            print("Connection refused by the server..")
            print("Let me sleep for 5 seconds")
            time.sleep(5)

    if not metadata_entries:
        typer.echo("No results found")
    else:
        typer.echo(f"Writing {len(metadata_entries)} entries to metadata.csv")
        with open("metadata.csv", "w") as f:
            writer = csv.DictWriter(f, fieldnames=metadata_entries[0].keys())
            writer.writeheader()
            writer.writerows(metadata_entries)

def process_batch(batch, output_dir):
    for row in batch:
        try:
            output_file = output_dir / f"{row['decision_id']}.pdf"
            if output_file.exists():
                typer.echo(f"Skipping {output_file} as it already exists")
                continue

            # time.sleep(1)
            decision_url = BASE_DECISIONS_URL + row["location"]
            urllib.request.urlretrieve(decision_url, output_file)
        except:
            print('sleeping for 5 sec.')
            time.sleep(5)
            continue
        
final_df = None

@app.command()
def download_decisions(
    metadata_file: Path = typer.Argument("metadata.csv", help="The path to the metadata file"),
    output_dir: Path = typer.Argument("decisions", help="The path to the output directory"),
):
    output_dir.mkdir(exist_ok=True); batch = []; batch_size = 100
    metadata_df = pd.read_csv("metadata.csv", encoding='cp1252').drop(columns=['location', 'title', 'extras', 'tag'])

    if os.path.exists('dataset.csv'):
        total_memory = os.path.getsize('dataset.csv')/(1024*1024)
    else:
        total_memory = 0

    with open(metadata_file) as f:
        try:
            reader = csv.DictReader(f)
            count = 0; c = 1
            for row in reader:
                batch.append(row)
                count += 1

                if count == batch_size:
                    process_batch(batch, output_dir)
                    df = create_pdf_df()

                    full_df = pd.merge(metadata_df, df, on='decision_id', how='inner')
                    full_df.loc[full_df['Partially Upheld'] == 'Yes', 'decision'] = 'Partially upheld'
                    full_df = full_df.drop(columns=['Partially Upheld'])

                    # label encode decisions
                    label_map = {'Upheld': 0, 'Not upheld': 1, 'Partially upheld': 2}
                    full_df['decision'] = full_df['decision'].map(label_map)

                    full_df.to_csv('dataset.csv', mode='a+', index=False, header=False)

                    temp = df.memory_usage(deep=True)/(1024**2)
                    total_memory += temp.sum().round(2)
                    print("Current memory usage: ", total_memory.round(2), " MB - ", c*count, " files")

                    batch = []; count = 0; c+=1
                    for f in os.listdir(output_dir):
                        os.remove(os.path.join(output_dir, f))

            process_batch(batch, output_dir)
            df = create_pdf_df()

            full_df = pd.merge(metadata_df, df, on='decision_id', how='inner')
            full_df.loc[full_df['Partially Upheld'] == 'Yes', 'decision'] = 'Partially upheld'
            full_df = full_df.drop(columns=['Partially Upheld'])

            # label encode decisions
            label_map = {'Upheld': 0, 'Not upheld': 1, 'Partially upheld': 2}
            full_df['decision'] = full_df['decision'].map(label_map)

            full_df.to_csv('dataset.csv', mode='a+', index=False, header=False)

            temp = df.memory_usage(deep=True)/(1024**2)
            total_memory += temp.sum().round(2)
            print("Final memory usage: ", total_memory.round(2), " MB")

            for f in os.listdir(output_dir):
                os.remove(os.path.join(output_dir, f))
        except Exception as e:
            print(e)

@app.command()
def validate():
    df = pd.read_csv('dataset.csv', header=None)
    df.columns = ['decision_id', 'Date', 'Company', 'Product', 'Decision', 'The complaint', 'What happened', 'Provisional decision', 'What Ive decided - and why', 'My final decision']
    condition = df['decision_id'].str.contains('DRN')

    df['Decision'] = pd.to_numeric(df['Decision'], errors='coerce')
    valid_decisions = [0, 1, 2]    
    condition2 = df['Decision'].isin(valid_decisions)

    condition3 = ~df['My final decision'].isna()
    condition4 = ~df['The complaint'].isna()
    condition5 = ~df['What happened'].isna()
    condition6 = ~df['What Ive decided - and why'].isna()
    
    df2 = df[condition & condition2 & condition3 & condition4 & condition5 & condition6]

    print(len(df))
    print(len(df2))

    print(len(df2['decision_id'].unique()))

    df2.to_csv('final_dataset.csv', index=False)

if __name__ == "__main__":
    app()