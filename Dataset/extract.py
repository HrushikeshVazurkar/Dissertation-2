import PyPDF2
import re
import os
import pandas as pd

def preprocess_text_from_pdf(text):
    text_normalized = text.encode('ascii', 'ignore').decode() # Unicode normalization (NFD form)
    text_normalized = re.sub(r'[^\x00-\x7F]+', ' ', text_normalized) # noise removal - non printable chars
    return text_normalized

def extract_pdf_text(pdf_file):
    text = ""
    with open(pdf_file, 'rb') as f:
        reader = PyPDF2.PdfReader(f)
        for page_num in range(len(reader.pages)):
            page = reader.pages[page_num]
            text += page.extract_text()
    
    return preprocess_text_from_pdf(re.sub('–', '-', text))

def extract_text_between_headings(text, headings):
    lines = text.splitlines()
    hdi = []

    for heading in headings:
        flag = 0
        for i, line in enumerate(lines):
            if heading in line:
                hdi.append(i); flag = 1
                break
        if flag == 0:
            hdi.append(-1)

    hdi[len(hdi) - 1] = len(lines)-2

    if hdi[2] != -1:
        complaint = ' '.join(lines[hdi[0] + 1: hdi[1]])
        what_happened = ' '.join(lines[hdi[1] + 1: hdi[2]])
        provisional = ' '.join(lines[hdi[2] + 1: hdi[3]])
        decided_and_why = ' '.join(lines[hdi[3] + 1: hdi[4]])
        final_decision = ' '.join(lines[hdi[4] + 1: hdi[5]])
    else:
        complaint = ' '.join(lines[hdi[0] + 1: hdi[1]])
        what_happened = ' '.join(lines[hdi[1] + 1: hdi[3]])
        provisional = None
        decided_and_why = ' '.join(lines[hdi[3] + 1: hdi[4]])
        final_decision = ' '.join(lines[hdi[4] + 1: hdi[5]])
        
    return [complaint, what_happened, provisional, decided_and_why, final_decision]

def create_pdf_df():
    directory = 'decisions'
    headings = [r'The complaint', r'What happened', r'provisional', r"What Ive decided - and why", r'My final decision', r'Ombudsman']

    data = []
    for filename in os.listdir(directory):
        pdf_file = os.path.join(directory, filename)
        pdf_text = extract_pdf_text(pdf_file)
        row = extract_text_between_headings(pdf_text, headings)
        row.insert(0, re.sub(r'.pdf', '', filename))

        if "partially uphold" in row[len(row) - 1]: 
            row.append("Yes")
        else:
            row.append("No")

        data.append(row)

    df = pd.DataFrame(data, columns=['decision_id', 'The complaint', 'What happened', 'Provisional decision', 'What Ive decided - and why', 'My final decision', 'Partially Upheld'])
    return df