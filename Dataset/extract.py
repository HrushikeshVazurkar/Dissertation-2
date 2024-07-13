import PyPDF2
import re
import os
import pandas as pd

def extract_pdf_text(pdf_file):
    text = ""
    with open(pdf_file, 'rb') as f:
        reader = PyPDF2.PdfReader(f)
        for page_num in range(len(reader.pages)):
            page = reader.pages[page_num]
            text += page.extract_text()
    return re.sub('”', '"', re.sub('“', '"', re.sub('–', '-', re.sub('’', '', re.sub(r'\uf0b7', '', text)))))

def extract_text_between_headings(text, headings):
    lines = text.splitlines()
    heading_index = []; extracted_texts = []

    for heading in headings:
        flag = 0
        for i, line in enumerate(lines):
            if heading in line:
                heading_index.append(i); flag = 1
                break
        if flag == 0:
            heading_index.append(-1)

    heading_index[len(heading_index) - 1] = len(lines)-2

    for idx in range(1, len(heading_index)):
        if heading_index[idx] == -1:
            pass
        elif heading_index[idx - 1] == -1:
            start_idx = heading_index[idx - 2]; end_idx = heading_index[idx]
            extracted_text = ' '.join(lines[start_idx + 1:end_idx])
            extracted_texts.append(extracted_text)
            extracted_texts.append(None)
        else:
            start_idx = heading_index[idx - 1]; end_idx = heading_index[idx]
            extracted_text = ' '.join(lines[start_idx + 1:end_idx])
            extracted_texts.append(extracted_text)

    return extracted_texts

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

    df = pd.DataFrame(data, columns=['decision_id', 'The complaint', 'What happened', 'Provisional decision', 'What Ive decided – and why', 'My final decision', 'Partially Upheld'])
    return df