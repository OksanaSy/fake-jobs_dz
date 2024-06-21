import os
from bs4 import BeautifulSoup
import pandas as pd
from pydantic import BaseModel, FieldValidationInfo, field_validator, ValidationError
from typing import Optional, Dict, Any


class BoxData(BaseModel):
    title: Optional[str] = None
    company: Optional[str] = None
    content: Optional[str] = None
    location: Optional[str] = None
    date: Optional[str] = None

    @field_validator('title', 'company', 'content', 'location', 'date')
    def content_must_not_be_empty(cls, v: Optional[str], field: FieldValidationInfo) -> Optional[str]:
        if v is not None and not v.strip():
            raise ValueError(f'{field.name} must not be empty')
        return v


def parse_html_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        soup = BeautifulSoup(file, 'html.parser')
        boxes = soup.find_all('div', class_='box')
        parsed_data = []
        for box in boxes:
            data = {}
            title = box.find('h1', class_='title')
            if title:
                data['title'] = title.get_text(strip=True)
            company = box.find('h2', class_='company')
            if company:
                data['company'] = company.get_text(strip=True)
            content_div = box.find('div', class_='content')
            if content_div:
                content_paragraphs = content_div.find_all('p')
                if content_paragraphs:
                    data['content'] = ' '.join(p.get_text(strip=True) for p in content_paragraphs if not p.get('id'))
                    for p in content_paragraphs:
                        if p.get('id') == 'location':
                            data['location'] = p.get_text(strip=True).replace('Location:', '').strip()
                        elif p.get('id') == 'date':
                            data['date'] = p.get_text(strip=True).replace('Posted:', '').strip()
            try:
                box_data = BoxData(**data)
                parsed_data.append(box_data.dict())
            except ValidationError as e:
                print(f"Validation error in file {file_path}: {e}")
        return parsed_data


def collect_data_from_directory(directory):
    all_data = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.html'):
                file_path = os.path.join(root, file)
                file_data = parse_html_file(file_path)
                all_data.extend(file_data)
    return all_data


def save_data_to_parquet(data, output_file):
    df = pd.DataFrame(data)
    df.to_parquet(output_file, index=False)


if __name__ == '__main__':
    directory = 'jobs'
    output_file = 'result.parquet'

    collected_data = collect_data_from_directory(directory)
    save_data_to_parquet(collected_data, output_file)
    print(f"Data saved to {output_file}")
