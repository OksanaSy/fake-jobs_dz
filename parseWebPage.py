import requests
import logging
from bs4 import BeautifulSoup
import pandas as pd
from pydantic import BaseModel, FieldValidationInfo, field_validator, ValidationError
from typing import Optional


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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


def parse_job_page(url):
    try:
        #logger.info(f"Fetching page: {url}")
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
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
                print(f"Validation error in file {url}: {e}")

        return parsed_data
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch page: {url}, Error: {str(e)}")
    except AttributeError as e:
        logger.error(f"AttributeError in parsing page: {url}, Error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in parsing page: {url}, Error: {str(e)}")


def collect_job_data(base_url):
    job_urls = []
    try:
        response = requests.get(base_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        cards = soup.find_all('div', class_='card')
        for card in cards:
            apply_tag = card.find('a', class_='card-footer-item', string='Apply', href=True)
            if apply_tag:
                job_url = apply_tag['href']
                job_urls.append(job_url)
        logger.info(f"Total job URLs found: {len(job_urls)}")
        job_data = [parse_job_page(url) for url in job_urls]
        return job_data
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch or parse base URL: {base_url}, Error: {str(e)}")


def save_data_to_parquet(data, output_file):
    df = pd.DataFrame(data)
    df['title'] = df[0].apply(lambda x: x['title'] if x is not None and 'title' in x else None)
    df['company'] = df[0].apply(lambda x: x['company'] if x is not None and 'company' in x else None)
    df['content'] = df[0].apply(lambda x: x['content'] if x is not None and 'content' in x else None)
    df['location'] = df[0].apply(lambda x: x['location'] if x is not None and 'location' in x else None)
    df['date'] = df[0].apply(lambda x: x['date'] if x is not None and 'date' in x else None)
    df.drop(columns=[0], inplace=True)
    df.to_parquet(output_file, index=False)


if __name__ == '__main__':
    base_url = 'https://realpython.github.io/fake-jobs/'
    output_file = 'result.parquet'

    collected_data = collect_job_data(base_url)
    if collected_data:
        save_data_to_parquet(collected_data, output_file)
        logger.info(f"Data saved to {output_file}")
    else:
        logger.error("No data collected or parsed.")
