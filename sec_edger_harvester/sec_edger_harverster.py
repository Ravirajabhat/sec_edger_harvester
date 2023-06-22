import json
import logging

import pandas as pd
import requests
import spacy
from bs4 import BeautifulSoup
from spacy.matcher import PhraseMatcher


class SecEdgerHarvester():

    def __init__(self,target_exhibit_numbers,target_phrases_in_exhibit_file,email_address="email@address.com"):
        
        self.nlp = spacy.load('en_core_web_sm')
        self.phrase_matcher = PhraseMatcher(nlp.vocab, attr="LOWER")
        
        patterns = [nlp(text.lower().strip()) for text in target_phrases_in_exhibit_file]
        self.phrase_matcher.add('AI', None, *patterns)
        self.target_exhibit_numbers=target_exhibit_numbers
        self.headers = {'User-Agent': email_address}
        # Define the base URL for the SEC's EDGAR website
        self.base_url = 'https://www.sec.gov'

    def get_target_exhibit_file_urls(cik):
        """
        """

        target_exhibit_file_urls = []
        logging.info(f"Started harversing for {cik}")
        search_url = f'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=&dateb=20230616&owner=exclude&search_text=&CIK={cik}&output=xml'

        # Send a request to the search URL
        response = requests.get(search_url,headers=self.headers)
    
        # Parse the response content with BeautifulSoup
        soup = BeautifulSoup(response.content, 'xml')

        # Find all filing detail URLs
        filing_detail_urls = soup.find_all('filingHREF')

        # Iterate over the filing detail URLs to extract the reinsurance contract URLs
        for filing_detail_url in filing_detail_urls:
            # Send a request to the filing detail URL with authentication headers
            filing_response = requests.get(filing_detail_url.text,headers=self.headers)
            # Iterate over the entries to extract the URLs of reinsurance contract files

            filing_soup = BeautifulSoup(filing_response.content, 'html.parser')
            
            # Find the exhibit table
            exhibit_table = filing_soup.find('table', class_='tableFile', summary='Document Format Files')
            
            if not exhibit_table:
                continue
            
            # Find the rows in the exhibit table
            rows = exhibit_table.find_all('tr')
            
            # Iterate over the rows to find the exhibit with number 99.1
            for row in rows:
                cells = row.find_all('td')
                if len(cells)<3:
                    continue
                exhibit_link = row.find('a')
            
                if cells[3].text[:5] in self.target_exhibit_numbers:
                    exhibit_url=""
                    if exhibit_link:
                        exhibit_url = self.base_url + exhibit_link['href']

                    file_name=exhibit_url.rsplit('/', 1)[-1]
                    exhibit_response = requests.get(exhibit_url,headers=self.headers)

                    soup = BeautifulSoup(exhibit_response.content, features="html.parser")
                    response_text=soup.get_text()

                    first_page_text=""
                    if 'page' in response_text.lower():
                        first_page_text=response_text.lower().split('page')[0].lower()
                        if len(first_page_text)>1000:
                            first_page_text=first_page_text[:1000]
                    else:
                        first_page_text=response_text[:500].lower()

                    sentence = nlp (first_page_text.lower())
                    matched_phrases = self.phrase_matcher(sentence)
                    
                    mached_pattern=[]
                    for match_id, start, end in matched_phrases:
                        span = sentence[start:end]  
                        mached_pattern.append(span.text)

                    if any(mached_pattern):
                       
                        log_text=f"{cells[3].text},{mached_pattern},{exhibit_url}\n"
                        logging.info(log_text)
                        target_exhibit_file_urls.append(({cells[3].text},mached_pattern,exhibit_url))

        logging.info(f"End harversing for {cik}")
        return target_exhibit_file_urls
