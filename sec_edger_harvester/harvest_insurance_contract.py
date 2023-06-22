import pandas as pd
import ray
from sec_edger_harverster import SecEdgerHarvester

headers = {'User-Agent': "email@address.com"}

# Target Phrases to be serched in first page of exhibit file
target_phrases_in_exhibit_file = [
    "reinsurance agreement",
    "insurance contract",
    "reinsurance contract",
    "insurance agreement",
    "risk-sharing agreement",
    "coinsurance agreement",
    "retrocession agreement",
    "quota share agreement",
    "excess of loss agreement",
    "stop loss agreement",
    "facultative reinsurance agreement",
    "treaty reinsurance agreement",
    "co-insurance agreement",
    "excess of loss reinsurance agreement",
    "stop loss reinsurance agreement",
    "quota share treaty",
    "excess of loss treaty",
    "facultative treaty",
    "proportional treaty",
    "non-proportional treaty",
    "fronting agreement",
    "retrocession treaty",
    "facultative reinsurance",
    "quota share reinsurance",
    "excess of loss reinsurance",
    "stop loss reinsurance",
    "fronting reinsurance",
    "retrocession reinsurance",
    "xol contract",
    "yrt agreement",
    "reins agreement"
]

target_exhibit_numbers = ["EX-10","EX-99"]

@ray.remote
def get_links(target_exhibit_numbers,target_phrases_in_exhibit_file,cik):
    
    harverster = SecEdgerHarvester(target_exhibit_numbers,target_phrases_in_exhibit_file)

    return harverster.get_target_exhibit_file_urls(cik)

def write_log_to_file(log_text,file_path):
    logging.info(log_text)
    file_object = open(file_path, 'a')
    file_object.write(log_text)
    file_object.close()
    return True

def harverst_insurance_contract(cik_list=None,result_path=None):
    
    if cik_list == None:
        #If CIK list is not provided consider cik of all company
        # get all companies data
        response = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers=headers
            )

        tickers_data = json.loads(response.text)

        # dictionary to dataframe
        companyData = pd.DataFrame.from_dict(tickers_data,
                                            orient='index')

        # add leading zeros to CIK
        companyData['cik_str'] = companyData['cik_str'].astype(
                                str).str.zfill(10)

        # Extract the CIK values from the JSON data
        cik_list = companyData['cik_str'].tolist()

        cik_list.sort()
        
    links = [get_links.remote(target_exhibit_numbers,target_phrases_in_exhibit_file,cik) for cik in cik_list]


    file_path="sec_edger_insurance_contracts.csv"
    if result_path:
        file_path=result_path

    log_text=f"exhibit_number,matching_phases,url\n"
    write_log_to_file(log_text,file_path)

    result_df_arr=[]
    for result_arr in ray.get(links):
        df = pd.DataFrame(result_arr, columns =['exhibit_number', 'matching_phases', 'url'])
        result_df_arr.append(df)
        for exhibit_number, matching_phases , url in result_arr:
            log_text=f"{exhibit_number},{matching_phases},{url}\n"
            write_log_to_file(log_text,file_path)

    result = pd.concat(result_df_arr, ignore_index=True, sort=False)
    return result.tolist()

    
    
