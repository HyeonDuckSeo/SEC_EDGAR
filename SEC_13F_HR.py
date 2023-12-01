# %%
from unicodedata import name
from datetime import datetime
import pandas as pd
import requests
import re
from tqdm import tqdm 
from bs4 import BeautifulSoup
from IPython.display import display
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

pd.set_option('display.max_rows', 50)

# KIC CIK : 0001441689
# BRK CIK : 0001067983


class sec_13f_hr:
    def __init__(self, requested_cik, start_date, file_path):
        self.sec_url = self.get_sec_url(requested_cik)
        self.reponse = self.get_request(self.sec_url)
        self.url_dict = self.get_filing_url(requested_cik, start_date)
        
        self.holdings_data = self.get_holdings_data(self.url_dict)
        self.holdings_change = self.get_holdings_change(self.holdings_data)
        
        self.cusips = self.get_all_cusips(self.holdings_change)
        self.cusips_map, self.ticker_mapped_data = self.get_ticker_merged(self.cusips, self.holdings_change)
        self.sector_mapped_data, self.sector_weight_data = self.get_sector_info(self.ticker_mapped_data, file_path)
        
        self.analytics(self.sector_mapped_data, self.sector_weight_data)
    
    def get_sec_url(self, requested_cik):
        sec_url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={}&type=13F-HR&dateb=&owner=exclude&count=100&search_text='.format(requested_cik)
        return sec_url   
    
    def get_request(self, sec_url):
        headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'HOST': 'www.sec.gov',
                }
        return requests.get(sec_url, headers=headers)      
    
    def get_filing_url(self, requested_cik, start_date):
        sec_url = self.get_sec_url(requested_cik)
        response = self.get_request(sec_url)
        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.select('table.tableFile2 tr')
        print(f" < CIK {requested_cik} SEC Domain URL > ")
        print(sec_url)
        print("")
        
        url_dict = {}
        for row in rows[1:]:  # (1: -> 헤더 행 건너뛰기)
            columns = row.find_all('td')
        
            # Filing Type
            filing_type = columns[0].text.strip()
            if filing_type != '13F-HR':
                print(f"정정공시 SKIP ", ('https://www.sec.gov' + columns[1].find('a')['href']))
                continue
            
            # Filing URL
            filing_url = columns[1].find('a')['href']       
            
            # Filing Date
            extracted_date = []
            filing_date = columns[3].text.strip()       # (FILING DATE 열의 인덱스는 3)
            filing_date = filing_date.replace("-", "") 
            extracted_date.append(filing_date)
            
            url_dict[filing_date] = ('https://www.sec.gov' + filing_url)
        url_dict = {key: value for key, value in url_dict.items() if key > start_date}  # 20130801부터 xml 제공
        display(url_dict)
        return url_dict
    
    def get_holdings_data(self, url_dict):
        holdings_data = pd.DataFrame()

        for i in tqdm(range(len(url_dict))):
            current_date = list(url_dict.keys())[i]
            response_holdings = self.get_request(url_dict[current_date])
            soup_holdings = BeautifulSoup(response_holdings.text, "html.parser")
            
            # Filing Date
            filing_date_label = soup_holdings.find('div', class_='infoHead', string='Filing Date')
            filing_date_value = filing_date_label.find_next('div', class_='info').text

            # Period of Report
            period_of_report_label = soup_holdings.find('div', class_='infoHead', string='Period of Report')
            period_of_report_value = period_of_report_label.find_next('div', class_='info').text

            # xml url
            tags_holdings = soup_holdings.findAll('a', attrs={'href': re.compile('xml')})
            xml_url = tags_holdings[3].get('href')
            response_xml = self.get_request('https://www.sec.gov' + xml_url)
            soup_xml = BeautifulSoup(response_xml.content, "lxml")
            
            columns = [
                        "Name of Issuer",
                        "CUSIP",
                        "Value",
                        "Shares",
                        "Investment Discretion",
                        "Voting Sole / Shared / None"
                    ]

            df = pd.DataFrame(columns= columns)

            issuers = soup_xml.body.findAll(re.compile('nameofissuer'))
            cusips = soup_xml.body.findAll(re.compile('cusip'))
            values = soup_xml.body.findAll(re.compile('value'))
            sshprnamts = soup_xml.body.findAll('sshprnamt')
            sshprnamttypes = soup_xml.body.findAll(re.compile('sshprnamttype'))
            investmentdiscretions = soup_xml.body.findAll(re.compile('investmentdiscretion'))
            soles = soup_xml.body.findAll(re.compile('sole'))
            shareds = soup_xml.body.findAll(re.compile('shared'))
            nones = soup_xml.body.findAll(re.compile('none'))
            
            for issuer, cusip, value, sshprnamt, sshprnamttype, investmentdiscretion, sole, shared, none in zip(issuers, cusips, values, sshprnamts, sshprnamttypes, investmentdiscretions, soles, shareds, nones):
                row = {
                        "Name of Issuer": issuer.text,
                        "CUSIP": cusip.text,
                        "Value": value.text,
                        "Shares": f"{sshprnamt.text} {sshprnamttype.text}",
                        "Investment Discretion": investmentdiscretion.text,
                        "Voting Sole / Shared / None": f"{sole.text} / {shared.text} / {none.text}"
                    }
                df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
            
            df['Value'] = pd.to_numeric(df['Value'])
            df['Total Value'] = df['Value'].sum()
            df['Shares'] = df['Shares'].str[:-3]
            df['Shares'] = pd.to_numeric(df['Shares'])
            df['Weight(%)'] = df['Value'] / df['Value'].sum() * 100
            df['Filing Date'] = filing_date_value
            df['Period of Report'] = period_of_report_value
            
            # Period of Report '2022-09-30' 이전은 (x$1000) 단위 사용, 연속성을 위해 단위 통일
            if df['Period of Report'][0] <= '2022-09-30':
                df['Value'] = df['Value'] * 1000
                df['Total Value'] = df['Total Value'] * 1000
                
            df = df.groupby(['Name of Issuer', 'CUSIP', 'Total Value', 'Filing Date', 'Period of Report']).agg({'Value': 'sum', 'Shares': 'sum', 'Weight(%)': 'sum'}).reset_index()
            df = df.sort_values(by='Weight(%)', ascending=False).reset_index(drop=True)
            holdings_data = pd.concat([holdings_data, df], axis=0)
        return holdings_data   
    
    def get_holdings_change(self, holdings_data):
        change_data = pd.DataFrame()
        sorted_list_asc = sorted(list(url_dict.keys()), reverse=False)
        formatted_list_asc = [datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d") for date in sorted_list_asc]

        for i in range(len(formatted_list_asc)):

            if i == 0:   
                continue # 첫 데이터셋의 경우 이전 구성종목 데이터와 비교 불가능하여 skip
            else:
                previous_date = formatted_list_asc[i-1]
                current_date = formatted_list_asc[i]   
                
                previous_dataset = holdings_data[holdings_data['Filing Date'] == previous_date].rename(columns=lambda x: x + '_Previous')
                current_dataset = holdings_data[holdings_data['Filing Date'] == current_date]
                merged_dataset = pd.merge(current_dataset, previous_dataset, left_on='CUSIP', right_on='CUSIP_Previous', how='outer')
                
                merged_dataset['Transaction'] = merged_dataset.apply(lambda row: 'Inclusion' if pd.isna(row['CUSIP_Previous']) else ('Exclusion' if pd.isna(row['CUSIP']) else ''), axis=1) # 종목 편출입 체크
                merged_dataset['Name of Issuer'].fillna(merged_dataset['Name of Issuer_Previous'], inplace=True)                                                                            # 종목 편출 시 NaN 값 처리 (종목명)
                merged_dataset['CUSIP'].fillna(merged_dataset['CUSIP_Previous'], inplace=True)                                                                                              # 종목 편출 시 NaN 값 처리 (종목코드)
                merged_dataset['Total Value'].fillna(merged_dataset['Total Value'][0], inplace=True)                                                                                        # 종목 편출 시 NaN 값 처리 (총 자산가치)
                merged_dataset['Filing Date'].fillna(merged_dataset['Filing Date'][0], inplace=True)                                                                                        # 종목 편출 시 NaN 값 처리 (공시일자)
                merged_dataset['Period of Report'].fillna(merged_dataset['Period of Report'][0], inplace=True)                                                                              # 종목 편출 시 NaN 값 처리 (기준일자)
                merged_dataset.update(merged_dataset[['Shares', 'Shares_Previous', 'Value', 'Value_Previous', 'Weight(%)', 'Weight(%)_Previous']].fillna(0))                                # 종목 편입 시 NaN 값 처리 
                
                merged_dataset['Shares_Change'] = merged_dataset['Shares'] - merged_dataset['Shares_Previous'] 
                merged_dataset['Weight_Change(%)'] = merged_dataset['Weight(%)'] - merged_dataset['Weight(%)_Previous'] 
                merged_dataset['Absolute_Weight_Change(%)'] = abs(merged_dataset['Weight_Change(%)'])
                
                merged_dataset = merged_dataset[['Filing Date', 'Filing Date_Previous', 'Period of Report', 'Period of Report_Previous',\
                                                    'Total Value', 'Name of Issuer', 'CUSIP', 'Value',\
                                                    'Shares', 'Shares_Previous', 'Shares_Change', 'Weight(%)', 'Weight(%)_Previous', 'Weight_Change(%)', 'Absolute_Weight_Change(%)', 'Transaction']]
                merged_dataset = merged_dataset.sort_values(by=['Transaction', 'Weight(%)'], ascending=[False, False])
                change_data = pd.concat([change_data, merged_dataset], axis=0).reset_index(drop=True)
        holdings_change = change_data.copy()
        return holdings_change
    
    def get_all_cusips(self, holdings_change):
        cusips = holdings_change['CUSIP'].drop_duplicates().to_list()
        return cusips

    def get_ticker_merged(self, cusips, holdings_change):
        
        def format_response(response):
            if "data" in response and len(response["data"]) != 0:
                match = response["data"][0]
                return match["ticker"], match["name"], match["securityType"]
            return "", "", ""

        def cusips_to_tickers(cusips):
            query = [{"idType": "ID_CUSIP", "idValue": cusip} for cusip in cusips]
            api_endpoint = "https://api.openfigi.com/v3/mapping"
            headers = {"X-OPENFIGI-APIKEY": "c5f6f03d-72fa-4c28-a99e-9d8a56ef2c13"}
            
            response = requests.post(api_endpoint, json=query, headers=headers)
            matches = response.json()
            tmp = [format_response(match) for match in matches]
            return [(tpl[0],) + tpl[1] for tpl in list(zip(cusips, tmp))]

        start = 0
        stop = 100
        
        cusips_map = []
        while start < len(cusips):
            if stop <= len(cusips):
                cusip_batch = cusips[start:stop]
            else:
                cusip_batch = cusips[start:]

            cusips_map += cusips_to_tickers(cusip_batch)
            
            start = start + 100
            stop = stop + 100

            if start >= len(cusips):
                break
        
        cusips_map = pd.DataFrame(cusips_map)
        cusips_map.rename(columns={cusips_map.columns[0]: 'CUSIP'}, inplace=True)
        cusips_map.rename(columns={cusips_map.columns[1]: 'TICKER'}, inplace=True)
        cusips_map.rename(columns={cusips_map.columns[2]: 'Name'}, inplace=True)
        cusips_map.rename(columns={cusips_map.columns[3]: 'Type'}, inplace=True)
        
        # 별도처리 (BERKSHIRE HATHAWAY, CHEVRON )
        cusips_map['TICKER'] = cusips_map['TICKER'].str.replace('/', '.')
        cusips_map.loc[cusips_map['TICKER'] == 'CHV', 'TICKER'] = 'CVX'
        
        ticker_mapped_data = pd.merge(holdings_change, cusips_map[['CUSIP', 'TICKER', 'Type']], how='left', on='CUSIP')
        return cusips_map, ticker_mapped_data

    def get_sector_info(self, ticker_mapped_data, file_path):
        sector_info = pd.read_excel(file_path)
        sector_mapped_data = pd.merge(ticker_mapped_data, sector_info[['TICKER', 'FREF_LISTING_EXCHANGE', 'SECTOR_NAME_BIG', 'SECTOR_NAME_MID']], how='left', on='TICKER')
        
        grouped_data = sector_mapped_data.groupby(['Period of Report', 'SECTOR_NAME_BIG'])['Weight(%)'].sum().reset_index()
        period_of_report = grouped_data['Period of Report'].drop_duplicates().reset_index(drop=True)
        
        full_data = pd.DataFrame()
        for i in range(len(period_of_report)):
            current_date = period_of_report[i]
            subset = grouped_data[grouped_data['Period of Report'] == current_date].set_index('SECTOR_NAME_BIG')
            subset.loc['Sum'] = subset['Weight(%)'].sum()
            subset = subset.rename(columns={'Weight(%)' : current_date})[[current_date]].reset_index()
            
            if i == 0:
                full_data = subset.copy()
            else:
                full_data = pd.merge(full_data, subset, how='left', on='SECTOR_NAME_BIG')
        
        sector_weight_data = full_data.set_index('SECTOR_NAME_BIG').T
        return sector_mapped_data, sector_weight_data

    def analytics(self, sector_mapped_data, sector_weight_data):
        
        # AUM & 구성종목 개수
        df = sector_mapped_data.groupby('Period of Report').agg({'Value': ['sum', 'count']})
        df.index = pd.to_datetime(df.index)
        
        fig, ax = plt.subplots(figsize=(20, 15))
        ax.plot(df['Value']['sum'])
        
        ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, loc: "{:,}".format(int(x)))) # y축의 숫자 형식을 정수로 변경하고 천 단위로 콤마로 구분
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=12))                                # x축의 날짜 축을 6개월 단위로 설정
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))                              # x축 날짜 형태 YYYY-MM-DD로 설정
        display(df)
        plt.show()
        
        # 비중 변화 TOP 10
        # weight_change = sector_mapped_data.sort_values(by=['Period of Report', 'Absolute_Weight_Change(%)'], ascending=[False, True])
        
        
        
requested_cik = input("Enter 10-digit CIK number : ")
SEC_13F_HR = sec_13f_hr(requested_cik, start_date='20130801', file_path='C:/Users/shd4323/Desktop/SEC_13F_HR/sector_info.xlsx')

# %%
url_dict = SEC_13F_HR.url_dict
holdings_data = SEC_13F_HR.holdings_data
holdings_change = SEC_13F_HR.holdings_change
cusips = SEC_13F_HR.cusips
cusips_map = SEC_13F_HR.cusips_map
ticker_mapped_data = SEC_13F_HR.ticker_mapped_data
sector_mapped_data = SEC_13F_HR.sector_mapped_data
sector_weight_data = SEC_13F_HR.sector_weight_data