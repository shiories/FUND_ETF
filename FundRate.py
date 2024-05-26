import re
import time
import aiohttp
import asyncio
import warnings
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime, timedelta




class FundRate:

    def __init__(self, company=""):
        warnings.filterwarnings("ignore", message="Unverified HTTPS request")
        self.company = company

        self.rate_url = "https://www.sitca.org.tw/ROC/Industry/IN2213.aspx?pid=IN2222_03"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
            "Content-Type": "application/x-www-form-urlencoded"
        }


    async def get_post(self, url, post_dict=None):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=self.headers, data=post_dict, verify_ssl=False) as response:
                    html = await response.text()

                soup = BeautifulSoup(html, 'html.parser')

                form = soup.find('form', {'id': 'aspnetForm'})
                input_fields = form.find_all('input')

                if post_dict is None:
                    post_dict = {}

                for input_field in input_fields:
                    if input_field.get('name') and input_field.get('value'):
                        post_dict[input_field['name']] = input_field['value']
            return post_dict
        except:
            print("取得不到請求碼")
            return None


    async def fetch_data(self, session, date_str):
        self.post_dict['ctl00$ContentPlaceHolder1$ddlQ_YM'] = date_str
        try:
            async with session.post(self.rate_url, headers=self.headers, data=self.post_dict, verify_ssl=False) as response:
                response_text = await response.text()
                print(f'{date_str}下載完畢')
                return response_text
            
        except Exception as e:
            self.post_dict = await self.get_post(self.rate_url)
            if not self.post_dict:
                return None
            self.post_dict['ctl00$ContentPlaceHolder1$ddlQ_YM'] = date_str
            try:
                async with session.post(self.rate_url, headers=self.headers, data=self.post_dict, verify_ssl=False) as response:
                    response_text = await response.text()
                    print(f'{date_str}再次嘗試下載')
                    return response_text
            except Exception as e:
                print(f"無法取得 {date_str} 的資料：{str(e)}")
                return None


    async def download_data(self, session, date):
        response_text = await self.fetch_data(session, date)
        if response_text:
            soup = BeautifulSoup(response_text, "html.parser")
            return await self.parse_data(soup)  # 直接返回異步處理對象
        else:
            return pd.DataFrame()


    async def range_main(self, date_df, headers):
        self.post_dict = await self.get_post(self.rate_url)
        async with aiohttp.ClientSession(headers=headers) as session:
            semaphore = asyncio.Semaphore(len(date_df))  
            tasks = [self.download_data_with_semaphore(session, date, semaphore) for date in date_df]
            results = await asyncio.gather(*tasks)
            result_df = pd.DataFrame()
            for date, result in zip(date_df, results):
                result_df = await self.process_result(date, result_df, result)
        return result_df


    async def download_data_with_semaphore(self, session, date, semaphore):
        async with semaphore:
            return await self.download_data(session, date)

        
    async def parse_data(self, soup):
        fund_dict = {'基金統編': [], '除息日': [], '股利率': []}
        table = soup.find('table')
        if table:
            for row in table.find_all('tr')[2:]:
                columns = row.find_all('td')
                if len(columns) >= 7:
                    fund_dict['基金統編'].append(columns[1].text.strip())  
                    fund_dict['除息日'].append(columns[5].text.strip())     
                    fund_dict['股利率'].append(columns[7].text.strip())     
        df = pd.DataFrame(fund_dict)
        df = df.iloc[2:]
        return df
            

    async def process_result(self, date, result_df, result):
        if result.empty:
            return
        else:
            result.rename(columns={'除息日': f"{date}_date"}, inplace=True)
            result.rename(columns={'股利率': f"{date}_rate"}, inplace=True)
            
        if result_df.empty:
            result_df = result
        else:
            result_df = result_df.merge(result, how='left', on='基金統編')
            
        print(f'result_df: \n{result_df.info()}')
        return result_df
        
        
    def run(self, start_date, end_date):
        self.post_dict = asyncio.run(self.get_post(self.rate_url))
        # 生成月份範圍
        date_df = pd.date_range(start_date, end_date, freq='MS').strftime("%Y%m")
        print(f'總請求筆數: {len(date_df)}筆')
        print(f'date_df: \n{date_df}筆')

        result_df = asyncio.run(self.range_main(date_df, self.headers))
        result_df.to_excel(f'test_df.xlsx')
        return result_df
        
        
        
if __name__ == "__main__":

    company = ""
    fond_rate = FundRate(company)

    start_date = "20230520"
    end_date = "20240420"
    result_df = fond_rate.run(start_date, end_date)
    print(result_df.info())







