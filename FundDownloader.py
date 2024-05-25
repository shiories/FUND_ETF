import re
import time
import aiohttp
import asyncio
import warnings
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime, timedelta


class FundDownloader:

    def __init__(self, company=""):
        warnings.filterwarnings("ignore", message="Unverified HTTPS request")
        self.company = company

        self.data_url = "https://www.sitca.org.tw/ROC/Industry/IN2106.aspx?pid=IN2213_02"
        self.basic_url = "https://www.sitca.org.tw/ROC/Industry/IN2105.aspx?pid=IN2212_02"

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        self.fund_types = {
            'AA1': {'範圍': '國內', '配置': '股票', '標的': '股票'},
            # 跨國投資股票型
            'AA2': {'範圍': '跨國', '配置': '股票', '標的': '股票'},
            # 國內投資平衡型
            'AB1': {'範圍': '國內', '配置': '平衡', '標的': '股票'},
            # 跨國投資平衡型
            'AB2': {'範圍': '跨國', '配置': '平衡', '標的': '股票'},
            # 國內投資固定收益一般債券型
            'AC12': {'範圍': '國內', '配置': '固定收益一般債券', '標的': '債券'},
            # 跨國投資固定收益一般債券型
            'AC21': {'範圍': '跨國', '配置': '固定收益一般債券', '標的': '債券'},
            # 金融資產證券化型
            'AC22': {'範圍': '', '配置': '金融資產證券化', '標的': '金融資產'},
            # 非投資等級債券型
            'AC23': {'範圍': '', '配置': '非投資等級債券', '標的': '債券'},
            # 國內投資貨幣市場基金
            'AD1': {'範圍': '國內', '配置': '貨幣市場', '標的': '貨幣資產'},
            # 跨國投資貨幣市場基金
            'AD2': {'範圍': '跨國', '配置': '貨幣市場', '標的': '貨幣資產'},
            # 國內投資組合型
            'AE1': {'範圍': '國內', '配置': '組合', '標的': ''},
            # 跨國投資組合型_股票型
            'AE21': {'範圍': '跨國', '配置': '組合', '標的': '股票'},
            # 跨國投資組合型_債券型
            'AE22': {'範圍': '跨國', '配置': '組合', '標的': '債券'},
            # 跨國投資組合型_平衡型
            'AE23': {'範圍': '跨國', '配置': '組合', '標的': '股票'},
            # 跨國投資組合型_其他
            'AE24': {'範圍': '跨國', '配置': '組合', '標的': '其他'},
            # 保本型
            'AF': {'範圍': '', '配置': '保本', '標的': ''},
            # 不動產證券化型
            'AG': {'範圍': '', '配置': '證券', '標的': '不動產'},
            # 國內投資指數股票型_股票型
            'AH11': {'範圍': '國內', '配置': '指數股票', '標的': '股票'},
            # 國內投資指數股票型_債券型
            'AH12': {'範圍': '國內', '配置': '指數股票', '標的': '債券'},
            # 國內投資指數股票型_槓桿型/反向型
            'AH13': {'範圍': '國內', '配置': '指數股票', '標的': '股票'},
            # 國內投資指數股票型_其他
            'AH14': {'範圍': '國內', '配置': '指數股票', '標的': '其他'},
            # 跨國投資指數股票型_股票型
            'AH21': {'範圍': '跨國', '配置': '指數股票', '標的': '股票'},
            # 跨國投資指數股票型_債券型
            'AH22': {'範圍': '跨國', '配置': '指數股票', '標的': '債券'},
            # 跨國投資指數股票型_槓桿型/反向型_股票
            'AH23': {'範圍': '跨國', '配置': '指數股票', '標的': '股票'},
            # 跨國投資指數股票型_槓桿型/反向型_債券
            'AH24': {'範圍': '跨國', '配置': '指數股票', '標的': '債券'},
            # 跨國投資指數股票型_不動產證券化/其他
            'AH25': {'範圍': '跨國', '配置': '指數股票', '標的': '不動產'},
            # 國內投資指數型
            'AI1': {'範圍': '國內', '配置': '指數', '標的': '指數'},
            # 跨國投資指數型
            'AI2': {'範圍': '跨國', '配置': '指數', '標的': '指數'},
            # 國內投資多重資產型
            'AJ1': {'範圍': '國內', '配置': '多重資產', '標的': '資產'},
            # 跨國投資多重資產型
            'AJ2': {'範圍': '跨國', '配置': '多重資產', '標的': '資產'},
            # 國內ETF連結基金
            'AK1': {'範圍': '國內', '配置': 'ETF', '標的': 'ETF'},
            # 類貨幣市場型
            'AC11': {'範圍': '', '配置': '貨幣市場', '標的': '貨幣資產'},
            # 私募基金
            'B': {'範圍': '', '配置': '基金', '標的': ''}
        }
        self.cache = {}  # 異步緩存


    async def fetch_data(self, session, date_str):
        self.post_dict['ctl00$ContentPlaceHolder1$txtQ_Date'] = date_str

        try:
            async with session.post(self.data_url, headers=self.headers, data=self.post_dict, verify_ssl=False) as response:
                response_text = await response.text(encoding='ISO-8859-1')
                print(f'{date_str}下載完畢')
                return response_text
            
        except Exception as e:
            self.post_dict = await self.get_post(self.data_url)
            if not self.post_dict:
                return None
            self.post_dict['ctl00$ContentPlaceHolder1$txtQ_Date'] = date_str
            try:
                async with session.post(self.data_url, headers=self.headers, data=self.post_dict, verify_ssl=False) as response:
                    response_text = await response.text(encoding='ISO-8859-1')
                    print(f'{date_str}再次嘗試下載')
                    return response_text
            except Exception as e:
                print(f"無法取得 {date_str} 的資料：{str(e)}")
                return None


    async def download_data(self, session, date):
        date_str = date.strftime('%Y%m%d')
        response_text = await self.fetch_data(session, date_str)
        if response_text:
            soup = BeautifulSoup(response_text, "html.parser")
            return await self.parse_data(soup)  # 直接返回異步處理對象
        else:
            return pd.DataFrame()


    async def parse_data(self, soup):
        fund_dict = {'基金統編': [], '漲跌': []}
        table = soup.find('table')
        if table:
            for row in table.find_all('tr')[2:]:
                columns = row.find_all('td')
                if len(columns) >= 10:
                    fund_dict['基金統編'].append(columns[4].text.strip())  
                    fund_dict['漲跌'].append(columns[9].text.strip())     

        df = pd.DataFrame(fund_dict)
        return df


    async def process_result(self, date, result_df, result):
        date_time = date.strftime('%Y-%m-%d')

        if result_df.empty:
            result_df = await self.download_basic()
            result_df['類型代號'] = result_df['類型代號'].map(self.fund_types)
            type_info = result_df['類型代號'].apply(pd.Series)
            result_df = pd.concat([result_df, type_info], axis=1).drop(columns=['類型代號'])
            keywords = ["新興市場", "全球", "亞洲", "中國", "亞太", "美國", "新加坡", "北美"]
            for keyword in keywords:
                result_df.loc[result_df["基金名稱"].str.contains(keyword), "範圍"] = keyword
            result_df.loc[result_df["基金名稱"].str.contains("新興邊境|新興"), "範圍"] = "新興市場"
            result_df.loc[result_df["基金名稱"].str.contains("環球|全方位"), "範圍"] = "全球"
            result_df.loc[result_df["基金名稱"].str.contains("美利堅"), "範圍"] = "美國"
            result_df["範圍"] = result_df["範圍"].fillna("全球")
                
        if not result.empty:
            result_df = result_df.merge(result[['基金統編', '漲跌']], how='left', on='基金統編')
            result_df.rename(columns={'漲跌': date_time}, inplace=True)
            
        return result_df


    async def download_basic(self):
        basic_post = {'ctl00$ContentPlaceHolder1$txtQ_Date': 202403,
                      "ctl00$ContentPlaceHolder1$ddlQ_Column": 1,
                      }
        basic_post = await self.get_post(self.basic_url)

        async with aiohttp.ClientSession() as session:
            async with session.post(self.basic_url, headers=self.headers, data=basic_post,
                                    verify_ssl=False) as response:
                response_text = await response.text()
        soup = BeautifulSoup(response_text, "html.parser")

        df_list = []
        table_rows = soup.find_all('tr')
        for row in table_rows:
            columns_data = row.find_all('td')
            if len(columns_data) >= 8:
                type_code = columns_data[0].text.strip()
                fund_code = columns_data[1].text.strip()
                fund_name = columns_data[4].text.strip()
                risk_level = columns_data[7].text.strip()
                pricing_currency = columns_data[12].text.strip()
                if re.match("^[A-Za-z0-9]+$", fund_code):
                    match = re.search(r'RR\d', risk_level)
                    if match:
                        risk_level = match.group()
                    else:
                        risk_level = None
                    df_list.append({"類型代號": type_code, "基金統編": fund_code, "基金名稱": re.sub(r'\([^)]*\)', '', fund_name),
                                    "風險等級": risk_level, "計價幣別": pricing_currency
                                    })

        basic_df = pd.DataFrame(df_list)
        print(f'基本資料下載完畢')

        return basic_df


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


    async def range_main(self, date_df, headers):
        self.post_dict = await self.get_post(self.data_url)
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


    def get_statistics(self, result_df):
        # 複製日期字串的列
        date_columns = result_df.columns[7:]
        result_df[date_columns] = result_df[date_columns].apply(pd.to_numeric, errors='coerce')
        date_df_values = result_df[date_columns]

        # 計算平均值和標準差
        average_values = date_df_values.mean(axis=1)
        std_dev_values = date_df_values.std(axis=1)

        # 將平均值和標準差添加到 DataFrame 中
        stats_df = pd.DataFrame({
            '平均值': average_values,
            '標準差': std_dev_values
        })
        
        # 將原始 DataFrame 與新統計列合併
        result_df = pd.concat([result_df, stats_df], axis=1)
        
        # 重新排列列順序
        columns_order = ['基金統編', '基金名稱', '風險等級', '計價幣別', '範圍', '配置', '標的', '平均值', '標準差'] + date_columns.tolist()
        result_df = result_df[columns_order]
        
        return result_df


    def run_range(self, start_date, end_date, to_excel=False):
        
        date_df = pd.date_range(start_date, end_date, freq='B')
        print(f'總請求筆數: {len(date_df)}筆')
        
        result_df = asyncio.run(self.range_main(date_df, self.headers))
        result_df = self.get_statistics(result_df)
        
        if to_excel:
            result_df.to_excel(f'{start_date}-{end_date}基金資料.xlsx', index=False)
        return result_df


    def merge_df(self, result_df, new_data):
        merge_columns = ['基金統編', '基金名稱', '風險等級', '計價幣別', '範圍', '配置', '標的']
        result_df = pd.merge(result_df, new_data, on=merge_columns, how="outer")
        # 找到包含 "_x" 或 "_y" 的列标题
        columns_to_drop = [col for col in result_df.columns if '_x' in col or '_y' in col]
        # 删除这些列
        result_df.drop(columns=columns_to_drop, inplace=True)

        # 重新排列
        sorted_columns = merge_columns + sorted(set(result_df.columns) - set(merge_columns))

        result_df = result_df[sorted_columns]
        
        return result_df


    def missing_list(self, result_df, start_date, end_date):
        # 生成日期範圍
        date_list = pd.Series(pd.date_range(start_date, end_date, freq='B'))
        data_index = pd.Series(result_df.iloc[:, 9:].columns)
        data_index = pd.to_datetime(data_index, format="%Y-%m-%d")
        print(f'data_index:\n{data_index}')

        # 比较检查缺失的日期列
        missing_dates = date_list[~date_list.isin(data_index)]
        print(f'總請求筆數: {len(missing_dates)}筆')
        print(f"缺失日期:\n{missing_dates}")
    
        return missing_dates


    def missing_data(self, file_name, start_date, end_date):
        start_time = time.time()
        # 讀取 Excel 文件
        try:
            result_df = pd.read_excel(f'{file_name}.xlsx')
            result_df = result_df.drop(columns=["平均值","標準差"])
        except FileNotFoundError:
            print(f'找不到{file_name}.xlsx')
            return

        missing_dates = self.missing_list(result_df, start_date, end_date)
        
        if missing_dates.empty:
            print("所有日期都已經存在於 DataFrame 中。")
            return result_df

        new_data = asyncio.run(self.range_main(missing_dates, self.headers))
        
        result_df = self.merge_df(result_df, new_data)
        result_df = self.get_statistics(result_df)

        end_time = time.time()
        print(f'下載{start_date}-{end_date}基金耗時:{round(end_time - start_time, 4)}秒')

        # 保存到 Excel 文件
        result_df.to_excel(f'{start_date}-{end_date}基金資料.xlsx', index=False)
        print(f"數據已保存到 {file_name}.xlsx")

        return result_df






if __name__ == "__main__":

    company = ""
    fond_downloader = FundDownloader(company)


    start_date = "20230520"
    end_date = "20240520"
    #result_df = fond_downloader.run_range(start_date, end_date, to_excel=True)
    #print(result_df.info())


    file_name = f"{start_date}-{end_date}基金資料"
    result_df = fond_downloader.missing_data(file_name, start_date, end_date)
    print(result_df.info())





