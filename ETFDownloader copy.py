import yfinance as yf
import pandas as pd
import numpy as np



code_list = [
    "00942B", "00937B", "00933B", "00931B", "00890B", "00884B", "00883B", "00870B", "00867B", 
    "00864B", "00863B", "00862B", "00860B", "00859B", "00857B", "00856B", "00853B", "00849B", 
    "00848B", "00847B", "00844B", "00846B", "00845B", "00842B", "00841B", "00840B", "00836B", 
    "00834B", "00799B", "00794B", "00831B", "00795B", "00793B", "00792B", "00788B", "00787B", 
    "00786B", "00785B", "00784B", "00791B", "00790B", "00789B", "00782B", "00781B", "00780B", 
    "00779B", "00778B", "00777B", "00773B", "00772B", "00768B", "00764B", "00761B", "00760B", 
    "00759B", "00758B", "00756B", "00755B", "00754B", "00751B", "00750B", "00749B", "00746B", 
    "00741B", "00740B", "00734B", "00718B", "00727B", "00726B", "00725B", "00721B", "00720B", 
    "00719B", "00724B", "00723B", "00722B", "00697B", "00696B", "00695B", "00694B", "00687B", 
    "00679B"
]

code_list = [code + ".TWO" for code in code_list]
print(code_list)

stock_data = yf.download(code_list, start="2023-05-01", end="2024-05-14", progress=False)
print(stock_data)

stock_data = stock_data["Close"]
stock_data = np.log(stock_data / stock_data.shift(1))




statistics = stock_data.describe()
statistics = statistics.T

statistics.to_excel("statistics.xlsx")
print(statistics)

