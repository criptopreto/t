from pathlib import Path
import os
import re
import time

import pandas as pd
from binance.client import Client
from database.crud import set_invalid_pair, save_historic, get_data_pair, get_last_data_pair, get_pair_historic, update_historic


def read_symbols()-> list:
    data = []
    print("Leyendo archivo de symbols...")
    file_symbols = open("symbols.txt", "r")
    for linea in file_symbols:
        if linea[0] == "#":
            pass
        else:
            linea = linea.strip("\n")
            if len(linea) > 0:
                data.append(linea)
    
    print(f"Leidos {len(data)} symbols {data}")

    file_symbols.close()
    return data

def read_intervals()-> list:
    data = []
    print("Leyendo archivo de intervals...")
    file_intervals = open("intervals.txt", "r")
    for linea in file_intervals:
        if linea[0] == "#":
            pass
        else:
            linea = linea.strip("\n")
            if len(linea) > 0:
                data.append(linea)
    
    print(f"Leidos {len(data)} intervals {data}")

    file_intervals.close()
    return data

def get_definition(x: str) -> str:
    return ' hours' if x[-1:] == 'h' else (' minutes' if x[-1:] == "m" else (' days' if x[-1:] == "d" else " weeks"))

def generate_data(df: pd.DataFrame) -> list:
    df = df.groupby('symbol')
    result_data = []
    for name, group in df:
        result_data.append({"par": name, "chg": group.iloc[-1]["chg"],
                            "min": round(group.iloc[-1]["low"], 8), "max": round(group.iloc[-1]["high"], 8),
                            "vol": group.iloc[-1]["volume"], "color": group.iloc[-1][f"color"]})

    return result_data

def get_time_coeficient(interval: str) -> float:
    try:
        int_types = {
            "h": 1,
            "m": 0.0166666666666667,
            "d": 24,
        }
        type_interval = interval[-1:]
        let_number = re.sub("\D", "", interval)
        result = float(int_types[type_interval] * float(let_number))
        return result
    except Exception as e:
        return 0

def get_historic(pair: str, interval: str, client: Client):
    data_pair = get_data_pair(pair, interval)
    if not data_pair:
        try:
            time_start =  int(interval[0:1] if len(
                interval) == 2 else interval[0:2]) * 1100
            delta_start = str(time_start) + get_definition(interval)
            start_str = str((pd.to_datetime(
                "today") - pd.Timedelta(delta_start)).date())
            
            data = pd.DataFrame(client.get_historical_klines(symbol=pair, start_str=start_str, interval=interval),
                                columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'closetime',
                                         'quoteAssetVolume', 'numberOfTrades', 'takerBuyBaseVol', 'takerBuyQuoteVol',
                                         'ignore'])
            if len(data.index) < 480:
                print(f"{pair} | Menor de 480 registros.")
                set_invalid_pair(pair, interval)
            else:
                data.datetime = pd.to_datetime(data.datetime, unit="ms")
                data.closetime = pd.to_datetime(data.closetime, unit="ms")

                data[["open", "high", "low", "close", "volume"]] = data[["open", "high", "low", "close", "volume"
                                                                        ]].apply(pd.to_numeric)
                data = data[['datetime', 'open', 'high', 'low', 'close', 'volume', 'closetime']]
                
                save_historic(data, pair, interval)
        except Exception as e:
            print(f"{pair} | Error get Historic: {e}")
            print("Invalidando par")
            set_invalid_pair(pair, interval)
    else:
        try:
            last_reg = pd.to_datetime(get_last_data_pair(pair, interval), unit="ms") - pd.Timedelta("4 Hours")
            interval_coeficient = get_time_coeficient(interval)
            if float(pd.Timedelta(pd.to_datetime("today") - last_reg).value / 3600000000000) >= interval_coeficient:
                last_reg = str(last_reg + pd.Timedelta(str("4 Hours")))

                data = pd.DataFrame(
                            client.get_historical_klines(symbol=pair, start_str=last_reg, interval=interval),
                            columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'closetime',
                                    'quoteAssetVolume', 'numberOfTrades', 'takerBuyBaseVol',
                                    'takerBuyQuoteVol',
                                    'ignore'])

                data.datetime = pd.to_datetime(data.datetime, unit="ms")
                data.closetime = pd.to_datetime(data.closetime, unit="ms")
                data[["open", "high", "low", "close", "volume"]] = data[["open", "high", "low", "close", "volume"
                                                                            ]].apply(pd.to_numeric)

                data = data[["datetime", "open", "high",
                                "low", "close", "volume", "closetime"]][0:]
                data = data.iloc[1:,:]

                # Actualizar el registro
                update_historic(data, pair, interval)
        except Exception as e:
            print(f"{pair} | Error get Historic: {e}")
            print("Invalidando par")
            set_invalid_pair(pair, interval)
