from pathlib import Path
import os
import re
import time

import pandas as pd
from binance.client import Client
from database.crud import set_invalid_pair


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

def get_historic(symbol: str, window: str, client: Client, is_thread: bool = False):
    par_filename = f"data/pares_ohlc/{symbol}_{window}.csv"
    # Buscamos el archivo localmente para verificar si tenemos información guardada
    if not Path(par_filename).exists():
        try:
            time_start = int(window[0:1] if len(
                window) == 2 else window[0:2]) * 1100
            delta_start = str(time_start) + get_definition(window)
            start_str = str((pd.to_datetime(
                "today") - pd.Timedelta(delta_start)).date())

            data = pd.DataFrame(client.get_historical_klines(symbol=symbol, start_str=start_str, interval=window),
                                columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'closetime',
                                         'quoteAssetVolume', 'numberOfTrades', 'takerBuyBaseVol', 'takerBuyQuoteVol',
                                         'ignore'])

            if len(data.index) < 480:
                print(f"{symbol} | Menor de 480 registros.")
                set_invalid_pair(symbol, window)
            else:
                data.datetime = pd.to_datetime(data.datetime, unit="ms")
                data.closetime = pd.to_datetime(data.closetime, unit="ms")

                data[["open", "high", "low", "close", "volume"]] = data[["open", "high", "low", "close", "volume"
                                                                        ]].apply(pd.to_numeric)
                data = data[['datetime', 'open', 'high',
                            'low', 'close', 'volume', 'closetime']]

                data.to_csv(par_filename, index=False)
        except Exception as e:
            print(f"{symbol} | Error get Historic: {e}")
            print("Invalidando par")
            set_invalid_pair(symbol, window)
    else:
        if not os.stat(par_filename).st_size == 0:
            par_df = pd.read_csv(par_filename)
            # Obtenemos el último registro
            try:
                last_reg = pd.to_datetime(par_df.iloc[-1]["datetime"]) - pd.Timedelta("4 Hours")

                # Verificamos si ha pasado tiempo válido entre el último registro y ahora.

                interval_coeficient = get_time_coeficient(window)

                if float(pd.Timedelta(pd.to_datetime("today") - last_reg).value / 3600000000000) >= interval_coeficient:
                    last_reg = str(last_reg + pd.Timedelta(str("4 Hours")))

                    data = pd.DataFrame(
                        client.get_historical_klines(symbol=symbol, start_str=last_reg, interval=window),
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

                    # Eliminar el último registro del archivo guardado para reemplazarlo por el registro actualizado
                    par_df.drop(par_df.tail(1).index, inplace=True)

                    par_df = par_df.append(data, ignore_index=True)

                    par_df.to_csv(par_filename, index=False)
                    # gen_squeeze(par_df, symbol, interval)
                else:
                    par_df[["open", "high", "low", "close", "volume"]] = par_df[
                        ["open", "high", "low", "close", "volume"
                         ]].apply(pd.to_numeric)

            except IndexError:
                print(f"{symbol} | Error get Historic: {IndexError}")
                print("Invalidando par")
                set_invalid_pair(symbol, window)