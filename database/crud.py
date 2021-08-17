from bson import ObjectId
import json

import pandas as pd

from .base import db, db_pair, exists_item, get_itemid, exists_item_pair
from .models import Symbol, Pair, pair_data

def set_symbol(name: str) -> bool:
    try:
        if not exists_item("symbols", "name", name):
            symbol = Symbol({"name": name})
            symbol.save()
        return True
    except Exception as e:
        print("SET SYMBOL : ", e)
        return False

def add_pairs_to_symbol(symbol_name: str, pairs: list):
    try:
        symbol = get_itemid("symbols", "name", symbol_name)
        for pair in pairs:
            pair = pair_data(pair)
            pair["symbol_id"] = symbol
            new_par = Pair(pair)
            new_par.save()
    except Exception as e:
        print(f"{symbol_name} | Add pairs to symbols: {e}")
    return []

def get_data_pair(pair_name: str, interval: str):
    try:
        query = db_pair[f"{pair_name}_{interval}"].find().count()
        if int(query) > 0:
            return True
    except Exception as e:
        print("Error")
        pass
    return False

def get_last_data_pair(pair_name: str, interval: str):
    try:
        query = db_pair[f"{pair_name}_{interval}"].find().sort({'_id':-1}).limit(1)
        print(query)
    except Exception as e:
        print("Errir get last data pair", e)

def set_invalid_pair(pair_name: str, interval: str):
    try:
        pairinfo = exists_item("pairs", "name", pair_name)
        if not pairinfo:
            print("El par", pair_name, " no existe...")
        else:
            pair = Pair({"_id": pairinfo})
            pair.reload()
            pair[f"is_valid_{interval}"] = False
            pair.save()
            print(f"{pair_name} - {interval} | Invalidado con éxito.")
    except Exception as e:
        print(f"{pair_name}-{interval} | Set ivalid pair: {e}")

def save_historic(pair_data: pd.DataFrame, pair: str, interval: str):
    try:
        records = json.loads(pair_data.to_json(orient="records"))
        db_pair[f"{pair}_{interval}"].insert_many(records)
    except Exception as e:
        print(f"{pair}-{interval} | Save Historic: {e}")