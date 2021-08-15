from bson import ObjectId
import pandas as pd
from .base import db, exists_item, get_itemid
from .models import Symbol, Pair

def set_symbol(name: str) -> bool:
    try:
        if not exists_item("symbols", "name", name):
            symbol = Symbol({"name": name})
            symbol.save()
        return True
    except Exception as e:
        print("SET SYMBOL : ", e)
        return False

def add_pairs_to_symbol(symbol_name: str, pairs: list)->list:
    try:
        symbol = get_itemid("symbols", "name", symbol_name)
        for pair in pairs:
            pair["symbol_id"] = symbol
            new_par = Pair(pair)
            new_par.save()
    except Exception as e:
        print(f"{symbol_name} | Add pairs to symbols: {e}")
    return []