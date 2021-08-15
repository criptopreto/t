from pymongo import collection
from .base import db
from model.pymongo_model import SimpleModel

class Pair(SimpleModel):
    collection = db.pairs

def pair_data(name: str)-> dict:
    return {"name": name, "is_valid_15m": True, "is_valid_30m": True, "is_valid_1h": True, "is_valid_4h": True, "is_valid_12h": True, "is_valid_1d": True}

class Symbol(SimpleModel):
    collection = db.symbols