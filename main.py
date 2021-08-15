import configparser
import time

from binance.client import Client

from helpers.functions import read_symbols, read_intervals, get_historic
from database.crud import set_symbol, add_pairs_to_symbol
# Variables
data_general = []

def log(msg: str):
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} | {msg}")

# Configuracion
config = configparser.ConfigParser()
config.read_file(open("secret.cfg"))
api_key = config.get('BINANCE', "ACTUAL_API_KEY")
secret_key = config.get("BINANCE", "ACTUAL_SECRET_KEY")

print("### POWER OSCILLATOR SCANNER ###")
print("######### VERSIÓN 0.0.1 ########")
print("### INICIALIZANDO EL SISTEMA ###")

symbols = read_symbols()

# Vamos a leer los pares para cada symbols

if len(symbols) == 0:
    print("Por favor agregue los symbols al archivo symbols.txt")
    exit()

log("Agregando los symbols a la base de datos...")
for symbol in symbols:
    set_symbol(symbol)
log("Listo")

log("Leyendo pares desde binance")
client = Client(api_key, secret_key)
pairs_df = client.get_all_tickers()
log("Listo")

log("Leyendo pares para cada una de los symbols")
for symbol in symbols:
    data_smb = {}
    data_smb["symbol"] = symbol
    data_smb["pairs"] = [x for x in (pairs_df[y]["symbol"] for y in range(
                len(pairs_df))) if x[-len(symbol):] == symbol]
    log(f"{symbol} | {len(data_smb['pairs'])} Pares validos.")
    data_general.append(data_smb)
log("Listo")

log("Leyendo la configuración de los intervals...")
intervals = read_intervals()
log("Listo.")

log("Guardar la lista de los pares en la base de datos")
for pair_symbol in data_general:
    add_pairs_to_symbol(pair_symbol["symbol"], pair_symbol["pairs"])
log("Listo.")

log("Extraer data para cada par en cada interval")
for interval in intervals:
    print(f"Interval: {interval} | {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
    for data in data_general:
        print(f"symbol: {data['symbol']} | {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
        for pair in data["pairs"]:
            get_historic(pair, interval, client)
log("Listo")
log(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))

log("Finalizado")