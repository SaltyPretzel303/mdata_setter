from os import _exit as exit, path, getenv
import json
from typing import Callable
from requests import post

INPUTS_CONFIG_PATH = "/valohai/config/inputs.json"
FILTER_FILE = "filter.py"
MDATA_GEN_FILE = "gen_mdata.py"
TOKEN_ENV_VAR = "VH_TOKEN"
DATUMS_PER_REQUEST = 1000
HOST = "https://app.valohai.com"

def exit_with(msg, code=1): 
    print(f"Exiting because: {msg}.")
    print("Bye")
    exit(code)

class Datum: 
    def __init__(self, raw: dict, input: str):
        self.raw = raw
        self.from_input = input

    @property
    def name(self):
        return self.raw["name"]
    
    @property
    def id(self):
        return self.raw.get("datum_id", None)
    
    @property
    def mdata(self): 
        return self.raw.get("metadata", None)

def resolve_filter() -> Callable[[dict], bool]:
    if path.exists(FILTER_FILE):
        from filter import filter

        return filter
    
    print("No filter provided, will process all datums.")
    return lambda x: True

def resolve_datums(filter: Callable[[dict], bool]) -> list[Datum]:
    if not path.exists(INPUTS_CONFIG_PATH):
        exit_with(f"Missing: {INPUTS_CONFIG_PATH}")

    with open(INPUTS_CONFIG_PATH, "r") as f:
        content = json.load(f)

    datums = []

    full_filter = lambda x: isinstance(x, dict) and "datum_id" in x and filter(x)

    for input in content.keys():
        datums += [Datum(file, input) for file in content[input]["files"] if full_filter(file)]

    return datums

def resolve_gen_mdata() -> Callable[[Datum], dict|None]:
    if path.exists(MDATA_GEN_FILE):
        from gen_mdata import gen_mdata
        return gen_mdata

    return lambda x: None

def iter_slice(arr, width): 
    for i in range(0, len(arr), width): 
        yield arr[i:i+width]

def apply_metadata(datums: list[Datum], resolve_mdata: Callable[[Datum], dict|None], token: str, host: str):

    for datum_slice in iter_slice(datums, DATUMS_PER_REQUEST): 
        metadata = {}

        for datum in datum_slice: 
            # if new_metadata := resolve_mdata(datum):
            #     metadata[datum.id] = new_metadata
            metadata[datum.id] = resolve_mdata(datum)

        if not metadata: 
            exit_with("No metadata resolved - nothing to apply!")

        header = {"Authorization": f"Token {token}"}
        try:
            res = post(get_apply_url(host), headers=header, json={"datum_metadata": metadata})
            print(f"{len(metadata)} metadata applied")
        except Exception as e:
            exit_with(f"Exception while applying metadata: {e}")
            return 
            # exit will close the app, return is here just so that the next if don't complain 
            # about res being unbound

        if not res.ok:
            txt = res.text or "Unknown reason"
            exit_with(f"Failed to apply metadata: {txt}")

def get_token() -> str | None:
    return getenv(TOKEN_ENV_VAR)

def get_apply_url(host: str) -> str:
    return f"{host}/api/v0/data/metadata/apply/"

if __name__ == "__main__": 
    print("Will add some metadata ...")

    token = get_token()
    if not token: 
        exit_with(f"Failed to read token from {TOKEN_ENV_VAR} env. variable")
        
    datum_filter = resolve_filter()
    datums = resolve_datums(datum_filter)

    gen_mdata = resolve_gen_mdata()
    apply_metadata(datums, gen_mdata, token, HOST)

    print("Leaving.")