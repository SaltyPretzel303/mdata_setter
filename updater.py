from dataclasses import dataclass
from os import _exit as exit, path
from os import getenv
import json
from typing import Callable
from requests import post

INPUTS_CONFIG_PATH = "/valohai/config/inputs.json"
FILTER_FILE = "filter.py"
MDATA_GEN_FILE = "gen_mdata.py"
TOKEN_ENV_VAR = "VH_TOKEN"
HOST = "http://mayhai.com"

def exit_with(msg, code=1): 
    print(f"Exiting because: {msg}.")
    print("Bye")
    exit(code)

class Datum: 
    def __init__(self, raw: dict[str, any], input: str):
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

def resolve_filter() -> Callable[[dict[str, any]], bool]:
    if path.exists(FILTER_FILE):
        from filter import filter

        return filter
    
    print("No filter provided, will process all datums.")
    return lambda x: True

def resolve_datums(filter: Callable[[dict[str, any]], bool]) -> list[Datum]:
    if not path.exists(INPUTS_CONFIG_PATH):
        exit_with(f"Missing: {INPUTS_CONFIG_PATH}")

    with open(INPUTS_CONFIG_PATH, "r") as f:
        content = json.load(f)

    datums = []

    full_filter = lambda x: isinstance(x, dict) and "datum_id" in x and filter(x)

    for input in content.keys():
        datums += [Datum(file, input) for file in content[input]["files"] if full_filter(file)]

    return datums

def resolve_gen_mdata() -> Callable[[Datum], dict[str, any]|None]:
    if path.exists(MDATA_GEN_FILE):
        from gen_mdata import gen_mdata
        return gen_mdata

    return lambda x: None

def apply_metadata(datums: list[Datum], resolve_mdata: Callable[[Datum], dict[str, any]], token: str, host: str):
    metadata = {}
    for datum in datums: 
        print("resoling mdata for: ", datum)
        if new_metadata := resolve_mdata(datum):
            metadata[datum.id] = new_metadata

    if not metadata: 
        exit_with("No metadata resolved - nothing to apply!")

    print("Resolved metadata")
    for item in metadata.items():
        print(item)

    header = {"Authorization": f"Token {token}"}

    try:
        res = post(get_apply_url(host), headers=header, json={"datum_metadata": metadata})
        print("Metadata applied!")
    except Exception as e:
        print("Exception while applying metadata: ", e)    
        exit_with(f"Failed to apply metadata")

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
    print("Resolved datums")
    for d in datums: 
        print(d.name)

    gen_mdata = resolve_gen_mdata()
    apply_metadata(datums, gen_mdata, token, "http://192.168.1.8")

    print("Leaving.")