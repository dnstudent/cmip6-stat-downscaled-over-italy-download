from data_download import variables, modes, cdd_variant
import requests
import json
from time import sleep

with requests.Session() as s:
    for variable in variables:
        for mode in modes:
            variant = cdd_variant(variable, mode)
            infos = s.get(f"https://ddshub.cmcc.it/web/datasets/cmip6-stat-downscaled-over-italy/{variant}").json()
            widget_info = {data["label"]: data for data in infos["widgets"]}
            with open(f"vars/{variant}.json", "w") as f:
                json.dump(widget_info, f)
            sleep(2)