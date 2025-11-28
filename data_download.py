import argparse
import logging
from pathlib import Path
from time import monotonic_ns, sleep

import ddsapi
from tqdm import tqdm

modes = ["hist", "future"]
variables = ["hurs", "pr", "sfcWind", "tas", "tasmax", "tasmin"]
scenarios = {"future": ["ssp126", "ssp370"], "hist": []}
models = [
    "CESM2",
    "CMCC-CM2-SR5",
    "CNRM-ESM2-1",
    "EC-Earth3-Veg",
    "IPSL-CM6A-LR",
    "MIROC6",
    "MPI-ESM1-2-HR",
    "NorESM2-MM",
    "UKESM1-0-LL",
]

def request_payload(
    model: str, variable: str, scenario: str | None, years: list[int] | list[str]
):
    assert (
        model in models
        and variable in variables
        and (scenario is None or scenario in scenarios["future"])
    )
    request = {
        "model": model,
        "variable": [cdd_varname(variable)],
        "time": {
            "year": list(map(str, years)),
            "month": list(map(str, range(1, 13))),
            "day": list(map(str, range(1, 32))),
        },
        "format": "netcdf",
    }
    if scenario:
        request["scenario"] = scenario
    return request


variants = {mode: [f"{var}-{mode}" for var in variables] for mode in modes}
models_var_assocs = {
    "CESM2": {
        "hist": ["pr", "sfcWind", "tas"],
        "future": ["pr", "sfcWind", "tas"],
    },
    "CMCC-CM2-SR5": {
        "hist": ["hurs", "pr", "sfcWind", "tas"],
        "future": ["hurs", "pr", "sfcWind", "tas"],
    },
    "CNRM-ESM2-1": {
        "hist": ["hurs", "pr", "tas", "tasmax", "tasmin"],
        "future": ["hurs", "pr", "tas", "tasmax", "tasmin"],
    },
    "EC-Earth3-Veg": {
        "hist": ["hurs", "pr", "tas", "tasmax", "tasmin"],
        "future": ["hurs", "pr", "tas", "tasmax", "tasmin"],
    },
    "IPSL-CM6A-LR": {"hist": variables, "future": variables},
    "MIROC6": {"hist": variables, "future": variables},
    "MPI-ESM1-2-HR": {"hist": variables, "future": variables},
    "NorESM2-MM": {"hist": variables, "future": variables},
    "UKESM1-0-LL": {"hist": variables, "future": variables},
}


def cdd_varname(variable: str):
    return f"{variable}Adjust"


def cdd_variant(variable: str, mode: str):
    return f"{variable}-{mode}"


def make_scenarios(proposed_scenarios: list[str | None], mode: str):
    if "default" in proposed_scenarios:
        if mode == "hist":
            return [None]
        else:
            return scenarios["future"]
    else:
        return proposed_scenarios



def outname(ds_root_dir: Path, mode: str, model: str, variable: str, scenario: str | None, years: list[int]):
    if scenario:
        return (
            ds_root_dir
            / mode
            / scenario
            / model
            / cdd_varname(variable)
            / f"{'_'.join(map(str, years))}.nc"
        )
    else:
        return (
            ds_root_dir
            / mode
            / model
            / cdd_varname(variable)
            / f"{'_'.join(map(str, years))}.nc"
        )


def valid_combo(mode: str, model: str, variable: str, scenario: str | None) -> bool:
    compatible_var = (
        model in models_var_assocs and variable in models_var_assocs[model][mode]
    )
    return (
        (scenario in scenarios["future"] and mode == "future")
        or (scenario is None and mode == "hist")
    ) and compatible_var


def valid_year(year: int, mode: str) -> bool:
    if mode == "hist":
        return 1985 <= year <= 2014
    elif mode == "future":
        return 2015 <= year <= 2100
    else:
        raise Exception(f"Invalid mode: {mode}")


def build_argparser():
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("out_path")
    _ = parser.add_argument(
        "--from-year", type=int, required=True, help="Download data starting from year"
    )
    _ = parser.add_argument(
        "--to-year",
        type=int,
        required=True,
        help="Download data up to this year (included)",
    )
    _ = parser.add_argument(
        "--mode",
        required=True,
        choices=modes,
        default=modes,
        help="Historical or future data. Valid options are 'hist' and 'future'.",
    )
    _ = parser.add_argument(
        "--variable",
        nargs="+",
        default=variables,
        help=f"Variables to download. Valid options are {', '.join(variables)}. Default is everything.",
    )
    _ = parser.add_argument(
        "--scenario",
        nargs="+",
        required=False,
        default=["default"],
        help="Scenarios to download. Valid options are 'ssp126', 'ssp370' and 'default'. Leave empty or 'default' for historical data.",
    )
    _ = parser.add_argument(
        "--model",
        nargs="+",
        default=models,
        help=f"Download data from these models. Valid options are {', '.join(models)}. Default is everything.",
    )
    _ = parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Create empty files instead of downloading",
    )
    return parser


def main():
    parser = build_argparser()
    args = parser.parse_args()

    ds_root_dir = Path(args.out_path)
    ds_root_dir.mkdir(exist_ok=True)

    c = ddsapi.Client()
    dataset = "cmip6-stat-downscaled-over-italy"

    logging.basicConfig(filename="download.log", format="%(asctime)s - %(levelname)s - %(message)s", force=True)
    logger = logging.getLogger(__name__)

    stack = [
        (str(args.mode), model, variable, scenario, [year])
        for scenario in make_scenarios(args.scenario, args.mode)
        for model in args.model
        for variable in args.variable
        for year in range(args.from_year, args.to_year + 1)
        if valid_combo(args.mode, model, variable, scenario)
        and valid_year(year, args.mode)
        and not outname(
            ds_root_dir, args.mode, model, variable, scenario, [year]
        ).exists()
    ]

    for mode, model, variable, scenario, years in tqdm(stack):
        variable_name = cdd_varname(variable)
        payload = request_payload(model, variable, scenario, years)
        fname = outname(ds_root_dir, mode, model, variable, scenario, years)
        fname.parent.mkdir(exist_ok=True, parents=True)
        try:
            # logger.info(f"Trying {model}, {variable}, {scenario}, {years}")
            payload = request_payload(model, variable, scenario, years)
            start = monotonic_ns()
            if not args.dry_run:
                _ = c.retrieve(dataset, cdd_variant(variable, mode), payload, str(fname))  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            delta_time = (monotonic_ns() - start) // 1e9
            logger.info(
                f"Downloaded data for years: {years}, model: {model}, scenario: {scenario}, variable: {variable_name} to {fname} in {delta_time}s"
            )
        except Exception as e:
            logger.error(
                f"Failed to download data for year: {years}, model: {model}, scenario: {scenario}, variable: {variable_name}; got: {e}"
            )
            if fname.exists():
                fname.unlink()
                logger.info(f"{fname} was deleted")
            sleep(5)


if __name__ == "__main__":
    main()
