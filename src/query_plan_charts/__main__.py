import argparse
import logging
import sys

try:
    import tomllib  # type: ignore
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore

from . import run_0d, run_1d, run_2d
from .base import ParameterConfig, ParameterizedStatement


def main():
    parser = argparse.ArgumentParser(description="XXX")
    parser.add_argument("configuration", metavar="CONFIG",
                        help="Path to configuration file")
    parser.add_argument("-v", "--verbose", action="count",
                        help="Verbosity level. "
                        "This may be specified up to three times.")
    args = parser.parse_args()

    logging.basicConfig()
    if not args.verbose:
        logging.getLogger().setLevel(logging.ERROR)
    elif args.verbose == 1:
        logging.getLogger().setLevel(logging.WARNING)
    elif args.verbose == 2:
        logging.getLogger().setLevel(logging.INFO)
    else:
        logging.getLogger().setLevel(logging.DEBUG)

    with open(args.configuration, "rb") as f:
        config_dict = tomllib.load(f)

    if "setup_statements" not in config_dict:
        print(
            "Missing 'setup_statements' value in configuration file",
            file=sys.stderr,
        )
        sys.exit(1)
    if not isinstance(config_dict["setup_statements"], list):
        print("Value for 'setup_statements' must be a list", file=sys.stderr)
        sys.exit(1)
    if "target_query" not in config_dict:
        print(
            "Missing 'target_query' value in configuration file",
            file=sys.stderr,
        )
        sys.exit(1)
    if not isinstance(config_dict["target_query"], str):
        print("Value for 'target_query' must be a string", file=sys.stderr)
        sys.exit(1)

    setup_statements = []
    parameters = []
    for raw_statement in config_dict["setup_statements"]:
        if isinstance(raw_statement, str):
            setup_statements.append(ParameterizedStatement(raw_statement, 0))
        elif isinstance(raw_statement, dict):
            if "statement" not in raw_statement:
                print("Statement table is missing a value for 'statement'",
                      file=sys.stderr)
                sys.exit(1)
            if not isinstance(raw_statement["statement"], str):
                print(
                    "Value for 'statement' must be a string",
                    file=sys.stderr,
                )
                sys.exit(1)

            if "parameters" not in raw_statement:
                setup_statements.append(ParameterizedStatement(
                    raw_statement["statement"], 0))
                continue
            if not isinstance(raw_statement["parameters"], list):
                print(
                    "Value for 'parameters' must be an array",
                    file=sys.stderr,
                )
                sys.exit(1)

            setup_statements.append(ParameterizedStatement(
                raw_statement["statement"],
                len(raw_statement["parameters"]),
            ))

            for raw_parameter in raw_statement["parameters"]:
                if "start" not in raw_parameter:
                    print("Statement table is missing a value for 'start'",
                          file=sys.stderr)
                    sys.exit(1)
                if not isinstance(raw_parameter["start"], int):
                    print(
                        "Value for 'start' must be an integer",
                        file=sys.stderr,
                    )
                    sys.exit(1)
                if "stop" not in raw_parameter:
                    print("Statement table is missing a value for 'stop'",
                          file=sys.stderr)
                    sys.exit(1)
                if not isinstance(raw_parameter["stop"], int):
                    print(
                        "Value for 'stop' must be an integer",
                        file=sys.stderr,
                    )
                    sys.exit(1)
                if "steps" not in raw_parameter:
                    print("Statement table is missing a value for 'steps'",
                          file=sys.stderr)
                    sys.exit(1)
                if not isinstance(raw_parameter["steps"], int):
                    print(
                        "Value for 'steps' must be an integer",
                        file=sys.stderr,
                    )
                    sys.exit(1)
                if "name" in raw_parameter:
                    if not isinstance(raw_parameter["name"], str):
                        print(
                            "Value for 'name' must be a string",
                            file=sys.stderr,
                        )
                        sys.exit(1)
                    name = raw_parameter["name"]
                else:
                    name = ""

                parameters.append(ParameterConfig(
                    raw_parameter["start"],
                    raw_parameter["stop"],
                    raw_parameter["steps"],
                    name,
                ))
        else:
            print(
                "Each statement must be provided as a string or a key-value "
                "table",
                file=sys.stderr,
            )
            sys.exit(1)

    if len(parameters) > 2:
        print("Too many parameters in queries", file=sys.stderr)
        sys.exit(1)
    elif len(parameters) == 2:
        run_2d(setup_statements,
               parameters[0], parameters[1], config_dict["target_query"])
    elif len(parameters) == 1:
        run_1d(setup_statements, parameters[0], config_dict["target_query"])
    elif len(parameters) == 0:
        run_0d(setup_statements, config_dict["target_query"])


if __name__ == "__main__":
    main()
