import argparse
import logging

try:
    import tomllib  # type: ignore
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore

from . import run


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

    print(config_dict)

    run()


if __name__ == "__main__":
    main()
