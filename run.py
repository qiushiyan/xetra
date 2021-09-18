"""entry to run ETL jobs"""
import logging
import logging.config
import yaml


def main():
    with open("./configs/config.yaml") as f:
        config = yaml.safe_load(f)
    log_config = config["logging"]
    logging.config.dictConfig(log_config)


if __name__ == "__main__":
    main()
