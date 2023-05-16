import logging

log_level = logging.INFO

log_config = {
    "version": 1,
    "root": {"handlers": ["console"], "level": log_level},
    "handlers": {
        "console": {
            "formatter": "std_out",
            "class": "logging.StreamHandler",
            "level": log_level,
        }
    },
    "formatters": {
        "std_out": {
            "format": "%(asctime)s : %(levelname)s : %(module)s : %(funcName)s : %(lineno)d : %(message)s",
            "datefmt": "%d-%m-%Y %I:%M:%S",
        }
    },
}

OUTPUT_FILE_TPL = "output/task_{0}_results.txt"

WEATHER_DATA_URL = "https://www.fifeweather.co.uk/cowdenbeath/200606.csv"

WEATHER_FILENAME = "weather_data.csv"
