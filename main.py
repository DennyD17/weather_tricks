import csv
import datetime
import itertools
import logging
from collections import Counter, defaultdict, namedtuple
from functools import wraps
from logging.config import dictConfig
from typing import List, Optional, Set, Tuple

import requests
from urllib3.exceptions import RequestError

from config import OUTPUT_FILE_TPL, WEATHER_DATA_URL, WEATHER_FILENAME, log_config
from constants import (
    AVG_JULY_TEMPERATURE,
    HI_TEMPERATURE_DELTA,
    HI_TEMPERATURE_TO_COMPARE,
    LOW_TEMPERATURE_DELTA,
    LOW_TEMPERATURE_TO_COMPARE,
)

dictConfig(log_config)
logger = logging.getLogger(__name__)


def logging_deco(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"Func {func.__name__} ({func.__doc__}) starts")
        result = func(*args, **kwargs)
        logger.info(f"Func {func.__name__} finished with result {result}")
        return result

    return wrapper


def download_weather_cvs() -> None:
    """Download csv file to local"""
    try:
        with requests.get(WEATHER_DATA_URL, stream=True) as response, open(
            WEATHER_FILENAME, "wb"
        ) as f:
            for line in response.iter_lines():
                f.write(line + "\n".encode())
    except RequestError as e:
        logger.warning(f"Error while downloading csv file: {e}")


class WeatherSnippet(
    namedtuple(
        "WeatherSnippet",
        [
            "date",
            "time",
            "outside_temperature",
            "hi_temperature",
            "low_temperature",
        ],
    )
):
    """Namedtuple to store data about weather in time unit"""

    __slots__ = ()

    def diff_with_avg_temp(self) -> float:
        """Difference between current temperature and average daily"""
        return self.date.avg_temperature - self.outside_temperature

    def __eq__(self, other):
        return self.time == other.time


class Date(datetime.date):
    """Class to store information about days and links to time snippets"""

    _unique_dates = set()

    def __init__(self, *args, **kwargs):
        self.weather_snippets: List[WeatherSnippet] = []
        self._max_temperature: Optional[float] = None
        self._hottest_times_of_the_day: Optional[List[str]] = None
        self._avg_temperature: Optional[float] = None

    @classmethod
    def from_string_uniq(cls, string: str, delimiter: str = "/") -> "Date":
        """Create Date class object from string"""
        try:
            day, month, year = map(int, string.split(delimiter))
            _date = cls(year, month, day)
        except ValueError:
            logger.exception(f"Can't cast {string} to date using delimiter {delimiter}")
            raise

        if _date in cls._unique_dates:
            logger.debug(f"Date {_date} already exists")
            return next(filter(lambda x: x == _date, cls._unique_dates))
        else:
            logger.debug(f"Created new date {_date}")
            cls._unique_dates.add(_date)
            return _date

    @property
    def max_temperature(self) -> float:
        """Max temperature of the day"""
        if not self._max_temperature:
            self._max_temperature = max(
                map(lambda x: x.outside_temperature, self.weather_snippets)
            )
            logger.debug(f"Max temperature for {self} - {self._max_temperature}")
        return self._max_temperature

    @property
    def hottest_times_of_the_day(self) -> List[str]:
        """Times with hottest temperature"""
        if not self._hottest_times_of_the_day:
            self._hottest_times_of_the_day = [
                snippet.time
                for snippet in self.weather_snippets
                if snippet.outside_temperature == self.max_temperature
            ]
            logger.debug(
                f"{self} the hottest temperature {self.max_temperature} was at {self._hottest_times_of_the_day}"
            )
        return self._hottest_times_of_the_day

    @property
    def avg_temperature(self) -> float:
        """Average temperature of the day"""
        if not self._avg_temperature:
            self._avg_temperature = sum(
                map(lambda x: x.outside_temperature, self.weather_snippets)
            ) / len(self.weather_snippets)
            logger.debug(f"Average temperature for {self} is {self._avg_temperature}")
        return self._avg_temperature

    def get_eq_time(self, snippet: WeatherSnippet) -> WeatherSnippet:
        """Get weather snippet with same time"""
        return next((s for s in self.weather_snippets if s == snippet), None)


class WeatherHandler:
    """Handler for working with weather data"""

    def __init__(self, snippets: List[WeatherSnippet], days: Set[Date]):
        self.snippets: List[WeatherSnippet] = snippets
        self.days: Set[Date] = days

    @classmethod
    def from_csv(cls) -> "WeatherHandler":
        """Create handler obj using csv file"""
        try:
            snippets = []
            _dates = set()
            with open(WEATHER_FILENAME, newline="") as csvfile:
                try:
                    reader = csv.DictReader(csvfile)
                except csv.Error:
                    logger.exception(
                        f"Unable to read file {WEATHER_FILENAME}. Bad format"
                    )
                    raise
                for row in reader:
                    _date = Date.from_string_uniq(row["Date"])
                    snippet = WeatherSnippet(
                        date=_date,
                        time=row["Time"],
                        outside_temperature=float(row["Outside Temperature"]),
                        hi_temperature=float(row["Hi Temperature"]),
                        low_temperature=float(row["Low Temperature"]),
                    )
                    _date.weather_snippets.append(snippet)
                    _dates.add(_date)
                    snippets.append(snippet)
            logger.info(
                f"Created handler to process {len(_dates)} dates and {len(snippet)} weather snippets"
            )
            return cls(snippets, _dates)
        except FileNotFoundError:
            logger.exception(f"File {WEATHER_FILENAME} was not found")
            raise

    @logging_deco
    def hottest_time_of_days(self) -> str:
        """What time of the day is the most commonly occurring hottest time"""
        hottest_times = list(
            itertools.chain.from_iterable(
                [day.hottest_times_of_the_day for day in self.days]
            )
        )
        counter = Counter(hottest_times)
        logger.info(f"Most common 5 hottest times: {counter.most_common(5)}")
        return counter.most_common(1)[0][0]

    @logging_deco
    def average_time_of_hottest_daily_temperature(self) -> Tuple[int, str]:
        """What is the average time of hottest daily temperature (over month)"""
        hottest_time_by_month = defaultdict(list)
        for day in self.days:
            hottest_time_by_month[day.month].extend(day.hottest_times_of_the_day)

        for month, times_of_hottest in hottest_time_by_month.items():
            times_in_minutes = []
            for elem in times_of_hottest:
                hours, minutes = map(int, elem.split(":"))
                times_in_minutes.append(hours * 60 + minutes)
            avg_hottest_time = sum(times_in_minutes) / len(times_in_minutes)
            avg_hottest_time = (
                f"{int(avg_hottest_time // 60)}: {int(avg_hottest_time % 60)}"
            )
            yield month, avg_hottest_time

    @logging_deco
    def top_n_hottest_times(self) -> str:
        """Which are the Top Ten hottest times on distinct days, preferably sorted by date order."""
        snippets_sorted_by_max_temp = sorted(
            self.snippets, key=lambda x: x.outside_temperature, reverse=True
        )
        days_used = set()
        top_ten = []
        for elem in snippets_sorted_by_max_temp:
            if elem.date in days_used:
                continue
            top_ten.append(elem)
            if len(top_ten) == 10:
                break
            days_used.add(elem.date)

        return "\n".join(
            f"Date: {elem.date}, Time: {elem.time}, Temperature {elem.outside_temperature}"
            for elem in top_ten
        )

    @logging_deco
    def days_with_hi_and_low_in_iterval(self) -> str:
        """
        Using the ‘Hi Temperature’ values produce a “.txt” file containing all of the Dates and Times
        where the “Hi Temperature” was within +/- 1 degree of 22.3 or the “Low Temperature” was
        within +/- 0.2 degree higher or lower of 10.3 over the first 9 days of June
        """
        data = filter(
            lambda x: abs(x.hi_temperature - HI_TEMPERATURE_TO_COMPARE)
            <= HI_TEMPERATURE_DELTA
            or abs(x.low_temperature - LOW_TEMPERATURE_TO_COMPARE)
            <= LOW_TEMPERATURE_DELTA,
            filter(lambda x: x.date.month == 6 and x.date.day < 10, self.snippets),
        )

        return "\n".join(
            f"Date: {elem.date}, Time: {elem.time}, Hi: {elem.hi_temperature}, Low: {elem.low_temperature}"
            for elem in data
        )

    @logging_deco
    def july_forecast(self):
        """create forecast for july"""
        june_days = sorted(filter(lambda x: x.month == 6, self.days))[:9]
        for june_day in june_days:
            july_day = Date.from_string_uniq(
                f"{june_day.day}/{june_day.month + 1}/{june_day.year}"
            )
            if len(july_day.weather_snippets) == len(june_day.weather_snippets):
                logger.info(
                    f"There are already full set of weather snippets for {july_day}"
                )
                continue

            # If there are aleady weather data for july day we have to calculate diff to keep average temperature
            delta = 0
            for snippet in july_day.weather_snippets:
                june_eq = june_day.get_eq_time(snippet)
                expected_value = AVG_JULY_TEMPERATURE - june_eq.diff_with_avg_temp()
                current_delta = snippet.outside_temperature - expected_value
                delta += current_delta
            diff = delta / (
                len(june_day.weather_snippets) - len(july_day.weather_snippets)
            )
            logger.debug(
                f"Diff to remove from outside temperature for {july_day} is {diff}"
            )

            for june_time in june_day.weather_snippets:
                if june_time in july_day.weather_snippets:
                    continue
                july_time = WeatherSnippet(
                    date=july_day,
                    time=june_time.time,
                    outside_temperature=round(
                        AVG_JULY_TEMPERATURE - june_time.diff_with_avg_temp() - diff, 1
                    ),
                    hi_temperature=june_day.avg_temperature,  # Not necessary to fill it
                    low_temperature=june_day.avg_temperature,  # Not necessary to fill it
                )
                july_day.weather_snippets.append(july_time)
                self.snippets.append(july_time)
            self.days.add(july_day)
            logger.debug(
                f"Avg temperature for {july_day} is {july_day.avg_temperature}"
            )

        for t in sorted(
            filter(lambda x: x.date.month == 7, self.snippets),
            key=lambda x: (x.date, x.time),
        ):
            yield f"{t.date} {t.time} {t.outside_temperature}\n"


if __name__ == "__main__":
    download_weather_cvs()
    handler = WeatherHandler.from_csv()

    with open(OUTPUT_FILE_TPL.format(1), "w") as f:
        f.write("What time of the day is the most commonly occurring hottest time?\n")
        f.write(handler.hottest_time_of_days())

        f.write(
            "\n\nWhat is the average time of hottest daily temperature (over month)?\n"
        )
        for month, temp in handler.average_time_of_hottest_daily_temperature():
            f.write(f"{month}: {temp}\n")

        f.write(
            "\n\nWhich are the Top Ten hottest times on distinct days, preferably sorted by date order?\n"
        )
        f.write(handler.top_n_hottest_times())

    with open(OUTPUT_FILE_TPL.format(2), "w") as f:
        f.write(handler.days_with_hi_and_low_in_iterval.__doc__)
        f.write(handler.days_with_hi_and_low_in_iterval())

    with open(OUTPUT_FILE_TPL.format(3), "w") as f:
        for elem in handler.july_forecast():
            f.write(elem)
