#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import re
from datetime import datetime
from time import sleep
from typing import Any, Iterable, List, Mapping, Tuple, Optional

import requests
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import NoAuth

URL_BASE = "https://itunes.apple.com"


class Reviews(HttpStream):
    url_base = URL_BASE
    cursor_field = "ds"
    primary_key = "ds"

    @staticmethod
    def __datetime_to_str(d: datetime) -> str:
        return d.strftime("%Y-%m-%dT%H:%M:%S")

    @staticmethod
    def __str_to_datetime(d: str) -> datetime:
        return datetime.strptime(d, "%Y-%m-%dT%H:%M:%S")

    def __update_params(self):
        self.__page_ind = 1

    # noinspection PyUnusedLocal
    def __init__(self, config, **kwargs):
        super().__init__()
        self.__config = config

        self.__logger = AirbyteLogger()

        self.__country_ind = 0
        self.__count_in_req = 0
        self.__count = 0
        self.__total_count = 0

        self.__update_params()

        self.__cursor_value = self.__str_to_datetime(self.__config["start_time"])
        self.__tmp_cursor_value = self.__str_to_datetime(self.__config["start_time"])

        self.__logger.info("Read latest review timestamp from config: {}".format(self.__config["start_time"]))

    @property
    def state(self) -> Mapping[str, Any]:
        ds = self.__datetime_to_str(self.__tmp_cursor_value)
        self.__logger.info("Saved latest review timestamp to file: {}".format(ds))
        return {self.cursor_field: ds}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        ds = value.get(self.cursor_field)
        if ds is not None:
            self.__logger.info("Read latest review timestamp from file: {}".format(ds))
            self.__cursor_value = self.__str_to_datetime(ds)
            self.__tmp_cursor_value = self.__str_to_datetime(ds)

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            ds = record.get("ds")
            if ds is not None:
                self.__tmp_cursor_value = max(self.__tmp_cursor_value, ds)
            yield record

    http_method = "GET"

    def path(
            self,
            *,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return "/{}/rss/customerreviews/page={}/id={}/sortby=mostrecent/json".format(
            self.__config["countries"][self.__country_ind], self.__page_ind, self.__config["app_id"]
        )

    @staticmethod
    def __fetch_review_items(response: requests.Response):
        return response.json()["feed"].get("entry", [])

    @staticmethod
    def __rename_field(v: dict, v_name: str, v1: dict, v1_name: str):
        field = v.get(v_name)
        if field is not None:
            v1[v1_name] = field

    def __transform(self, v: dict):
        message = ""

        title = v["title"]["label"]
        content = v["content"]["label"]
        message = title + '. ' + content

        v1 = {
            "source": "App Store",
            "lang": "unknown",
            "country": self.__config["countries"][self.__country_ind],
            "ticket_id": v["id"]["label"],
            "user_id": v["author"]["name"]["label"],
            "ds": datetime.strptime(v["updated"]["label"][:19], "%Y-%m-%dT%H:%M:%S"),
            "version": v["im:version"]["label"],
            "message": message,
            "score": v["im:rating"]["label"],
        }

        return v1

    def parse_response(
            self,
            response: requests.Response,
            *,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        result = []
        review_items = self.__fetch_review_items(response)
        for review in review_items:
            v = self.__transform(review)
            ds = v.get("ds")
            if (ds is None) or (ds > self.__cursor_value):
                result.append(v)
        self.__count_in_req = len(result)
        self.__count += self.__count_in_req
        return result

    def __fetch_next_page_token(self, response: requests.Response):
        token = "token"

        link = next(link["attributes"]["href"] for link in response.json()["feed"]["link"] if link["attributes"]["rel"] == "next")
        new_page_ind = int(re.findall("page=([0-9]+)", link)[0])

        if (self.__count_in_req == 0) or (self.__page_ind >= new_page_ind):
            self.__logger.info("Fetched {} reviews for country=\"{}\"".format(
                self.__count, self.__config["countries"][self.__country_ind]
            ))
            self.__country_ind += 1
            if self.__country_ind == len(self.__config["countries"]):
                self.__logger.info("Totally fetched {} reviews".format(self.__total_count + self.__count))
                token = None
            else:
                self.__update_params()

            self.__total_count += self.__count
            self.__count = 0
        else:
            self.__page_ind = new_page_ind

        return token

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        timeout_ms = self.__config.get("timeout_ms")
        if timeout_ms is not None:
            sleep(timeout_ms / 1000.0)
        return self.__fetch_next_page_token(response)


class SourceAppStoreRss(AbstractSource):
    @staticmethod
    def __get_app_id(app_name: str) -> [str, None]:
        resp = requests.get("https://www.google.com/search", params={"q": f"app store {app_name}"})
        pattern = fr"https://apps.apple.com/[a-z]{{2}}/.+?/id([0-9]+)"
        app_id = re.search(pattern, resp.text)
        if app_id is not None:
            app_id = app_id.group(1)
        return app_id

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        logger.info("Checking connection configuration for app \"{}\"...".format(config["app_name"]))

        logger.info("Checking \"app_name\"...")
        app_id = self.__get_app_id(config["app_name"])
        if app_id is None:
            error_text = "\"app_name\" \"{}\" is invalid!".format(config["app_name"])
            logger.error(error_text)
            return False, {"key": "app_name", "value": config["app_name"], "error_text": error_text}
        logger.info("\"app_name\" \"{}\" is valid".format(config["app_name"]))

        if config.get("app_id") is not None:
            logger.info("Checking \"app_id\"...")
            if config["app_id"] != app_id:
                error_text = "\"app_id\" \"{}\" is invalid!".format(config["app_id"])
                logger.error(error_text)
                return False, {"key": "app_id", "value": config["app_id"], "error_text": error_text}
            logger.info("\"app_id\" \"{}\" is valid".format(config["app_id"]))

        logger.info("Checking \"countries\" values...")
        for ind, gl in enumerate(config["countries"]):
            logger.info("Checking \"countries\" {}'th value \"{}\"".format(ind, gl))
            path = "/{}/rss/customerreviews/page={}/id={}/sortby=mostrecent/json".format(gl, 1, app_id)
            response = requests.get(URL_BASE + path)
            if len(response.text) == 0:
                error_text = "\"countries\" {}'th value \"{}\" is invalid!".format(ind, gl)
                logger.error(error_text)
                return False, {"key": "countries", "value": config["countries"], "error_text": error_text}
            logger.info("\"countries\" {}'th value \"{}\" is valid".format(ind, gl))
        logger.info("\"countries\" values are valid")

        logger.info("Connection configuration is valid!")
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = NoAuth()
        if config.get("app_id") is None:
            config["app_id"] = self.__get_app_id(config["app_name"])
        return [Reviews(authenticator=auth, config=config)]
