from collections import deque
import logging
import time
import socket
import hashlib

import requests
import pdpyras
import yaml


LOG = logging.getLogger(__name__)


hostname = socket.getfqdn()


class Check(object):

    def __init__(self, url, pd_api_key, **kwargs):
        self.url = url
        self.pd_api_key = pd_api_key

        self.previous_checks = deque(maxlen=10)

        sha = hashlib.sha256()
        sha.update(url.encode("ascii"))
        self.dup_key = sha.hexdigest()

    def run(self):

        status = 0
        resp_text = ""
        exc_text = ""

        try:
            resp = requests.get(self.url)
            resp_text = resp.text
            resp.raise_for_status()
        except Exception as ex:
            status = 1
            exc_text = str(ex)

        LOG.info("Check results: status=%r body=%r exc=%r" % (status, resp_text, exc_text))

        self.previous_checks.append(status)
        self._report({"body": resp_text, "exception": exc_text})

    def _report(self, data):
        pd = pdpyras.EventsAPISession(self.pd_api_key)

        if len(list(filter(lambda x: x != 0, list(self.previous_checks)[-2:]))) >= 2:
            # last two are consecutive failures
            LOG.info("Reporting as triggered")
            pd.trigger(
                "Service at %s is DOWN" % self.url,
                source=hostname,
                severity="critical",
                custom_details=data,
                dedup_key=self.dup_key
            )
        else:
            LOG.info("Reporting as resolved")
            pd.resolve(self.dup_key)


def main():

    with open("./watchman.yaml") as watchfile:
        watch_data = yaml.safe_load(watchfile)

    checks = []

    for check in watch_data.get("checks", []):
        checks.append(Check(**check))

    while True:
        try:
            for check in checks:
                check.run()
        except Exception as ex:
            LOG.exception("error in main loop: %s" % ex)

        time.sleep(60)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
