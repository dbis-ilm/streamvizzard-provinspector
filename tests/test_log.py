import logging

from provinspector.utils.log import LOG_FORMAT, LOG_LEVEL, create_logger


class TestLog:
    def test_create_logger(self):
        create_logger()
        log = logging.getLogger()

        assert log.level == LOG_LEVEL

        assert log.handlers[0].formatter._fmt == LOG_FORMAT  # type:ignore
