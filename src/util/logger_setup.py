import logging
from util.constants import MSG_FORMAT, DATETIME_FORMAT

logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)