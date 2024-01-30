import logging
import os
import sys

logging.basicConfig(
    level=logging.getLevelName(os.getenv('LOG_LEVEL', "DEBUG")),
    datefmt="%Y%jT%H%M%S",
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)