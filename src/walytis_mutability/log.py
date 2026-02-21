import logging
from logging.handlers import RotatingFileHandler
import os

from emtest.log_utils import get_app_log_dir


# Formatter
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

# Console handler (INFO+)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)


# # Root logger
# logger_root = logging.getLogger()
# logger_root.setLevel(logging.DEBUG)  # Global default
# logger_root.addHandler(console_handler)
# # logger_root.addHandler(file_handler)

logger_walymut = logging.getLogger("Walytis_Mutability")
logger_walymut.setLevel(logging.DEBUG)


file_handler = None
LOG_DIR = get_app_log_dir("WalytisMutability", "Waly")
if LOG_DIR is None:
    logger_walymut.info("Logging to files is disabled.")
else:
    LOG_PATH = os.path.join(LOG_DIR, "WalytisMutability.log")
    logger_walymut.info(f"Logging to {os.path.abspath(LOG_PATH)}")

    file_handler = RotatingFileHandler(
        LOG_PATH, maxBytes=5 * 1024 * 1024, backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger_walymut.addHandler(file_handler)
