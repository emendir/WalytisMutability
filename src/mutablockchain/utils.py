from loguru import logger
import sys
try:
    logger.remove(0)
except ValueError:
    pass
logger.add(sys.stdout, format="<level>{message}</level>")


logger.add("mutablock.log", rotation="1 week")
