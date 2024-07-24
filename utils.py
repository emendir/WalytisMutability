from loguru import logger
import sys
logger.remove(0)
logger.add(sys.stdout, format="<level>{message}</level>")


logger.add("mutablock.log", rotation="1 week")
