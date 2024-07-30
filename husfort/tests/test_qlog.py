from loguru import logger
from husfort.qlog import define_logger

define_logger(level="TRACE", fmt_msg="<level>{message}</level>")

logger.trace("This is a trace")
logger.debug("This is a debug")
logger.info("This is a info")
logger.success("This is success")
logger.warning("This is a warning")
logger.error("This is a error")
logger.critical("This is a critical")
