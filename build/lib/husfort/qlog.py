import sys
from loguru import logger


def define_logger(
        level: str = "DEBUG",
        fmt_datetime: str = "[<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green>]",
        fmt_level: str = "[<level>{level:<8}</level>]",
        fmt_name: str = "<cyan>{name}</cyan>",
        fmt_func: str = "<cyan>{function}</cyan>",
        fmt_line: str = "<cyan>{line}</cyan>",
        fmt_msg: str = "{message}",
        show_location: bool = False
) -> None:
    """

    :param level: from which level to print, available options =
                  ["TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"]
    :param fmt_datetime: datetime format, default = "[<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green>]"
    :param fmt_level: level format, default = "[<level>{level:<8}</level>]"
    :param fmt_name: script file name, default = "<cyan>{name}</cyan>"
    :param fmt_func: function name, default = "<cyan>{function}</cyan>"
    :param fmt_line: line number, default = "<cyan>{line}</cyan>"
    :param fmt_msg: format for message, default = "{message}", to color it, try "<level>{message}</level>"
    :param show_location: whether to print location info in the log
    :return: None, use this function once in the main entry point of project, all the loggers in the module will
             be affected
    """
    logger.remove(0)
    fmt_location = f"[{fmt_name}:{fmt_func}:{fmt_line}]"
    if show_location:
        fmt_record = f"{fmt_datetime}{fmt_level} {fmt_msg}{fmt_location}"
    else:
        fmt_record = f"{fmt_datetime}{fmt_level} {fmt_msg}"
    logger.add(sys.stdout, level=level, format=fmt_record)
