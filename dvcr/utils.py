import logging
import random

import colorama

COLOR = [
    colorama.Fore.RED,
    colorama.Fore.GREEN,
    colorama.Fore.YELLOW,
    colorama.Fore.BLUE,
    colorama.Fore.MAGENTA,
    colorama.Fore.CYAN,
    colorama.Fore.LIGHTRED_EX,
    colorama.Fore.LIGHTGREEN_EX,
    colorama.Fore.LIGHTYELLOW_EX,
    colorama.Fore.LIGHTBLUE_EX,
    colorama.Fore.LIGHTMAGENTA_EX,
    colorama.Fore.LIGHTCYAN_EX,
]

colorama.init(autoreset=True)


def init_logger(name: str, level: int = logging.INFO):

    logger_name = "dvcr_" + name

    if logger_name in logging.root.manager.loggerDict:
        return logging.getLogger(name=logger_name)   # Return logger immediately if it already exists

    logger = logging.getLogger(name=logger_name)

    try:
        color = random.choice(COLOR)
        COLOR.remove(color)
    except IndexError:
        color = colorama.Fore.WHITE

    logger.setLevel(level=level)

    handler = logging.StreamHandler()

    formatter = logging.Formatter(
            color + "[" + name + "]" + colorama.Fore.RESET + ": %(message)s"
    )

    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger


def bright(string):
    return colorama.Style.BRIGHT + string + colorama.Style.NORMAL


def resolve_path_or_str(path_or_str):
    """

    :param path_or_str:             String representing a path to a file, or
    :return:
    """
    try:
        with open(path_or_str, "rb") as _file:
            data = _file.read()
    except OSError:
        data = path_or_str
        if isinstance(data, str):
            data = data.encode("utf8")

    return data
