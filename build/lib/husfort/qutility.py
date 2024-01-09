import os


def SetFontColor(c):
    def inner(s: str):
        return f"\033[{c}m{s}\033[0m"

    return inner


SFR = SetFontColor(c="0;31;40")  # Red
SFG = SetFontColor(c="0;32;40")  # Green
SFY = SetFontColor(c="0;33;40")  # Yellow
SFB = SetFontColor(c="0;34;40")  # Blue
SFM = SetFontColor(c="0;35;40")  # Magenta
SFC = SetFontColor(c="0;36;40")  # Cyan
SFW = SetFontColor(c="0;37;40")  # White


def check_and_mkdir(dir_path: str):
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)
    return 0


def check_and_makedirs(dir_path: str):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    return 0


def hide_cursor():
    print("\033[?25l", end="")
    return 0


def show_cursor():
    print("\033[?25h", end="")
    return 0


if __name__ == "__main__":
    test_string = "hello, world"
    print(SFR(test_string))
    print(SFG(test_string))
    print(SFY(test_string))
    print(SFB(test_string))
    print(SFM(test_string))
    print(SFC(test_string))
    print(SFW(test_string))
