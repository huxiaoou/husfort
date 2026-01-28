import argparse
from dataclasses import dataclass
from rich.console import Console
from rich.table import Table


@dataclass(frozen=True)
class ColorRGB:
    r: int
    g: int
    b: int


@dataclass(frozen=True)
class Block:
    hex_color: str
    width: int
    height: int

    @property
    def color_rgb(self) -> ColorRGB:
        hex_6d = self.hex_color.lstrip("#")
        r = int(hex_6d[0:2], 16)
        g = int(hex_6d[2:4], 16)
        b = int(hex_6d[4:6], 16)
        return ColorRGB(r, g, b)

    def color_rgb_as_string(self) -> str:
        rgb = self.color_rgb
        return f"({rgb.r}, {rgb.g}, {rgb.b})"


@dataclass(frozen=True)
class Palette:
    name: str
    blocks: list[Block]


def plot_palette(palette: Palette) -> None:
    theme_color = "#32CD32"
    table = Table(
        title=f"Palette: {palette.name}",
        title_style=f"bold {theme_color}",
        header_style=f"bold {theme_color}",
    )
    table.add_column(header="HEX", justify="right", style="#FFFAFA")
    table.add_column(header="RGB", justify="right", style="#4169E1")
    table.add_column(header="EXAMPLE", justify="right", style=theme_color)
    for block in palette.blocks:
        hex_color, rgb_color = block.hex_color, block.color_rgb_as_string()
        for _ in range(block.height):
            text_hex, text_rgb = (hex_color, rgb_color) if _ == 0 else ("", "")
            table.add_row(
                text_hex,
                text_rgb,
                f"[{hex_color} on {hex_color}]{'a':>{block.width}s}",
            )
    console = Console()
    console.print(table)
    console.print()  # Add an empty line between palettes
    return


def get_palettes(data: dict[str, list[str]], width: int, height: int) -> list[Palette]:
    palettes: list[Palette] = []
    for palette_name, hex_colors in data.items():
        p = Palette(
            name=palette_name,
            blocks=[Block(hex_color=hex_color, width=width, height=height) for hex_color in hex_colors],
        )
        palettes.append(p)
    return palettes


def main(palettes: list[Palette]) -> None:
    for palette in palettes:
        plot_palette(palette)
    return


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Display color palettes in the terminal.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--src",
        type=str,
        default=None,
        help="A yaml file to privde color palettes (default: None), an example is like this\n"
        "palettes:\n"
        "  Classic Tavern:\n"
        "    - '#5d432c'\n"
        "    - '#c8782e'\n"
        "  Enchanted Forest:\n"
        "    - '#1a3b2d'\n"
        "    - '#6a994e'\n"
        "  Gothic Fortress:\n"
        "    - '#2d2d2d'\n"
        "    - '#8a9597'\n",
    )
    parser.add_argument(
        "--width",
        type=int,
        default=32,
        help="Width of each color block (default: 32)",
    )
    parser.add_argument(
        "--height",
        type=int,
        default=1,
        help="Height of each color block (default: 1)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()

    if args.src:
        import yaml

        with open(args.src, "r") as f:
            _config = yaml.safe_load(f)
        data = _config.get("palettes", {})
    else:
        data = {
            "Classic Tavern": ["#5d432c", "#c8782e", "#e8d8c9", "#2a4c3f", "#8a1b1b"],
            "Enchanted Forest": ["#1a3b2d", "#6a994e", "#4a3728", "#a2c5cc", "#9d4edd"],
            "Gothic Fortress": ["#2d2d2d", "#8a9597", "#4a6479", "#6d597a", "#b7410e"],
        }

    palettes = get_palettes(data=data, width=args.width, height=args.height)
    main(palettes=palettes)
