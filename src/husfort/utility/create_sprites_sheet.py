import os
import argparse
from PIL import Image
from husfort.qlog import define_logger, logger
from husfort.qutility import SFG, SFY

define_logger()


def parse_args():
    arg_parser = argparse.ArgumentParser("A program to generate sprite sheets")
    arg_parser.add_argument("src", type=str, help="src directory")
    arg_parser.add_argument("--save", type=str, required=True, help="save path of the sprite_sheet")
    arg_parser.add_argument("--ncol", type=int, default=6, help="number of cols")
    arg_parser.add_argument("--shrink", type=float, default=1.0, help="the ratio to shrink")
    arg_parser.add_argument("--width", type=int, default=None,
                            help="if provided, will be used to set unit cell width (in pixel) for png")
    arg_parser.add_argument("--height", type=int, default=None,
                            help="if provided, will be used to set unit cell height (in pixel) for png")
    _args = arg_parser.parse_args()
    return _args


def get_pngs(src: str, reverse: bool = False) -> list[str]:
    ls = [file_path for file_path in os.listdir(src) if file_path.endswith(".png") or file_path.endswith(".PNG")]
    return sorted(ls, reverse=reverse)


def cal_nrow(tot_count: int, ncol_: int) -> int:
    _nrow, _remain = tot_count // ncol_, tot_count % ncol_
    if _remain > 0:
        _nrow += 1
    return _nrow


def get_png_size(p_path: str) -> tuple[int, int]:
    with Image.open(p_path) as _img:
        return _img.width, _img.height


if __name__ == "__main__":
    args = parse_args()
    pngs = get_pngs(args.src)
    ncol = args.ncol
    if pngs:
        png_count = len(pngs)
        nrow = cal_nrow(tot_count=png_count, ncol_=ncol)
        if args.width:
            if args.height:
                png_w, png_h = args.width, args.height
            else:
                png_w = png_h = args.width
        else:
            if args.height:
                png_w = png_h = args.height
            else:
                png_w, png_h = get_png_size(os.path.join(args.src, pngs[0]))
        img_w, img_h = png_w * ncol, png_h * nrow
        merged_image = Image.new(mode="RGBA", size=(img_w, img_h))
        for sn, png in enumerate(pngs):
            loc_col, loc_row = sn % ncol, sn // ncol
            png_path = os.path.join(args.src, png)
            with Image.open(png_path) as img:
                w, h = png_w * loc_col, png_h * loc_row
                merged_image.paste(img, box=(w, h))
                logger.info(f"{SFG(png_path)} is added at location({SFY(f'width={w:>6d}, height={h:>6d}')})")
        if args.shrink <= 1.0:
            logger.info(f"Shrinking sprite sheet to ration {args.shrink}")
            shrink_size = (int(img_w * args.shrink), int(img_h * args.shrink))
            merged_image = merged_image.resize(size=shrink_size)
        else:
            logger.warning(f"shrink ratio = {args.shrink}, which is greater than 1. And no shrinking is done")
        merged_image.save(args.save)
        logger.info(f"The sprite is saved to {SFG(args.save)}")
    else:
        print(f"There is no png available at {args.src}")
