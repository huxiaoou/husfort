import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

plt.rcParams["font.sans-serif"] = ["SimHei"]
plt.rcParams["axes.unicode_minus"] = False  # make compatible with negative or minus sign


class CPlot(object):
    def __init__(
            self,
            fig_name: str,
            fig_save_dir: str,
            fig_save_type: str = "pdf",
            fig_size: tuple = (16, 9),
            style: str = "seaborn-v0_8-poster",
    ):
        """

        :param fig_name:
        :param fig_save_dir:
        :param fig_save_type:
        :param fig_size:
        :param style: more styles comes from
                      https://matplotlib.org/3.7.5/gallery/style_sheets/style_sheets_reference.html
        """
        self.fig_name = fig_name
        self.fig_save_dir = fig_save_dir
        self.fig_save_type = fig_save_type
        self.fig_size = fig_size
        self.style = style
        plt.style.use(self.style)
        self.fig = plt.figure(figsize=self.fig_size)
        self.ax = plt.axes()

    def set_title(self, title: str = None, size: int = 32, loc: str = "center"):
        """

        :param title:
        :param size:
        :param loc: {'center', 'left', 'right'}
        :return:
        """
        self.ax.set_title(title, fontsize=size, loc=loc)
        return 0

    def set_legend(self, size: int = 12, loc: str | None = "upper left"):
        """

        :param loc: use 'loc = None' to remove legend
        :param size:
        :return:
        """
        if loc is None:
            self.ax.get_legend().remove()
        else:
            self.ax.legend(loc=loc, fontsize=size)
        return 0

    def set_tick_label_font(self, tick_label_font: str = "Times New Roman"):
        self.ax.tick_params(axis="both", fontname=tick_label_font)
        return 0

    def set_axis_x(
            self,
            xlim: tuple = (None, None),
            xtick_spread: float = None,
            xtick_count: int = 10,
            xlabel: str = None,
            xlabel_size: int = 12,
            xtick_label_size: int = 12,
            xtick_label_rotation: int = 0,
            xtick_direction: str = "in",
            xgrid_visible: bool = False,
    ):
        if xlim != (None, None):
            x_range = xlim[1] - xlim[0]
            if xtick_spread:
                xticks = np.arange(xlim[0], xlim[1], xtick_spread)
            elif xtick_count:
                xticks = np.arange(xlim[0], xlim[1], x_range / xtick_count)
            else:
                xticks = None
            if xticks is not None:
                self.ax.set_xticks(xticks)
        else:
            # use default xticks
            pass
        self.ax.set_xlabel(xlabel, fontsize=xlabel_size)
        self.ax.set_xlim(xlim[0], xlim[1])
        self.ax.tick_params(
            axis="x",
            labelsize=xtick_label_size,
            rotation=xtick_label_rotation,
            direction=xtick_direction,
        )
        self.ax.grid(visible=xgrid_visible, axis="x")
        return 0

    def set_axis_y(
            self,
            ylim: tuple = (None, None),
            ytick_spread: float = None,
            ytick_count: int = 10,
            ylabel: str = None,
            ylabel_size: int = 12,
            ytick_label_size: int = 12,
            ytick_label_rotation: int = 0,
            ytick_direction: str = "in",
            ygrid_visible: bool = False,
    ):
        if ylim != (None, None):
            y_range = ylim[1] - ylim[0]
            if ytick_spread:
                yticks = np.arange(ylim[0], ylim[1], ytick_spread)
            elif ytick_count:
                yticks = np.arange(ylim[0], ylim[1], y_range / ytick_count)
            else:
                yticks = None
            if yticks is not None:
                self.ax.set_yticks(yticks)
        else:
            # use default yticks
            pass
        self.ax.set_ylabel(ylabel, fontsize=ylabel_size)
        self.ax.set_ylim(ylim[0], ylim[1])
        self.ax.tick_params(
            axis="y",
            labelsize=ytick_label_size,
            rotation=ytick_label_rotation,
            direction=ytick_direction,
        )
        self.ax.grid(visible=ygrid_visible, axis="y")
        return 0

    def add_text(self, x: float | int, y: float | int, text: str, size: int = 12):
        self.ax.text(x, y, s=text, fontdict={"size": size})
        return 0

    def save(self):
        fig0_name = f"{self.fig_name}.{self.fig_save_type}"
        fig0_path = os.path.join(self.fig_save_dir, fig0_name)
        self.fig.savefig(fig0_path, bbox_inches="tight")
        return 0

    def close(self):
        plt.close(self.fig)
        return 0

    def plot(self):
        raise NotImplementedError

    def save_and_close(self):
        self.save()
        self.close()
        return 0


class CPlotFromDataFrame(CPlot):
    def __init__(
            self,
            plot_data: pd.DataFrame,
            fig_name: str,
            fig_save_dir: str,
            fig_save_type: str = "pdf",
            fig_size: tuple = (16, 9),
            style: str = "seaborn-v0_8-poster",
            colormap: str = None,
    ):
        """

        :param plot_data: A dataframe with columns to plot, and xticklabels are from index
        :param colormap:
        """
        self.plot_data = plot_data
        self.data_len = len(plot_data)
        self.colormap = colormap
        super().__init__(
            fig_name=fig_name,
            fig_save_dir=fig_save_dir,
            fig_save_type=fig_save_type,
            fig_size=fig_size,
            style=style,
        )

    def set_axis_x(
            self,
            xlim: tuple = (None, None),
            xtick_spread: float = None,
            xtick_count: int = 10,
            xlabel: str = None,
            xlabel_size: int = 12,
            xtick_label_size: int = 12,
            xtick_label_rotation: int = 0,
            xgrid_visible: bool = False,
    ):
        other_kwargs = {
            "xtick_spread": xtick_spread,
            "xtick_count": xtick_count,
            "xlabel": xlabel,
            "xlabel_size": xlabel_size,
            "xtick_label_size": xtick_label_size,
            "xtick_label_rotation": xtick_label_rotation,
            "xgrid_visible": xgrid_visible,
        }
        if xlim == (None, None):
            super().set_axis_x(xlim=(0, self.data_len), **other_kwargs)
        else:
            super().set_axis_x(xlim=xlim, **other_kwargs)
        xticklabels = self.plot_data.index[self.ax.get_xticks().astype(int)]
        self.ax.set_xticklabels(xticklabels)
        return 0

    def add_vlines_from_index(self, vlines_index: list, color: str = "r", style: str = "dashed"):
        if vlines_index:
            xlocs = [self.plot_data.index.get_loc(z) for z in vlines_index]
            ymin, ymax = self.ax.get_ylim()
            self.ax.vlines(xlocs, ymin=ymin, ymax=ymax, colors=color, linestyles=style)
        return 0

    def add_hlines_from_value(self, hlines_value: list, color: str = "r", style: str = "dashed"):
        if hlines_value:
            xmin, xmax = self.ax.get_xlim()
            self.ax.hlines(hlines_value, xmin=xmin, xmax=xmax, colors=color, linestyles=style)
        return 0


class CPlotLines(CPlotFromDataFrame):
    def __init__(
            self,
            plot_data: pd.DataFrame,
            fig_name: str,
            fig_save_dir: str,
            fig_save_type: str = "pdf",
            fig_size: tuple = (16, 9),
            style: str = "seaborn-v0_8-poster",
            colormap: str = None,
            line_width: float = 2,
            line_style: list = None,
            line_color: list = None,
    ):
        """

        :param plot_data: A dataframe with columns to plot,
        :param colormap:
        :param line_width:
        :param line_style: one or more ('-', '--', '-.', ':')
        :param line_color: if this parameter is used, then
                           DO NOT use colormap and
                           DO NOT specify colors in line_style

                           could be str, array-like, or dict, optional

                           The color for each of the DataFrame’s columns. Possible values are:

                           A single color string referred to by name, RGB or RGBA code, for
                           instance ‘red’ or ‘#a98d19’.

                           A sequence of color strings referred to by name, RGB or RGBA code,
                           which will be used for each column recursively.
                           For instance ['green', 'yellow'] each column’s line will be filled
                           in green or yellow, alternatively.
                           If there is only a single column to be plotted, then only the first
                           color from the color list will be used.

                           A dict of the form {column_name:color}, so that each column will be
                           colored accordingly. For example, if your columns are called a and b,
                           then passing {‘a’: ‘green’, ‘b’: ‘red’} will color lines for column
                           'a' in green and lines for column 'b' in red.

                           short name for color {
                                'b':blue,
                                'g':green,
                                'r':red,
                                'c':cyan,
                                'm':magenta,
                                'y':yellow,
                                'k':black,
                                'w':white,
                            }
        """

        self.line_width = line_width
        self.line_style = line_style
        self.line_color = line_color
        super().__init__(
            plot_data=plot_data,
            fig_name=fig_name,
            fig_save_dir=fig_save_dir,
            fig_save_type=fig_save_type,
            fig_size=fig_size,
            style=style,
            colormap=colormap,
        )

    def plot(self):
        if self.line_color:
            self.plot_data.plot.line(
                ax=self.ax,
                lw=self.line_width,
                style=self.line_style or "-",
                color=self.line_color,
            )
        elif self.colormap:
            self.plot_data.plot.line(
                ax=self.ax,
                lw=self.line_width,
                style=self.line_style or "-",
                colormap=self.colormap,
            )
        else:
            self.plot_data.plot.line(
                ax=self.ax,
                lw=self.line_width,
                style=self.line_style or "-",
            )
        return 0


if __name__ == "__main__":
    from random import randint

    test_size = 60
    test_save_dir = r"E:\TMP"

    data = pd.DataFrame(
        {
            "T": [str(_) for _ in range(2014, 2014 + test_size)],
            "x": [randint(95, 105) for _ in range(test_size)],
            "y": [randint(95, 105) for _ in range(test_size)],
        }
    ).set_index("T")
    my_artist = CPlotLines(
        plot_data=data,
        fig_name="Test",
        fig_save_dir=test_save_dir,
        line_width=4,
        line_style=["-", "-."],
        line_color=["#DC143C", "#228B22"],
    )
    my_artist.plot()
    my_artist.set_legend(size=16, loc="upper left")
    my_artist.set_axis_x(
        xtick_count=10,
        xlabel="Test-XLabels",
        xtick_label_size=24,
        xtick_label_rotation=45,
    )
    my_artist.set_title(title="Test-Tile", size=48, loc="left")
    my_artist.save_and_close()
