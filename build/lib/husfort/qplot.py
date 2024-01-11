import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

plt.rcParams["font.sans-serif"] = ["SimHei"]
plt.rcParams["axes.unicode_minus"] = False  # make compatible with negative or minus sign
plt.rcParams["xtick.direction"] = "in"  # maker ticker direction of x-axis to inner
plt.rcParams["ytick.direction"] = "in"  # maker ticker direction of y-axis to inner


class CPlotBase(object):
    def __init__(self, fig_size: tuple = (16, 9), fig_name: str = None,
                 style: str = "seaborn-v0_8-poster", colormap: str = None,
                 fig_save_dir: str = ".", fig_save_type: str = "pdf"):
        self.fig_size = fig_size
        self.fig_name = fig_name
        self.style = style
        self.colormap = colormap
        self.fig_save_dir = fig_save_dir
        self.fig_save_type = fig_save_type
        self.fig: plt.Figure | None = None
        self.ax: plt.Axes | None = None

    def _core(self):
        pass

    def plot(self):
        plt.style.use(self.style)
        self.fig, self.ax = plt.subplots(figsize=self.fig_size)
        self._core()
        fig0_name = self.fig_name + "." + self.fig_save_type
        fig0_path = os.path.join(self.fig_save_dir, fig0_name)
        self.fig.savefig(fig0_path, bbox_inches="tight")
        plt.close(self.fig)
        return 0


class CPlotAdjustAxes(CPlotBase):

    def __init__(self, title: str = None, title_size: int = 32,
                 xtick_count: int = 10, xtick_spread: float = None, xlabel: str = None, xlabel_size: int = 12,
                 ytick_count: int = 10, ytick_spread: float = None, ylabel: str = None, ylabel_size: int = 12,
                 xlim: tuple = (None, None),
                 ylim: tuple = (None, None),
                 xtick_label_size: int = 12, xtick_label_rotation: int = 0,
                 ytick_label_size: int = 12, ytick_label_rotation: int = 0,
                 xtick_label_font: str = "Times New Roman",
                 ytick_label_font: str = "Times New Roman",
                 legend_loc: str = "upper left", legend_fontsize: int = 12,
                 **kwargs):
        self.title, self.title_size = title, title_size
        self.xtick_count, self.xtick_spread = xtick_count, xtick_spread
        self.ytick_count, self.ytick_spread = ytick_count, ytick_spread
        self.xlabel, self.ylabel = xlabel, ylabel
        self.xlabel_size, self.ylabel_size = xlabel_size, ylabel_size
        self.xlim, self.ylim = xlim, ylim
        self.xtick_label_size, self.xtick_label_rotation = xtick_label_size, xtick_label_rotation
        self.ytick_label_size, self.ytick_label_rotation = ytick_label_size, ytick_label_rotation
        self.xtick_label_font = xtick_label_font
        self.ytick_label_font = ytick_label_font
        self.legend_loc, self.legend_fontsize = legend_loc, legend_fontsize
        super().__init__(**kwargs)

    def _set_axes(self):
        if self.ylim != (None, None):
            y_range = self.ylim[1] - self.ylim[0]
            if self.ytick_spread:
                yticks = np.arange(self.ylim[0], self.ylim[1], self.ytick_spread)
            elif self.ytick_count:
                yticks = np.arange(self.ylim[0], self.ylim[1], y_range / self.ytick_count)
            else:
                yticks = None

            if yticks is not None:
                self.ax.set_yticks(yticks)

        self.ax.set_title(self.title, fontsize=self.title_size)
        self.ax.set_xlabel(self.xlabel, fontsize=self.xlabel_size)
        self.ax.set_ylabel(self.ylabel, fontsize=self.ylabel_size)
        self.ax.set_xlim(self.xlim[0], self.xlim[1])
        self.ax.set_ylim(self.ylim[0], self.ylim[1])
        self.ax.tick_params(axis="x", labelsize=self.xtick_label_size, rotation=self.xtick_label_rotation)
        self.ax.tick_params(axis="y", labelsize=self.ytick_label_size, rotation=self.ytick_label_rotation)
        if self.legend_loc is not None:
            self.ax.legend(loc=self.legend_loc, fontsize=self.legend_fontsize)
        else:
            self.ax.get_legend().remove()
        plt.xticks(fontname=self.xtick_label_font)
        plt.yticks(fontname=self.ytick_label_font)
        return 0


class CPlotFromDataFrame(CPlotAdjustAxes):
    def __init__(self, plot_df: pd.DataFrame, **kwargs):
        self.plot_df = plot_df
        self.data_len = len(plot_df)
        super().__init__(**kwargs)

    def _set_axes(self):
        if self.xtick_spread:
            xticks = np.arange(0, self.data_len, self.xtick_spread)
        elif self.xtick_count:
            xticks = np.arange(0, self.data_len, max(int((self.data_len - 1) / self.xtick_count), 1))
        else:
            xticks = None
        if xticks is not None:
            # no available for scatter plot
            xticklabels = self.plot_df.index[xticks]
            self.ax.set_xticks(xticks)
            self.ax.set_xticklabels(xticklabels)
        super()._set_axes()
        return 0


class CPlotLines(CPlotFromDataFrame):
    def __init__(self, line_width: float = 2, line_style: list = None, line_color: list = None, **kwargs):
        """

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
        :param kwargs:
        """

        self.line_width = line_width
        self.line_style = line_style
        self.line_color = line_color
        super().__init__(**kwargs)

    def _core(self):
        if self.line_color:
            self.plot_df.plot.line(
                ax=self.ax, lw=self.line_width, style=self.line_style if self.line_style else "-",
                color=self.line_color
            )
        else:
            self.plot_df.plot.line(
                ax=self.ax, lw=self.line_width, style=self.line_style if self.line_style else "-",
                colormap=self.colormap
            )
        self._set_axes()
        return 0


class CPlotBars(CPlotFromDataFrame):
    def __init__(self, bar_color: list = None, bar_width: float = 0.8, bar_alpha: float = 1.0,
                 stacked: bool = False, **kwargs):
        self.bar_color = bar_color
        self.bar_width = bar_width
        self.bar_alpha = bar_alpha
        self.stacked = stacked
        super().__init__(**kwargs)

    def _core(self):
        if self.bar_color:
            self.plot_df.plot.bar(
                ax=self.ax, color=self.bar_color, width=self.bar_width,
                alpha=self.bar_alpha, stacked=self.stacked
            )
        else:
            self.plot_df.plot.bar(
                ax=self.ax, colormap=self.colormap, width=self.bar_width,
                alpha=self.bar_alpha, stacked=self.stacked
            )
        self._set_axes()
        return 0


class CPlotScatter(CPlotFromDataFrame):
    def __init__(self, point_x: str, point_y: str, point_size=None, point_color=None,
                 annotations_using_index: bool = False, annotations: list[str] = None,
                 annotations_location_drift: tuple = (0, 0),
                 annotations_fontsize: int = 12,
                 **kwargs):
        self.point_x = point_x
        self.point_y = point_y
        self.point_size = point_size
        self.point_color = point_color
        self.annotations_using_index = annotations_using_index
        self.annotations = annotations
        self.annotations_location_drift = annotations_location_drift
        self.annotations_fontsize = annotations_fontsize
        super().__init__(**kwargs)

    def _core(self):
        self.plot_df.plot.scatter(ax=self.ax, x=self.point_x, y=self.point_y, s=self.point_size, c=self.point_color)
        if self.annotations_using_index:
            self.annotations = self.plot_df.index.tolist()
        if self.annotations:
            for loc_x, loc_y, label in zip(self.plot_df[self.point_x], self.plot_df[self.point_y], self.annotations):
                self.ax.annotate(label, xy=(loc_x, loc_y),
                                 xytext=(loc_x + self.annotations_location_drift[0],
                                         loc_y + self.annotations_location_drift[1]),
                                 fontsize=self.annotations_fontsize)
        return 0


class CPlotLinesTwinx(CPlotLines):
    def __init__(self,
                 ytick_count_twin: int = None, ytick_spread_twin: float = None, ylabel_twin: str = None,
                 ylabel_size_twin: int = 12, ylim_twin: tuple = (None, None),
                 ytick_label_size_twin: int = 12, ytick_label_rotation_twin: int = 0,
                 ygrid_visible: bool = False,
                 **kwargs):
        self.ytick_count_twin, self.ytick_spread_twin = ytick_count_twin, ytick_spread_twin
        self.ylabel_twin = ylabel_twin
        self.ylabel_size_twin = ylabel_size_twin
        self.ylim_twin = ylim_twin
        self.ytick_label_size_twin, self.ytick_label_rotation_twin = ytick_label_size_twin, ytick_label_rotation_twin
        self.ygrid_visible = ygrid_visible
        super().__init__(**kwargs)
        self.ax_twin: plt.Axes | None = None

    def __set_twinx_y_axis(self):
        if self.ylim_twin != (None, None):
            y_range = self.ylim_twin[1] - self.ylim_twin[0]
            if self.ytick_count_twin:
                yticks = np.arange(self.ylim_twin[0], self.ylim_twin[1], y_range / self.ytick_count_twin)
            elif self.ytick_spread_twin:
                yticks = np.arange(self.ylim_twin[0], self.ylim_twin[1], self.ytick_spread_twin)
            else:
                yticks = None

            if yticks is not None:
                self.ax_twin.set_yticks(yticks)

        self.ax_twin.set_ylabel(self.ylabel_twin, fontsize=self.ylabel_size_twin)
        self.ax_twin.set_ylim(self.ylim_twin[0], self.ylim_twin[1])
        self.ax_twin.tick_params(axis="y", labelsize=self.ytick_label_size_twin,
                                 rotation=self.ytick_label_rotation_twin)
        self.ax_twin.grid(visible=self.ygrid_visible, axis="y")
        return 0

    def __adjust_legend(self):
        if self.legend_loc is not None:
            lines0, labels0 = self.ax.get_legend_handles_labels()
            lines1, labels1 = self.ax_twin.get_legend_handles_labels()
            self.ax.legend(lines0 + lines1, labels0 + labels1, loc=self.legend_loc)
        self.ax_twin.get_legend().remove()
        return 0

    def _core(self):
        super()._core()
        self.__set_twinx_y_axis()
        self.__adjust_legend()
        return 0


class CPlotLinesTwinxBar(CPlotLinesTwinx):
    def __init__(self, plot_df: pd.DataFrame, primary_cols: list[str], secondary_cols: list[str],
                 bar_color: list = None, bar_width: float = 0.8, bar_alpha: float = 1.0, bar_colormap: str = None,
                 **kwargs):
        self.bar_df = plot_df[secondary_cols]
        self.ax_twin: plt.Axes | None = None
        self.bar_color = bar_color
        self.bar_colormap = bar_colormap
        self.bar_width = bar_width
        self.bar_alpha = bar_alpha
        super().__init__(plot_df=plot_df[primary_cols], **kwargs)

    def _core(self):
        self.ax_twin = self.ax.twinx()
        if self.bar_color:
            self.bar_df.plot.bar(ax=self.ax_twin, color=self.bar_color,
                                 width=self.bar_width, alpha=self.bar_alpha)
        else:
            self.bar_df.plot.bar(ax=self.ax_twin, colormap=self.bar_colormap,
                                 width=self.bar_width, alpha=self.bar_alpha)
        super()._core()
        return 0


class CPlotLinesTwinxLine(CPlotLinesTwinx):
    def __init__(self, plot_df: pd.DataFrame, primary_cols: list[str], secondary_cols: list[str],
                 second_line_width: float = 2, second_line_style: list = None, second_line_color: list = None,
                 second_colormap: str = None,
                 **kwargs):
        self.second_line_df = plot_df[secondary_cols]
        self.ax_twin: plt.Axes | None = None
        self.second_line_width = second_line_width
        self.second_line_style = second_line_style
        self.second_line_color = second_line_color
        self.second_colormap = second_colormap
        super().__init__(plot_df=plot_df[primary_cols], **kwargs)

    def _core(self):
        self.ax_twin = self.ax.twinx()
        if self.second_line_color:
            self.second_line_df.plot.line(ax=self.ax_twin, lw=self.second_line_width,
                                          style=self.second_line_style if self.line_style else "-",
                                          color=self.second_line_color)
        else:
            self.second_line_df.plot.line(ax=self.ax_twin, lw=self.second_line_width,
                                          style=self.second_line_style if self.line_style else "-",
                                          colormap=self.second_colormap)
        super()._core()
        return 0


class CPlotSingleNavWithDrawdown(CPlotLinesTwinxBar):
    def __init__(self, nav_srs: pd.Series, nav_label: str, drawdown_label: str,
                 nav_line_color: list = None, nav_line_width: float = 2.0,
                 drawdown_color: list = None, drawdown_alpha: float = 0.6,
                 **kwargs):
        drawdown_srs = 1 - nav_srs / nav_srs.cummax()
        drawdown_ylim = (drawdown_srs.max() * 5, 0)
        nav_and_drawdown_df = pd.DataFrame({
            nav_label: nav_srs,
            drawdown_label: drawdown_srs,
        })
        super().__init__(
            plot_df=nav_and_drawdown_df, primary_cols=[nav_label], secondary_cols=[drawdown_label],
            bar_color=drawdown_color, bar_alpha=drawdown_alpha,
            line_width=nav_line_width, line_color=nav_line_color,
            ylim_twin=drawdown_ylim,
            legend_loc="lower center",
            **kwargs)


if __name__ == "__main__":
    n = 252 * 5
    df = pd.DataFrame({
        "T": [f"T{_:03d}" for _ in range(n)],
        "上证50": np.cumprod((np.random.random(n) * 2 - 1) / 100 + 1),
        "沪深300": np.cumprod((np.random.random(n) * 2 - 1) / 100 + 1),
        "中证500": np.cumprod((np.random.random(n) * 2 - 1) / 100 + 1),
        "南华商品": np.cumprod((np.random.random(n) * 2 - 1) / 100 + 1),
        "TEST": np.random.random(n) * 2 - 1,
    }).set_index("T")
    print(df.tail())

    artist = CPlotLines(
        plot_df=df, fig_name="test", style="seaborn-v0_8-poster",
        # xtick_count=10, xtick_label_rotation=90,
        # ylim=(0.5, 2.1), ytick_spread=0.25,
        ylim=(-1, 2.1), ytick_count=10,
        # line_style=['-', '--', '-.', ':', ],
        # line_width=2,
        # line_color=['r', 'g', 'b'],
        line_color=['#A62525', '#188A06', '#06708A', '#DAF90E'],
        # color_map="winter",
        xtick_label_size=16, ytick_label_size=16,
        # title="指数走势", xlabel='xxx', ylabel='yyy',
        fig_save_dir="E:\\TMP"
    )
    artist.plot()

    artist = CPlotLinesTwinxBar(
        plot_df=df, primary_cols=["沪深300", "中证500", "南华商品"], secondary_cols=["TEST"],
        bar_color=["#DC143C"],
        fig_name="test_twin_bar", style="seaborn-v0_8-poster",
        xtick_count=12,
        ytick_count_twin=6, ytick_spread_twin=None, ylabel_twin="bar-test",
        ylabel_size_twin=36, ylim_twin=(-3, 3),
        ytick_label_size_twin=24, ytick_label_rotation_twin=90,
        fig_save_dir="E:\\TMP"
    )
    artist.plot()

    artist = CPlotSingleNavWithDrawdown(
        nav_srs=df["上证50"], nav_label="上证50", drawdown_label="回撤",
        nav_line_color=["#00008B"], drawdown_color=["#DC143C"],
        xtick_count=12,
        fig_name="test_nav_drawdown", style="seaborn-v0_8-poster",
        fig_save_dir="E:\\TMP"
    )
    artist.plot()
