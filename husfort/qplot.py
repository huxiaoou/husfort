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
                 style: str = "seaborn-v0_8-poster",
                 fig_save_dir: str = ".", fig_save_type: str = "pdf"):
        self.fig_size = fig_size
        self.fig_name = fig_name
        self.style = style
        self.fig_save_dir = fig_save_dir
        self.fig_save_type = fig_save_type
        self.fig: plt.Figure | None = None
        self.ax: plt.Axes | None = None

    def _set_title(self, title: str = None, size: int = 32, loc: str = None):
        self.ax.set_title(title, fontsize=size, loc=loc)
        return 0

    def _set_legend(self, size: int = 12, loc: str = "upper left"):
        """

        :param loc: use loc = None to remove legend
        :param size:
        :return:
        """
        if loc is not None:
            self.ax.legend(loc=loc, fontsize=size)
        else:
            self.ax.get_legend().remove()
        return 0

    def _set_tick_label_font(self, tick_label_font: str = "Times New Roman"):
        self.ax.tick_params(axis="both", fontname=tick_label_font)
        return 0

    def _set_axis_x(self,
                    xlim: tuple = (None, None),
                    xtick_spread: float = None, xtick_count: int = 10,
                    xlabel: str = None, xlabel_size: int = 12,
                    xtick_label_size: int = 12, xtick_label_rotation: int = 0,
                    xgrid_visible: bool = False
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
        self.ax.tick_params(axis="x", labelsize=xtick_label_size, rotation=xtick_label_rotation)
        self.ax.grid(visible=xgrid_visible, axis="x")
        return 0

    def _set_axis_y(self,
                    ylim: tuple = (None, None),
                    ytick_spread: float = None, ytick_count: int = 10,
                    ylabel: str = None, ylabel_size: int = 12,
                    ytick_label_size: int = 12, ytick_label_rotation: int = 0,
                    ygrid_visible: bool = False
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
        self.ax.tick_params(axis="y", labelsize=ytick_label_size, rotation=ytick_label_rotation)
        self.ax.grid(visible=ygrid_visible, axis="y")
        return 0

    def open(self):
        plt.style.use(self.style)
        self.fig, self.ax = plt.subplots(figsize=self.fig_size)
        return 0

    def plot(self, title: str = None, title_size: int = 32, title_loc: str = None,
             legend_size: int = 12, legend_loc: str = "upper left",

             xlim: tuple = (None, None),
             xtick_spread: float = None, xtick_count: int = 10,
             xlabel: str = None, xlabel_size: int = 12,
             xtick_label_size: int = 12, xtick_label_rotation: int = 0,
             xgrid_visible: bool = False,

             ylim: tuple = (None, None),
             ytick_spread: float = None, ytick_count: int = 10,
             ylabel: str = None, ylabel_size: int = 12,
             ytick_label_size: int = 12, ytick_label_rotation: int = 0,
             ygrid_visible: bool = False

             ):
        self._set_title(title=title, size=title_size, loc=title_loc)
        self._set_legend(size=legend_size, loc=legend_loc)
        self._set_axis_x(xlim=xlim, xtick_spread=xtick_spread, xtick_count=xtick_count,
                         xlabel=xlabel, xlabel_size=xlabel_size,
                         xtick_label_size=xtick_label_size, xtick_label_rotation=xtick_label_rotation,
                         xgrid_visible=xgrid_visible)
        self._set_axis_y(ylim=ylim, ytick_spread=ytick_spread, ytick_count=ytick_count,
                         ylabel=ylabel, ylabel_size=ylabel_size,
                         ytick_label_size=ytick_label_size, ytick_label_rotation=ytick_label_rotation,
                         ygrid_visible=ygrid_visible
                         )
        return 0

    def close(self):
        fig0_name = f"{self.fig_name}.{self.fig_save_type}"
        fig0_path = os.path.join(self.fig_save_dir, fig0_name)
        self.fig.savefig(fig0_path, bbox_inches="tight")
        plt.close(self.fig)
        return 0


class CPlotFromDataFrame(CPlotBase):
    def __init__(self, plot_df: pd.DataFrame, colormap: str = None, **kwargs):
        self.plot_df = plot_df
        self.data_len = len(plot_df)
        self.colormap = colormap
        super().__init__(**kwargs)

    def _set_axis_x(self, xlim: tuple = (None, None), **kwargs):
        if xlim == (None, None):
            super()._set_axis_x(xlim=(0, self.data_len), **kwargs)
        else:
            super()._set_axis_x(xlim=xlim, **kwargs)
        xticklabels = self.plot_df.index[self.ax.get_xticks().astype(int)]
        self.ax.set_xticklabels(xticklabels)
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

    def plot(self, **kwargs):
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
        super().plot(**kwargs)
        return 0


class CPlotBars(CPlotFromDataFrame):
    def __init__(self, bar_color: list = None, bar_width: float = 0.8, bar_alpha: float = 1.0,
                 stacked: bool = False, **kwargs):
        self.bar_color = bar_color
        self.bar_width = bar_width
        self.bar_alpha = bar_alpha
        self.stacked = stacked
        super().__init__(**kwargs)

    def plot(self, **kwargs):
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
        super().plot(**kwargs)
        return 0


class CPlotLinesTwinx(CPlotLines):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ax_twin: plt.Axes | None = None

    def _set_axis_y_twin(self,
                         ylim_twin: tuple = (None, None),
                         ytick_count_twin: int = 10, ytick_spread_twin: float = None,
                         ylabel_twin: str = None, ylabel_size_twin: int = 12,
                         ytick_label_size_twin: int = 12, ytick_label_rotation_twin: int = 0,
                         ygrid_visible_twin: bool = False
                         ):
        if ylim_twin != (None, None):
            y_range = ylim_twin[1] - ylim_twin[0]
            if ytick_spread_twin:
                yticks = np.arange(ylim_twin[0], ylim_twin[1], ytick_spread_twin)
            elif ytick_count_twin:
                yticks = np.arange(ylim_twin[0], ylim_twin[1], y_range / ytick_count_twin)
            else:
                yticks = None
            if yticks is not None:
                self.ax_twin.set_yticks(yticks)
        else:
            # use default yticks
            pass
        self.ax_twin.set_ylabel(ylabel_twin, fontsize=ylabel_size_twin)
        self.ax_twin.set_ylim(ylim_twin[0], ylim_twin[1])
        self.ax_twin.tick_params(axis="y", labelsize=ytick_label_size_twin, rotation=ytick_label_rotation_twin)
        self.ax_twin.grid(visible=ygrid_visible_twin, axis="y")
        return 0

    def _set_legend(self, loc: str = "upper left", size: int = 12):
        if loc is not None:
            lines0, labels0 = self.ax.get_legend_handles_labels()
            lines1, labels1 = self.ax_twin.get_legend_handles_labels()
            self.ax.legend(lines0 + lines1, labels0 + labels1, loc=loc)
        self.ax_twin.get_legend().remove()
        return 0

    def plot(self, ylim_twin: tuple = (None, None),
             ytick_count_twin: int = 10, ytick_spread_twin: float = None,
             ylabel_twin: str = None, ylabel_size_twin: int = 12,
             ytick_label_size_twin: int = 12, ytick_label_rotation_twin: int = 0,
             ygrid_visible_twin: bool = False,
             **kwargs):
        super().plot(**kwargs)
        self._set_axis_y_twin(
            ylim_twin=ylim_twin,
            ytick_count_twin=ytick_count_twin, ytick_spread_twin=ytick_spread_twin,
            ylabel_twin=ylabel_twin, ylabel_size_twin=ylabel_size_twin,
            ytick_label_size_twin=ytick_label_size_twin, ytick_label_rotation_twin=ytick_label_rotation_twin,
            ygrid_visible_twin=ygrid_visible_twin
        )
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

    def plot(self, **kwargs):
        self.ax_twin = self.ax.twinx()
        if self.second_line_color:
            self.second_line_df.plot.line(ax=self.ax_twin, lw=self.second_line_width,
                                          style=self.second_line_style if self.line_style else "-",
                                          color=self.second_line_color)
        else:
            self.second_line_df.plot.line(ax=self.ax_twin, lw=self.second_line_width,
                                          style=self.second_line_style if self.line_style else "-",
                                          colormap=self.second_colormap)
        super().plot(**kwargs)
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

    def plot(self, **kwargs):
        self.ax_twin = self.ax.twinx()
        if self.bar_color:
            self.bar_df.plot.bar(
                ax=self.ax_twin, color=self.bar_color, width=self.bar_width, alpha=self.bar_alpha)
        else:
            self.bar_df.plot.bar(
                ax=self.ax_twin, colormap=self.bar_colormap, width=self.bar_width, alpha=self.bar_alpha)
        super().plot(**kwargs)
        return 0


class CPlotSingleNavWithDrawdown(CPlotLinesTwinxBar):
    def __init__(self, nav_srs: pd.Series, nav_label: str, drawdown_label: str,
                 nav_line_color: list = None, nav_line_width: float = 2.0,
                 drawdown_color: list = None, drawdown_alpha: float = 0.6,
                 **kwargs):
        self.lbl_nav, self.lbl_dd = nav_label, drawdown_label
        self.nav_and_drawdown_df = pd.DataFrame({
            self.lbl_nav: nav_srs,
            self.lbl_dd: 1 - nav_srs / nav_srs.cummax(),
        })
        super().__init__(
            plot_df=self.nav_and_drawdown_df, primary_cols=[self.lbl_nav], secondary_cols=[self.lbl_dd],
            line_width=nav_line_width, line_color=nav_line_color,
            bar_color=drawdown_color, bar_alpha=drawdown_alpha,
            **kwargs)

    def plot(self, ylim_twin: tuple = (None, None), **kwargs):
        if ylim_twin == (None, None):
            drawdown_ylim = (self.nav_and_drawdown_df[self.lbl_dd].max() * 5, 0)
        else:
            drawdown_ylim = ylim_twin
        super().plot(ylim_twin=drawdown_ylim, **kwargs)
        return 0


class CPlotScatter(CPlotFromDataFrame):
    def __init__(self, point_x: str, point_y: str, point_size=None, point_color=None,
                 annotations_using_index: bool = False, annotations: list[str] = None,
                 annotations_location_drift: tuple = (0, 0),
                 annotations_fontsize: int = 12,
                 **kwargs):
        self.point_x, self.point_y = point_x, point_y
        self.point_size, self.point_color = point_size, point_color
        self.annotations_using_index = annotations_using_index
        self.annotations = annotations
        self.annotations_location_drift = annotations_location_drift
        self.annotations_fontsize = annotations_fontsize
        super().__init__(**kwargs)

    def plot_scatter(self):
        self.plot_df.plot.scatter(ax=self.ax, x=self.point_x, y=self.point_y, s=self.point_size, c=self.point_color)
        if self.annotations_using_index:
            self.annotations = self.plot_df.index.tolist()
        if self.annotations:
            for loc_x, loc_y, label in zip(self.plot_df[self.point_x], self.plot_df[self.point_y], self.annotations):
                xytext = (loc_x + self.annotations_location_drift[0], loc_y + self.annotations_location_drift[1])
                self.ax.annotate(label, xy=(loc_x, loc_y), xytext=xytext, fontsize=self.annotations_fontsize)
        return 0


if __name__ == "__main__":
    n = 252 * 5
    df = pd.DataFrame({
        "T": [f"T{_:03d}" for _ in range(n)],
        "上证50": np.cumprod((np.random.random(n) * 2 - 1) / 100 + 1),
        "沪深300": np.cumprod((np.random.random(n) * 2 - 1) / 100 + 1),
        "中证500": np.cumprod((np.random.random(n) * 2 - 1) / 100 + 1),
        "南华商品": np.cumprod((np.random.random(n) * 2 - 1) / 100 + 1),
        "TEST": np.random.random(n) * 2 - 1,
        "TEST2": np.random.random(n) * 2 - 1,
    }).set_index("T")
    print(df.tail())

    # test plot lines
    artist = CPlotLines(
        plot_df=df[["沪深300", "中证500", "南华商品"]], fig_name="test-plot-lines", style="seaborn-v0_8-poster",
        line_style=['-', '--', '-.', ':', ], line_width=2,
        line_color=['#A62525', '#188A06', '#06708A', '#DAF90E'],
        fig_save_dir="E:\\TMP"
    )
    artist.open()
    artist.plot(
        xtick_label_size=16, xlabel='xxx', xtick_count=3,
        ylim=(-0.5, 2.0), ytick_label_size=16, ylabel='yyy', ytick_spread=0.25,
        title="指数走势"
    )
    artist.close()

    # test plot bars
    artist = CPlotBars(
        plot_df=df[["TEST", "TEST2"]], fig_name="test-plot-bars", style="seaborn-v0_8-poster",
        bar_alpha=1.0, bar_width=1.0, bar_color=['#A62525', '#188A06', '#06708A'],
        fig_save_dir="E:\\TMP"
    )
    artist.open()
    artist.plot(
        xtick_label_size=16, xlabel='xxx', xtick_count=3, xlabel_size=32,
        ylim=(-0.5, 2.0), ytick_label_size=16, ylabel='yyy', ytick_spread=0.25,
        title="指数走势"
    )
    artist.close()

    # test plot twinx line + line
    artist = CPlotLinesTwinxLine(
        plot_df=df, primary_cols=["沪深300", "中证500"], secondary_cols=["上证50", "南华商品"],
        line_style=["-"], second_line_style=["-."],
        line_color=["r", "g"], second_line_color=["b", "y"],
        line_width=1, second_line_width=3,
        fig_name="test-plot-twin-line_line", style="seaborn-v0_8-poster",
        fig_save_dir="E:\\TMP"
    )
    artist.open()
    artist.plot(ylim=(-0.5, 2.0), ylim_twin=(0.5, 1.5))
    artist.close()

    # test plot twinx line + bar
    artist = CPlotLinesTwinxBar(
        plot_df=df, primary_cols=["沪深300", "中证500", "南华商品"], secondary_cols=["TEST"],
        bar_color=["#DC143C"], bar_width=1.0, bar_alpha=0.8,
        fig_name="test-plot-twin-line_bar", style="seaborn-v0_8-poster",
        fig_save_dir="E:\\TMP"
    )
    artist.open()
    artist.plot(xtick_count=12,
                ytick_count_twin=6, ytick_spread_twin=None, ylabel_twin="bar-test",
                ylabel_size_twin=36, ylim_twin=(-3, 3),
                ytick_label_size_twin=24, ytick_label_rotation_twin=90)
    artist.close()

    # test plot drawdown
    artist = CPlotSingleNavWithDrawdown(
        nav_srs=df["上证50"], nav_label="上证50", drawdown_label="回撤",
        nav_line_color=["#00008B"], drawdown_color=["#DC143C"],
        fig_name="test-plot-nav_drawdown", style="seaborn-v0_8-poster",
        fig_save_dir="E:\\TMP"
    )
    artist.open()
    artist.plot(legend_loc="lower center", xtick_count=12, ylim_twin=(0.50, 0), ytick_spread_twin=-0.05,
                xtick_label_size=24, ytick_label_size=24, ytick_label_size_twin=28)
    artist.close()
