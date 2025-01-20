if __name__ == "__main__":
    import pandas as pd
    from random import randint
    from husfort.qplot import CPlotLines, CPlotBarsV, CPlotBarsH

    test_size = 60
    test_save_dir = r"E:\TMP"

    data = pd.DataFrame(
        {
            "T": [str(_) for _ in range(2014, 2014 + test_size)],
            "x": [randint(95, 105) for _ in range(test_size)],
            "y": [randint(95, 105) for _ in range(test_size)],
        }
    ).set_index("T")

    # --- test for plot lines
    my_artist = CPlotLines(
        plot_data=data,
        fig_name="test-qplot-lines",
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
    my_artist.set_title(title="test-qplot-lines", size=48, loc="left")
    my_artist.save_and_close()

    # --- test for plot bars
    my_artist = CPlotBarsV(
        plot_data=data,
        fig_name="test-qplot-bars",
        fig_save_dir=test_save_dir,
        bar_color=["#000080", "#4682B4"],
        bar_width=0.8,
        bar_alpha=0.6,
        stacked=False,
        align="edge",
    )
    my_artist.plot()
    my_artist.set_legend(size=16, loc="upper center")
    my_artist.set_axis_x(
        xtick_count=16,
        xlabel="Test-XLabels",
        xtick_label_size=20,
        xtick_label_rotation=90,
        using_index_as_x=True,
    )
    my_artist.set_axis_y(ylim=(0, 120), update_yticklabels=False)
    my_artist.set_title(title="test-qplot-bars", size=36, loc="center")
    my_artist.save_and_close()

    # --- test for plot bars horizontal
    my_artist = CPlotBarsH(
        plot_data=data,
        fig_name="test-qplot-barsH",
        fig_save_dir=test_save_dir,
        bar_color=["#008B8B", "#2F4F4F"],
        bar_width=0.8,
        bar_alpha=0.6,
        stacked=False,
        align="edge",
    )
    my_artist.plot()
    my_artist.set_legend(size=16, loc="upper right")
    my_artist.set_axis_y(
        ytick_count=16,
        ylabel="Test-YLabels",
        ytick_label_size=20,
        ytick_label_rotation=0,
        using_columns_as_y=False,
    )
    my_artist.set_axis_x(xlim=(0, 120), update_xticklabels=False)
    my_artist.set_title(title="test-qplot-barsH", size=36, loc="center")
    my_artist.save_and_close()
