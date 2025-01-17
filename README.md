# Description

HUSFORT is "Handy & Utility Solution For Operation, Research and Trading" for short.

This project is designed to provide some highly frequently used tools in quantitative trading fields, especially for individual developer and(or) investor.

HUSFORT 是 "Handy & Utility Solution For Operation, Research and Trading" 的缩写.

本项目旨在提供一些量化交易工作中常用的工具包, 尤其适合个人开发者/投资者使用.

---

## Build and Install

### Windows

```powershell
.\install.ps1
```

### Linux

```bash
chmod u+x ./install.sh
./install.sh
```

---

## Module Description

### qutility

#### SFG等函数终端输出渲染

使用SFG/SFR等函数使在终端中输出颜色字体

#### qtimer装饰器

为函数添加运行时间监测等功能

#### check_and_mkdir等函数

创建,删除文件和文件夹

#### error_handler

处理多进程中抛出的错误, 否则可能无法得到正确的回报.

#### batched

分批处理可迭代的数据

```python
from qtools_sxzq.qwidgets import SFG

print(f"This output is normal, {SFG('but this output is green')}")
```

---

### qcalendar

提供一个CCalendar类,处理日频的交易日期

```python
from qtools_sxzq.qcalendar import CCalendar

calendar = CCalendar("calendar.csv")
trade_dates = calendar.get_iter_list(bgn_date="20120104", stp_date="20120111")
print(trade_dates)
```

输出

```bash
    ['20120104', '20120105', '20120106', '20120109', '20120110']
```

更多用法请参考该类的方法.


---

### qsqlite

提供一个`CMgrSqlDb` `CDbStruct`等类来管理`sqldb`数据库

```python
import pandas as pd
import scipy.stats as sps
import random


def create_data(nrow: int, ncol: int, cnames: list[str], hist_dates: list[str],
                random_state: int = None) -> pd.DataFrame:
    _data = sps.norm.rvs(size=(nrow, ncol), loc=0, scale=1, random_state=random_state)
    _df = pd.DataFrame(data=_data, columns=cnames)
    _df["trade_date"] = hist_dates[0:nrow]
    _df["instrument"] = random.choices(population=list("abcd"), k=nrow)
    _df = _df[["trade_date"] + ["instrument"] + cnames]
    return _df


if __name__ == "__main__":
    from qtools_sxzq.qcalendar import CCalendar
    from qtools_sxzq.qsqlite import CMgrSqlDb, CSqlTable, CSqlVar

    calendar_path = r"/path/to/calendar"
    calendar = CCalendar(calendar_path)
    h_dates = calendar.get_iter_list(bgn_date="20120101", stp_date="20250101")

    db_save_dir, db_name = r"/tmp", "test.db"
    table_name = "testTable"
    nr, nc = 20, 5
    cnms = [f"C{_:02d}" for _ in range(nc)]
    df = create_data(nr * 2, nc, cnames=cnms, hist_dates=h_dates)
    df_head, df_tail = df.head(nr), df.tail(nr).reset_index(drop=True)
    table = CSqlTable(
        name=table_name,
        primary_keys=[CSqlVar("trade_date", "TEXT"), CSqlVar("instrument", "TEXT")],
        value_columns=[CSqlVar(_, "REAL") for _ in cnms]
    )
    sql_lib = CMgrSqlDb(
        db_save_dir=db_save_dir,
        db_name=db_name,
        table=table,
        mode="w",
        verbose=True,
    )

    # --- first writing
    append_date = df_head["trade_date"].iloc[0]
    if sql_lib.check_continuity(incoming_date=append_date, calendar=calendar) == 0:
        sql_lib.update(df_head)
        df0 = sql_lib.read()
        print(f"The original data length = {len(df0)} ")
        print(df0)

    # --- appending
    append_date = df_tail["trade_date"].iloc[0]
    if sql_lib.check_continuity(incoming_date=append_date, calendar=calendar) == 0:
        sql_lib.update(df_tail)
        print("Append data to lib")
        df0 = sql_lib.read()
        print(f"After appending, data length = {len(df0)} ")
        print(df0)

    # --- head and tail
    print("The first 5 rows from the data, with columns=['trade_date', 'C00']")
    print(sql_lib.head(value_columns=["trade_date", "C00"]))
    print("The last 10 rows from the data")
    print(sql_lib.tail(n=10))

    # --- query
    df1 = sql_lib.read_by_conditions(conditions=[("trade_date", "<=", "20120131")])
    print("Query: trade_date <= '20120131'")
    print(df1)

    df3 = sql_lib.read_by_conditions(conditions=[("instrument", "=", "d"), ("trade_date", "<", "20120205")])
    print("Query: (instrument = 'd') AND (trade_date < '20120205')")
    print(df3)

    # --- continuity check
    sql_lib.check_continuity(incoming_date="20120306", calendar=calendar)
    sql_lib.check_continuity(incoming_date="20120307", calendar=calendar)
    sql_lib.check_continuity(incoming_date="20120308", calendar=calendar)
```

更多用法请参考该类的方法.

---

### qh5

类比qsqlite, 提供一个管理h5文件的类

---

### qplot

`matplotlib`基础上进一步封装的绘图函数, 在Pycharm/VSCode智能提示加持下, 绘图参数更加清晰.

以下示例

#### 生成数据

```python
import pandas as pd
from random import randint

test_size = 60
data = pd.DataFrame(
    {
        "T": [str(_) for _ in range(2014, 2014 + test_size)],
        "x": [randint(95, 105) for _ in range(test_size)],
        "y": [randint(95, 105) for _ in range(test_size)],
    }
).set_index("T")
```

#### 绘图

```python
from qtools_sxzq.qplot import CPlotLines

my_artist = CPlotLines(
    plot_data=data,
    fig_name="test-qplot-lines",  # 保存图片文件名
    fig_save_dir=r"/tmp",  # 保存位置
    line_width=4,  # 线条宽度
    line_style=["-", "-."],  # 线条样式
    line_color=["#DC143C", "#228B22"],  # 线条颜色,可以用utility.view_colors查看更多颜色
)
my_artist.plot()
my_artist.set_legend(size=16, loc="upper left")  # 设置图例
my_artist.set_axis_x(  # 设置x轴
    xtick_count=10,  # x轴标签数量
    # xtick_spread=20, # x轴标签间距,与数量不要同时设置
    xlabel="Test-XLabels",  # x轴标签
    xtick_label_size=24,  # x轴刻度标签大小
    xtick_label_rotation=45,  # x轴刻度标签旋转角度
)
my_artist.set_title(title="test-qplot-lines", size=48, loc="left")
my_artist.save_and_close()
```

**绘图时请确认data是pd.DataFrame, 且索引index是字符串格式,否则set_axis_x()函数可能不会正常运行**

---

### qevaluation

回测绩效评估模块, 计算收益率(净值)曲线的常见风险收益指标

#### 生成模拟的收益率序列

```python
import scipy.stats as sps
import pandas as pd

n = 250
ret_val = sps.norm.rvs(size=n, loc=0.001, scale=0.01, random_state=0)
start_date = pd.Timestamp.now()
date_range = pd.date_range(start=start_date, periods=n)
ret_srs = pd.Series(data=ret_val, index=date_range)
print(ret_srs)
```

输出结果

```bash
2025-01-17 10:42:37.590840    0.018641
2025-01-18 10:42:37.590840    0.005002
2025-01-19 10:42:37.590840    0.010787
2025-01-20 10:42:37.590840    0.023409
2025-01-21 10:42:37.590840    0.019676
                                ...
2025-09-19 10:42:37.590840   -0.015760
2025-09-20 10:42:37.590840    0.012523
2025-09-21 10:42:37.590840    0.011796
2025-09-22 10:42:37.590840   -0.007134
2025-09-23 10:42:37.590840   -0.013664
```

调用`qevaluation`计算指标

```python
import pandas as pd
from qtools_sxzq.qevaluation import CNAV

nav = CNAV(input_srs=ret_srs, input_type="RET")
nav.cal_all_indicators()
sum_data = nav.to_dict()

pd.set_option("display.float_format", "{:.6f}".format)
print(pd.Series(sum_data))
```

输出结果

```bash
retMean                        0.001318 # 收益率均值
retStd                         0.009981 # 收益率标准差
hpr                            0.372830 # 持有期收益
retAnnual                      0.329452 # 年华收益
volAnnual                      0.157813 # 年化波动
sharpe                         2.087605 # 夏普比率
calmar                         2.758407 # 卡玛比率
mdd                            0.119436 # 最大回撤
mddT         2025-04-07 10:42:37.590840 # 最大回撤结束时间
lddDur                               49 # 最长回撤期
lddDurT      2025-04-07 10:42:37.590840 # 最长回撤期结束时间
lrd                                  68 # 最长恢复期
lrdT         2025-04-26 10:42:37.590840 # 最长恢复期结束时间
```

---

### qinstruments

提供一个类来管理管理期货合约要素

---

### qlog

提供一个自定义的格式logger类

---

### qsimulation

提供一个高精度日频策略测试

---

### qmails

提供一个自动收发邮件的类
