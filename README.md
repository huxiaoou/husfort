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

After installation, a set of command-line utility scripts are also copied to `/usr/local/bin` (Linux) or a scripts directory (Windows). See the [Utility Scripts](#utility-scripts) section for details.

---

## Module Description

### qutility

通用工具函数.

#### 终端彩色输出 (SFR / SFG / SFY / SFB / SFM / SFC / SFW)

使用 `SFR`、`SFG`、`SFY` 等函数在终端中输出彩色字体 (Red / Green / Yellow / Blue / Magenta / Cyan / White).

```python
from husfort.qutility import SFG, SFR, SFY

print(f"This output is normal, {SFG('but this output is green')}")
print(f"{SFR('error')}, {SFY('warning')}")
```

#### qtimer 装饰器

为函数添加运行时间监测.

```python
from husfort.qutility import qtimer

@qtimer
def my_func():
    ...
```

#### 文件与目录操作

| 函数 | 说明 |
|------|------|
| `check_and_mkdir(dir_path)` | 目录不存在时创建 |
| `check_and_makedirs(dir_path)` | 递归创建多级目录 |
| `remove_files_in_the_dir(dir_path)` | 删除目录下所有文件 |
| `check_and_remove_tree(dir_path)` | 删除目录树 |

#### error_handler

处理多进程中抛出的错误.

```python
from husfort.qutility import error_handler
from multiprocessing import Pool

with Pool() as pool:
    pool.map_async(my_func, args, error_callback=error_handler)
```

#### batched

分批处理可迭代数据.

```python
from husfort.qutility import batched

for batch in batched(range(100), batch_size=10):
    print(list(batch))
```

---

### qcalendar

提供 `CCalendar` 类处理日频交易日期, 以及 `CCalendarSection` / `CCalendarMonth` 类处理日期区间.

```python
from husfort.qcalendar import CCalendar

calendar = CCalendar("calendar.csv")
trade_dates = calendar.get_iter_list(bgn_date="20120104", stp_date="20120111")
print(trade_dates)
# ['20120104', '20120105', '20120106', '20120109', '20120110']
```

更多用法请参考该类的方法.

---

### qsqlite

提供 `CMgrSqlDb`、`CDbStruct`、`CSqlTable`、`CSqlVar` 等类来管理 SQLite 数据库.

```python
import pandas as pd
import scipy.stats as sps
import random
from husfort.qcalendar import CCalendar
from husfort.qsqlite import CMgrSqlDb, CDbStruct, CSqlTable, CSqlVar

calendar_path = r"/path/to/calendar.csv"
calendar = CCalendar(calendar_path)
h_dates = calendar.get_iter_list(bgn_date="20120101", stp_date="20250101")

nr, nc = 20, 5
cnms = [f"C{_:02d}" for _ in range(nc)]
table = CSqlTable(
    name="testTable",
    primary_keys=[CSqlVar("trade_date", "TEXT"), CSqlVar("instrument", "TEXT")],
    value_columns=[CSqlVar(c, "REAL") for c in cnms],
)
sql_lib = CMgrSqlDb(
    db_save_dir=r"/tmp",
    db_name="test.db",
    table=table,
    mode="w",
    verbose=True,
)

# 写入数据
sql_lib.update(df)

# 读取数据
df0 = sql_lib.read()

# 条件查询
df1 = sql_lib.read_by_conditions(conditions=[("trade_date", "<=", "20120131")])

# 首尾查看
print(sql_lib.head(value_columns=["trade_date", "C00"]))
print(sql_lib.tail(n=10))

# 连续性检查
sql_lib.check_continuity(incoming_date="20120306", calendar=calendar)
```

更多用法请参考该类的方法.

---

### qh5

类比 `qsqlite`, 提供 `CDbHDF5` 和 `CDbHDF5PlusTDates` 类来管理 HDF5 (`.h5`) 文件.

```python
from husfort.qh5 import CDbHDF5
```

---

### qplot

在 `matplotlib` 基础上进一步封装的绘图类, 在 PyCharm / VSCode 智能提示加持下绘图参数更加清晰.

主要类:

| 类 | 说明 |
|----|------|
| `CPlotLines` | 折线图 |
| `CPlotLinesWithBars` | 折线 + 柱状双轴图 |
| `CPlotLinesWithLines` | 双轴折线图 |
| `CPlotBarsV` | 垂直柱状图 |
| `CPlotBarsH` | 水平柱状图 |

```python
import pandas as pd
from random import randint
from husfort.qplot import CPlotLines

test_size = 60
data = pd.DataFrame(
    {
        "T": [str(_) for _ in range(2014, 2014 + test_size)],
        "x": [randint(95, 105) for _ in range(test_size)],
        "y": [randint(95, 105) for _ in range(test_size)],
    }
).set_index("T")

my_artist = CPlotLines(
    plot_data=data,
    fig_name="test-qplot-lines",
    fig_save_dir=r"/tmp",
    line_width=4,
    line_style=["-", "-."],
    line_color=["#DC143C", "#228B22"],  # 可用 view_colors 脚本查看更多颜色
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
```

**注意: `plot_data` 须为 `pd.DataFrame`, 且索引 `index` 为字符串格式, 否则 `set_axis_x()` 可能不会正常运行.**

---

### qevaluation

回测绩效评估模块, 通过 `CNAV` 类计算收益率/净值曲线的常见风险收益指标.

```python
import scipy.stats as sps
import pandas as pd
from husfort.qevaluation import CNAV

n = 250
ret_val = sps.norm.rvs(size=n, loc=0.001, scale=0.01, random_state=0)
date_range = pd.date_range(start=pd.Timestamp.now(), periods=n)
ret_srs = pd.Series(data=ret_val, index=date_range)

nav = CNAV(input_srs=ret_srs, input_type="RET")
nav.cal_all_indicators()
sum_data = nav.to_dict()

pd.set_option("display.float_format", "{:.6f}".format)
print(pd.Series(sum_data))
```

输出结果

```
retMean      0.001318  # 收益率均值
retStd       0.009981  # 收益率标准差
hpr          0.372830  # 持有期收益
retAnnual    0.329452  # 年化收益
volAnnual    0.157813  # 年化波动
sharpe       2.087605  # 夏普比率
calmar       2.758407  # 卡玛比率
mdd          0.119436  # 最大回撤
mddT         ...       # 最大回撤结束时间
lddDur       49        # 最长回撤期(天)
lddDurT      ...       # 最长回撤期结束时间
lrd          68        # 最长恢复期(天)
lrdT         ...       # 最长恢复期结束时间
```

---

### qinstruments

提供 `CInstrument` 和 `CInstruMgr` 类管理期货合约要素, 以及 `parse_instrument_from_contract` 工具函数.

```python
from husfort.qinstruments import CInstruMgr, parse_instrument_from_contract

instrument = parse_instrument_from_contract("rb2501")  # -> "rb"
```

---

### qlog

提供 `define_logger` 函数和全局 `logger` 对象 (基于 `loguru`), 用于统一日志格式.

```python
from husfort.qlog import define_logger, logger

define_logger()
logger.info("This is an info message")
```

---

### qmails

提供 `CAgentEmail` 类实现自动收发电子邮件, 支持文本附件 (`CAttachmentText`).

```python
from husfort.qmails import CAgentEmail, CAttachmentText
```

---

### qmultiprocessing

提供多进程/单进程任务调度工具, 集成 `rich` 进度条展示.

| 函数/类 | 说明 |
|---------|------|
| `mul_process_for_tasks` | 多进程并行执行任务列表 |
| `uni_process_for_tasks` | 单进程顺序执行任务列表 |
| `CAgentQueue` | 进程间通信队列代理 |

```python
from husfort.qmultiprocessing import mul_process_for_tasks

mul_process_for_tasks(
    tasks=my_task_list,
    processes=4,
)
```

---

### qoptimization

提供基于 `scipy` 和 `numba` 的投资组合优化器.

| 类 | 说明 |
|----|------|
| `COptimizerPortfolioUtility` | 最大化效用函数 (均值-方差) |
| `COptimizerPortfolioSharpe` | 最大化夏普比率 |

```python
from husfort.qoptimization import COptimizerPortfolioSharpe

optimizer = COptimizerPortfolioSharpe(
    mean=expected_returns,
    cov=covariance_matrix,
)
result = optimizer.optimize()
```

---

### qremote

提供通过 SSH/SCP 在远程主机间传输文件的工具函数.

```python
from husfort.qremote import CHost, scp_from_remote, scp_to_remote

host = CHost(hostname="192.168.1.1", username="user", port=22)
scp_from_remote(host, remote_path="/data/file.csv", local_path="/tmp/file.csv")
scp_to_remote(host, local_path="/tmp/file.csv", remote_path="/data/file.csv")
```

---

### qsimulation

提供高精度日频策略回测框架, 支持期货合约换月、持仓管理、成本计算等.

主要类:

| 类 | 说明 |
|----|------|
| `CSimulation` | 回测主类 |
| `CAccount` | 账户管理 |
| `CPosition` | 持仓管理 |
| `CTrade` | 交易记录 |
| `CMgrMajContract` | 主力合约管理 |
| `CMgrMktData` | 行情数据管理 |
| `CSignal` | 信号管理 |

---

### qsimquick

在 `qsimulation` 基础上提供轻量化的快速回测工具 `CSimQuick`, 适合因子/信号的快速验证.

```python
from husfort.qsimquick import CSimQuick
```

---

### qdataviewer

提供 CSV、SQLite、HDF5 文件的数据查看类, 支持列过滤、条件筛选、排序、头尾截取等.

| 类 | 说明 |
|----|------|
| `CDataViewerCSV` | 查看 CSV/Excel 文件 |
| `CDataViewerSql` | 查看 SQLite 数据库 |
| `CDataViewerH5` | 查看 HDF5 文件 |

---

### qviewer_pnl

提供基于 `rich` 的实时盈亏 (PnL) 终端查看器, 支持天勤账户数据展示 (Windows).

---

## Utility Scripts

安装后以下脚本被复制到系统路径, 可直接在终端调用:

| 脚本 | 说明 |
|------|------|
| `view_csv` | 在终端查看 CSV/Excel 文件内容 |
| `view_sql` | 在终端查看 SQLite 数据库表内容 |
| `view_h5` | 在终端查看 HDF5 文件内容 |
| `ls_sql` | 列出 SQLite 数据库中的所有表 |
| `view_colors` | 在终端展示内置颜色名称及色值对照表 |
| `view_palette` | 在终端展示调色板 |
| `get_datetime_fromtimestamp` | 将 Unix 时间戳转换为可读时间 |
| `create_sprites_sheet` | 将多张图片合并为一张精灵图 |

示例:

```bash
view_csv /path/to/file.csv --head 10
view_sql /path/to/db.db --table myTable
ls_sql /path/to/db.db
get_datetime_fromtimestamp -t 1700000000
```
