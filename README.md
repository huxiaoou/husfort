# Description

HUSFORT is "Handy & Utility Solution For Operation, Research and Trading" for short.

This project is designed to provide some highly frequently used tools in daily quantitative trading works.

---

## Build and Install

```powershell
python setup.py build
python setup.py sdist
pip install .\dist\husfort-1.0.0.tar.gz
```

## Module Description

### qcalendar

+ provide some classes to manage trade dates and sections

### qevaluation

+ provide a class to evaluate portfolio performance

### qinstruments

+ provide a class to manage instruments information of Futures

### qsimulation

+ provide a class to do simulations with prices and quantity

### qsqlite

+ provide classes to provide quick access to sqlite database

### qutility

+ provide some miscellaneous functions
