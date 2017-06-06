# spark-install
> Cross-platform installer for Apache Spark.

This project provides a cross-platform installer for Apache Spark designed to use system resources efficiently under a common API. This initial version comes with support for R and Python that arose from a collaboration between [RStudio](https://www.rstudio.com) and [Microsoft](https://www.microsoft.com).

## R

```r
# install from github
devtools::install_github(repo = "rstudio/spark-install", subdir = "R")
library(sparkinstall)

# lists the versions available to install
spark_available_versions()

# installs an specific version
spark_install(version = "1.6.2")

# uninstalls an specific version
spark_uninstall(version = "1.6.2", hadoop_version = "2.6")
```

## Python

```py
# install from github
from urllib import urlopen          # Python 2.X
from urllib.request import urlopen  # Python 3.X
exec urlopen("https://raw.githubusercontent.com/rstudio/spark-install/master/Python/spark_install.py").read() in globals()

# lists the versions available to install
spark_versions()

# installs an specific version
spark_install(spark_version = "1.6.2")

# uninstalls an specific version
spark_uninstall(spark_version = "1.6.2", hadoop_version = "cdh4")
```
