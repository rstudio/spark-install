# spark-install
> Cross-platform installer for Apache Spark.

This project provides a cross-platform installer for Apache Spark designed to use system resources efficiently under a common API. This initial version commes with support for R and Python that arose from a collaboration between [RStudio](https://www.rstudio.com) and [Microsoft](https://www.microsoft.com).

## R

```
# lists the versions available to install
spark_available_versions()

# installs an specific version
spark_install(version = "1.6.2")

# uninstalls an specific version
spark_uninstall(version = "1.6.2", hadoop_version = "2.6")
```

## Python

```
import spark_install

# lists the versions available to install
spark_install.spark_versions()

# installs an specific version
spark_install.spark_install(spark_version = "1.6.2")

# uninstalls an specific version
spark_install.spark_uninstall(spark_version = "1.6.2", hadoop_version = "2.6")
```
