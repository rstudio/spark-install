# spark-install
> Cross-platform installer for Apache Spark.

This project provides a cross-platform installer for Apache Spark designed to use system resources efficiently under a common API. This initial version commes with support for R and Python that arose from a collaboration between [RStudio](https://www.rstudio.com) and [Microsoft](https://www.microsoft.com).

## R

```
# lists the versions available to install
spark_available_versions()

# installs an specific version
spark_install(version = "2.1.0")

# uninstalls an specific version
spark_uninstall(version = "2.1.0", hadoop_version = "2.7")
```

## Python

```
```
