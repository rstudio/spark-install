# Introduction 
This Python script is intended to provide a smooth, cross-platform installation experience
for Spark, including WinUtils on Windows.

# Getting Started
Python 2.7 or 3.5 is required to execute this script. If installing on
Python 3.6, ensure you choose Spark version 2.1.1 or higher (see [SPARK-19109](https://issues.apache.org/jira/browse/SPARK-19019)).

Running the script with no parameters will grab the latest Spark/Hadoop combination
version available.

Command line options -sv and -hv  (or --sparkversion and --hadoopversion) allow the user
to specify exactly which version pairing to use.  Invalid pairings will present the list
of valid options to the user.
