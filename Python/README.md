Spark Installer Script in Python

Intent:
Simplify the installation of Spark on multiple platforms, along with the installation of
platform-specific operations such as environment variables and, for Windows systems,
the appropriate version of WinUtils, the utilty required for HDFS support on Windows.

Assumptions:

The versions of Spark supported, and the versions of Hadoop supported by those versions,
are maintained in a list maintained by RStudio at:  
"https://raw.githubusercontent.com/rstudio/sparklyr/master/inst/extdata/install_spark.csv"
The document specified the versions of Spark and Hadoop as well as the URL used for
downloading the Spark content, in the format:
spark,hadoop,hadoop_label,download,default,hadoop_default

It is assumed that the file will be maintained, and that its format will not change, as 
well as that it will be available at the URL specified.

The URL used for Winutils: "https://github.com/steveloughran/winutils/archive/master.zip"
This location is assumed not to change, if it does, code change will be required.

Defined:

Base Install location:  
	Windows: <LOCALAPPDATA>\spark
	Mac/Linux: <HOME>\spark
These locations are per-user.

Versions of Spark are installed into subfolders of the base install named for the Spark and Hadoop version pair

Example for Spark 1.6.0 and Hadoop 2.4 would be:  <AppData>\Local\spark\spark-1.6.0-bin-hadoop2.4



Python Command line flags:
-sv, --sparkversion : Specifies the specific Spark version to download, must be a version included in the pairings file downloaded (Required in Uninstall mode)
-hv, --hadoopversion : Specifies the Hadoop version against which the specified Spark version is compiled, must be included in the pairings file (Required in Uninstall mode)
-U, --Uninstall : This flag causes the script to operate in uninstall mode, where the specified version pair specified by -sv and -hv will be removed
-i, --information : This flag lists all installed Spark versions and the Hadoop version each one supports

Flow:

Launching the script fires Main()
Main parses the arguments:
	In the case of Uninstall flag, check that the Spark and Hadoop variables are specified, as both are required for uninstallation, fire the uninstall routine if all required values are present.
	In the case of the Information flag, list all of the installed versions of Spark installed
	In the case of no arguments, or spark and hadoop versions only, move to checking that Java 8 is installed on the system.
		Check for Java 8, halt if it is not present
		Provided that Java 8 is installed, fire the spark_install function with the two version numbers specified.
		Spark_install will check the base install directory for a TGZ file which matches the spark-hadoop version pair specified, if found it will proceed with that file
			If the file is not found, download the file from the URL specfiied in the CSV file
		Unpack the TGZ into a version-pair specific subfolder of the base installation
		Set the SPARK_HOME environment variable to the installation folder
			On Windows, broadcast a system wide event indicating that an environment variable is set
		Modify the PYTHONPATH variable to include the spark installation
		Set the log4j logging values in the configuration file log4j.properties
		Modify values in hive-site.xml in the Spark configuration directory


Uninstall: Takes the Spark and Hadoop versions specified, locates the install directory for the specific version pair and removes the folder containing that pairing.

Information: Searches the base install directory for folders which match the naming convention for Spark installs and lists those for the user, one version per line

Install: 
	Checks if the base intall folder exists, and will create the base folder if it does not yet exist.
	Checks base install directory for the presence of a TGZ file containing the specified versions of Spark and Hadoop
