import os
import re
import sys
import shutil
import logging

SPARK_VERSIONS_FILE_PATTERN = "spark-(.*)-bin-(?:hadoop)?(.*)"
SPARK_VERSIONS_URL = "https://raw.githubusercontent.com/rstudio/spark-install/master/common/versions.json"
WINUTILS_URL = "https://github.com/steveloughran/winutils/archive/master.zip"

NL = os.linesep

def _verify_java():
    import subprocess
    try:
        import re
        output = subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT)
        logging.debug(output)

        match = re.search(b"(\d+\.\d+)", output)

        if match:
            logging.debug("Found a match")
            if match.group() == b'1.8':
                logging.info("Found Java version 8, continuing.")
                return True
            else:
                logging.info("Did not detect Java Version 8, please install Java 8 before continuing.")
                return False
        else:
            logging.info("Java could not be detected on this system, please install Java 8 before continuing.")
            return False

    except:
        logging.info("Warning: Java was not found in your path. Please ensure that Java 8 is configured correctly otherwise launching the gateway will fail")
        return False


def _file_age_days(jsonfile):
    from datetime import datetime
    ctime = os.stat(jsonfile).st_ctime
    return (datetime.fromtimestamp(ctime) - datetime.now()).days


def _combine_versions(spark_version, hadoop_version):
    return spark_version + " " + hadoop_version


def _download_file(url, local_file):
    try:
        from urllib import urlretrieve
    except ImportError:
        from urllib.request import urlretrieve
    urlretrieve(url, local_file)


def spark_can_install():
    install_dir = spark_install_dir()
    if not os.path.isdir(install_dir):
        os.makedirs(install_dir)


def spark_versions_initialize(connecting=False):
    import json
    spark_can_install()
    jsonfile = os.path.join(spark_install_dir(), "versions.json")
    if not os.path.isfile(jsonfile) or _file_age_days(jsonfile) > 30 or connecting:
        logging.info("Downloading %s to %s" % (SPARK_VERSIONS_URL, jsonfile))
        _download_file(SPARK_VERSIONS_URL, jsonfile)
    with open(jsonfile) as jf:
        return json.load(jf)

def spark_versions(connecting=False):
    versions = spark_versions_initialize(connecting)
    installed = set([_combine_versions(v["spark"], v["hadoop"]) for v in spark_installed_versions()])
    for v in versions: v["installed"] = _combine_versions(v["spark"], v["hadoop"]) in installed
    return versions


def spark_versions_info(spark_version, hadoop_version):
    versions = [v for v in spark_versions() if v["spark"] == spark_version and v["hadoop"] == hadoop_version]

    if versions == []:
        raise ValueError("Unable to find Spark version: %s and Hadoop version: %s" % (spark_version, hadoop_version))

    package_name = versions[0]["pattern"]%(spark_version, hadoop_version)
    component_name = os.path.splitext(package_name)[0]
    package_remote_path = versions[0]["base"] + package_name

    return {"component_name": component_name,
            "package_name": package_name,
            "package_remote_path": package_remote_path}


def spark_installed_versions():
    base_dir = spark_install_dir()
    versions = []
    for candidate in os.listdir(base_dir):
        match = re.match(SPARK_VERSIONS_FILE_PATTERN, candidate)
        fullpath = os.path.join(base_dir, candidate)
        if os.path.isdir(fullpath) and match:
            versions.append({"spark": match.group(1), "hadoop": match.group(2), "dir": fullpath})
    return versions


def spark_install_available(spark_version, hadoop_version):
    info = spark_install_info(spark_version, hadoop_version)
    return os.path.isdir(info["spark_version_dir"])


def spark_install_find(spark_version=None, hadoop_version=None, installed_only=True, connecting=False):
    versions = spark_versions(connecting)
    if installed_only:
        versions = filter(lambda v: v["installed"], versions)
    if spark_version:
        versions = filter(lambda v: v["spark"] == spark_version, versions)
    if hadoop_version:
        versions = filter(lambda v: v["hadoop"] == hadoop_version, versions)
    versions = list(versions)

    if versions == []:
        logging.critical("Please select an available version pair for Spark and Hadoop from the following list: ")
        available_versions = spark_versions_initialize(connecting)
        sep = "+" + "-"*18 + "+"
        fmt = "|{:>8}| {:>8}|"
        logging.critical(NL + NL.join([sep] + 
                                 [fmt.format("Spark", "Hadoop")] + 
                                 [sep] + 
                                 [fmt.format(v["spark"], v["hadoop"]) for v in available_versions] + 
                                 [sep]))
        raise RuntimeError("Unrecognized combination of Spark/Hadoop versions: (%s, %s). Please select a valid pair of Spark and Hadoop versions to download."%(spark_version, hadoop_version))

    candidate = sorted(versions, key=lambda rec: _combine_versions(rec["spark"], rec["hadoop"]))[-1]
    return spark_install_info(candidate["spark"], candidate["hadoop"])

def spark_default_version():
    if len(spark_installed_versions()) > 0:
        version = spark_install_find()
    else:
        version = sorted(spark_versions_initialize(), key=lambda rec: _combine_versions(rec["spark"], rec["hadoop"]))[-1]
    return {"spark": version["spark"], "hadoop": version["hadoop"]}

def spark_install_info(spark_version, hadoop_version):
    info = spark_versions_info(spark_version, hadoop_version)
    component_name = info["component_name"]
    package_name = info["package_name"]
    package_remote_path = info["package_remote_path"]

    spark_dir = spark_install_dir()

    spark_version_dir = os.path.join(spark_dir, component_name)

    return {"spark_dir": spark_dir,
            "package_local_path": os.path.join(spark_dir, package_name),
            "package_remote_path": package_remote_path,
            "spark_version_dir": spark_version_dir,
            "spark_conf_dir": os.path.join(spark_version_dir, "conf"),
            "spark": spark_version,
            "hadoop": hadoop_version,
            "installed": os.path.isdir(spark_version_dir)}


def spark_uninstall(spark_version, hadoop_version):
    logging.debug("Inside uninstall routine.")
    info = spark_versions_info(spark_version, hadoop_version)
    spark_dir = os.path.join(spark_install_dir(), info["component_name"])
    shutil.rmtree(spark_dir, ignore_errors=True)
    logging.debug("File tree removed.")


def spark_install_dir():
    homedir = os.getenv("LOCALAPPDATA") if sys.platform == "win32" else os.getenv("HOME")
    return os.getenv("SPARK_INSTALL_DIR", os.path.join(homedir, "spark"))


def spark_conf_log4j_set_value(install_info, properties, reset):
    log4jproperties_file = os.path.join(install_info["spark_conf_dir"], "log4j.properties")
    if not os.path.isfile(log4jproperties_file) or reset:
        template = os.path.join(install_info["spark_conf_dir"], "log4j.properties.template")
        shutil.copyfile(template, log4jproperties_file)

    with open(log4jproperties_file, "r") as infile:
        lines = infile.readlines()
        for i in range(len(lines)):
            if lines[i].startswith("#") or "=" not in lines[i]:
                continue
            k, v = lines[i].split("=")
            lines[i] = "=".join((k, properties.get(k, v)))
            if k in properties:
                del properties[k]

    with open(log4jproperties_file, "w") as outfile:
        outfile.writelines([line + NL for line in lines])
        #Now write out values in Properties that didn't have base values in the template
        for key, value in properties.items():
            newline = "=".join((key, value))
            outfile.writelines([newline + NL])


def spark_hive_file_set_value(hive_path, properties):
    with open(hive_path, "w") as hive_file:
        hive_file.write("<configuration>" + NL)
        for k, v in properties.items():
            hive_file.write(NL.join(["  <property>",
                                     "    <name>" + k + "</name>",
                                     "    <value>" + str(v) + "</value>",
                                     "  </property>" + NL]))
        hive_file.write("</configuration>" + NL)


def spark_conf_file_set_value(install_info, properties, reset):
    spark_conf_file = os.path.join(install_info["spark_conf_dir"], "spark-defaults.conf")
    if not os.path.isfile(spark_conf_file) or reset:
        template = os.path.join(install_info["spark_conf_dir"], "spark-defaults.conf.template")
        shutil.copyfile(template, spark_conf_file)

    max_key_len = 35
    with open(spark_conf_file, "r") as infile:
        lines = infile.readlines()
        for i in range(len(lines)):
            if lines[i].startswith("#") or " " not in lines[i]: continue
            k, v = lines[i].split()
            lines[i] = ' '.join((k.lpad(max_key_len), properties.get(k, v)))

    with open(spark_conf_file, "w") as outfile:
        outfile.writelines([line + NL for line in lines])


def spark_set_env_vars(spark_version_dir):
    import glob
    zipfiles = glob.glob(os.path.join(spark_version_dir, "python", "lib", "*.zip"))
    if zipfiles != [] and zipfiles[0] not in sys.path:
        position = [index for (index, path) in enumerate(sys.path) if
                    re.match(SPARK_VERSIONS_FILE_PATTERN, path)] or len(sys.path)
        sys.path = sys.path[:position] + zipfiles + sys.path[position:]

    persistent_vars = {}

    path_delim = ";" if sys.platform == "win32" else ":"
    path_values = os.environ.get("PYTHONPATH", "").split(path_delim)
    if zipfiles != [] and zipfiles[0] not in path_values:
        position = [index for (index, path) in enumerate(path_values) if
                    re.match(SPARK_VERSIONS_FILE_PATTERN, path)] or len(path_values)
        path_values = path_values[:position] + zipfiles + path_values[position:]
        os.environ["PYTHONPATH"] = path_delim.join(path_values)
        persistent_vars["PYTHONPATH"] = path_delim.join(path_values)

    if os.environ.get("SPARK_HOME", "") != spark_version_dir:
        os.environ["SPARK_HOME"] = spark_version_dir
        persistent_vars["SPARK_HOME"] = spark_version_dir

    if persistent_vars == {}:
        return

    if sys.platform == "win32":
        try:
            import _winreg as winreg
        except ImportError:
            import winreg
        logging.info("Setting the following variables in your registry under HKEY_CURRENT_USER\\Environment:")
        for k, v in persistent_vars.items():
            logging.info("%s = %s (REG_SZ)" % (k, v))
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment", 0, winreg.KEY_SET_VALUE) as hkey:
            for value, value_data in persistent_vars.items():
                winreg.SetValueEx(hkey, value, 0, winreg.REG_SZ, value_data)
        try:
            import win32gui, win32con
            win32gui.SendMessageTimeout(win32con.HWND_BROADCAST, win32con.WM_SETTINGCHANGE, 0, "Environment", win32con.SMTO_ABORTIFHUNG, 5000)
        except ImportError:
            logging.warning("Could not refresh the registry, please install the PyWin32 package")
    else:
        logging.info("Set the following environment variables in your initialization file such as ~/.bashrc: ")
        for k, v in persistent_vars.items():
            logging.info("export %s = %s" % (k, v))

def spark_remove_env_vars():
    # Remove env variables since there's no other spark installed.
    os.environ.pop("SPARK_HOME")
    os.environ.pop("PYTHONPATH")
    os.unsetenv("SPARK_HOME")
    os.unsetenv("PYTHONPATH")

def spark_install_winutils(spark_dir, hadoop_version):
    import glob
    if not os.path.isdir(os.path.join(spark_dir, "winutils-master")):
        _download_file(WINUTILS_URL, os.path.join(spark_dir, "winutils-master.zip"))
        from zipfile import ZipFile
        with ZipFile(os.path.join(spark_dir, "winutils-master.zip")) as zf:
            zf.extractall(spark_dir)

    candidates = glob.glob(os.path.join(spark_dir, "winutils-master", "hadoop-" + hadoop_version + "*"))

    if candidates == []:
        logging.info("No compatible WinUtils found for Hadoop version %s." % hadoop_version)
        return

    os.environ["HADOOP_HOME"] = candidates[-1]


def spark_install(spark_version=None, hadoop_version=None, reset=True, loglevel="INFO"):

    info = spark_install_find(spark_version, hadoop_version, installed_only=False)

    spark_can_install()

    logging.info("Installing and configuring Spark version: %s, Hadoop version: %s" % (info["spark"], info["hadoop"]))

    if not os.path.isdir(info["spark_version_dir"]):
        if not os.path.isfile(info["package_local_path"]):
            import urllib
            logging.info("Downloading %s into %s" % (info["package_remote_path"], info["package_local_path"]))
            _download_file(info["package_remote_path"], info["package_local_path"])

        logging.info("Extracting %s into %s" % (info["package_local_path"], info["spark_dir"]))
        import tarfile
        with tarfile.open(info["package_local_path"]) as tf:
            tf.extractall(info["spark_dir"])

    if loglevel:
        from collections import OrderedDict
        configs = OrderedDict()
        configs["log4j.rootCategory"] = ",".join((loglevel, "console", "localfile"))
        configs["log4j.appender.localfile"] = "org.apache.log4j.DailyRollingFileAppender"
        configs["log4j.appender.localfile.file"] = "log4j.spark.log"
        configs["log4j.appender.localfile.layout"] = "org.apache.log4j.PatternLayout"
        configs["log4j.appender.localfile.layout.ConversionPattern"] = "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n"
        spark_conf_log4j_set_value(info, configs, reset)

    hive_site_path = os.path.join(info["spark_conf_dir"], "hive-site.xml")
    hive_path = None

    if not os.path.isfile(hive_site_path) or reset:
        hive_properties = OrderedDict()
        hive_properties["javax.jdo.option.ConnectionURL"] = "jdbc:derby:memory:databaseName=metastore_db;create=true",
        hive_properties["javax.jdo.option.ConnectionDriverName"] = "org.apache.derby.jdbc.EmbeddedDriver"

        if sys.platform == "win32":
            hive_path = os.path.join(info["spark_version_dir"], "tmp", "hive")
            hive_properties["hive.exec.scratchdir"] = hive_path
            hive_properties["hive.exec.local.scratchdir"] = hive_path
            hive_properties["hive.metastore.warehouse.dir"] = hive_path

        spark_hive_file_set_value(hive_site_path, hive_properties)

        if hive_path:
            spark_properties = OrderedDict()
            spark_properties["spark.sql.warehouse.dir"] = hive_path
            spark_conf_file_set_value(info, spark_properties, reset)

    spark_set_env_vars(info["spark_version_dir"])

    if sys.platform == "win32":
        spark_install_winutils(info["spark_dir"], info["hadoop"])


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Spark Installation Script")
    parser.add_argument("-sv", "--spark-version", help="Spark Version to be used.", required=False, dest="spark_version")
    parser.add_argument("-hv", "--hadoop-version", help="Hadoop Version to be used.", required=False, dest="hadoop_version")
    parser.add_argument("-u", "--uninstall", help="Uninstall Spark", action="store_true", default=False, required=False)
    parser.add_argument("-i", "--information", help="Show installed versions of Spark", action="store_true", default=False, required=False)
    parser.add_argument("-l", "--log-level", help="Set the log level", choices=["DEBUG", "INFO", "WARNING"], default="WARNING", required=False, dest="log_level")

    args = parser.parse_args()

    # Set up logging parameters 
    logging.basicConfig(filename="install_spark.log", format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s", level=getattr(logging, args.log_level))
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.info("Logging started")
    
    # Debug log the values 
    logging.debug("Spark Version specified: %s" % args.spark_version)
    logging.debug("Hadoop Version specified: %s" % args.hadoop_version)
    logging.debug("Uninstall argument: %s" % args.uninstall)
    logging.debug("Information argument: %s" % args.information)

    # Check for uninstall or information flags and react appropriately
    if args.uninstall:
        if args.spark_version and args.hadoop_version:
            spark_uninstall(args.spark_version, args.hadoop_version)
        else:
            logging.critical("Spark and Hadoop versions must be specified for uninstallation. Use -i to view installed versions.")
    elif args.information:
        installedversions = list(spark_installed_versions())
        fmt = "{:>8}| {:>8}| {:<}"
        print(fmt.format("Spark", "Hadoop", "Location"))        
        for elem in installedversions:
            print(fmt.format(elem["spark"], elem["hadoop"], elem["dir"]))
    else:
        # Verify that Java 1.8 is running on the system and if it is, run the install.
        if _verify_java():
            logging.debug("Prerequisites checked successfully, running installation.")
            logging.debug("Spark Version: %s" % args.spark_version)
            logging.debug("Hadoop Version: %s" % args.hadoop_version)
            spark_install(args.spark_version, args.hadoop_version, True, "INFO")
            logging.debug("Completed the install")
        else:
            logging.critical("A prerequisite for installation has not been satisfied. Please check output log for details.")


if __name__ == "__main__":
    main()
