import unittest
import subprocess
import spark_install
import sys
import os

class TestSparkInstall(unittest.TestCase):

    def setUp(self):
        pass

    def test_1_run_install(self):
        spark_install.spark_install(sparkversion, hadoopversion)

    def test_2_if_install_exists(self):
        homedir = os.getenv("LOCALAPPDATA") if sys.platform == "win32" else os.getenv("HOME")
        if not os.path.exists(os.path.join(homedir, "spark")):
            assert (), "Parent installation path does not exist."

        detectionoutput = spark_install.spark_installed_versions()
        if len(detectionoutput) <= 0:
            raise ValueError("No versions of product detected as installed.")

    def test_3_word_count(self):
        if subprocess.call("python test_word_count.py") < 0:
            assert(), "test_word_count has failed."

    def test_4_uninstall(self):
        if subprocess.call("python spark_install.py -U -sv " + sparkversion + " -hv " + hadoopversion) < 0:
            assert(), "Uninstall process failed."

    def test_5_if_install_removed(self):
        detectionoutput = spark_install.spark_installed_versions()
        if len(detectionoutput) > 0:
            raise ValueError("Error, Product detected as still installed.")


if __name__ == "__main__":
    sparkversion = "2.1.0"
    hadoopversion = "2.7"

    unittest.main()

