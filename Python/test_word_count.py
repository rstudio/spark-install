import os
import shutil


def main():
    from pyspark.context import SparkContext
    from operator import add

    # Clean up previous results that can cause failure
    if os.path.isdir(os.path.join(os.getcwd(), "wc_out")):
        shutil.rmtree(os.path.join(os.getcwd(), "wc_out"))

    #print(os.environ.get("SPARK_HOME"))
    sc = SparkContext(appName="HelloWorld")
    f = sc.textFile("test_word_count.txt")
    wc = f.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add)
    wc.saveAsTextFile("wc_out")

    # Check if results files exist and raise error if they do not.
    if os.path.isdir(os.path.join(os.getcwd(), "wc_out")):
        print("Test succeeded, results files are present for word count.")
    else:
        # print("Test failed, no results files were found for word count.")
        raise ValueError("test_word_count.py has Failed.")

if __name__ == "__main__":
    main()