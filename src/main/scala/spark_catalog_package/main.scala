/**
  * Error Reporting
  * ---------------
  * including this script inside the following package, give an error in spark-shell, however it
  * does not in Intellij for example. This error is given because a file which defines Classes or
  * Objects and is not compiled with "scalac" cannot be defined as belonging to a package.
  *
  * Required tips in spark-shell
  * ----------------------------
  * * Comment the line below < package spark_catalog_package >
  *
  */
package spark_catalog_package

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object main {
    def main(args: Array[String]): Unit = {

        var cat_coordinator = Catalog_Coordinator("myApp","local")

        val spark_home = sys.env("SPARK_HOME")
        val dataset_path = spark_home.concat("/data/mllib/sample_libsvm_data.txt")

        cat_coordinator.start_spark_session()
        val spark: SparkSession = cat_coordinator.get_SparkSession()

        Algorithms.spark_session = spark
        Algorithms.dataset_path = dataset_path

        var array_ = new Array[String](0)
        classification_LSVM.main(array_)


    }
}
