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

        cat_coordinator.start_spark_session()
        val spark: SparkSession = cat_coordinator.get_SparkSession()


        val sc_ = spark.sparkContext
        val data = Array(1, 2, 3, 4, 5)
        val distData = sc_.parallelize(data)

        val spark_home = sys.env("SPARK_HOME")

        // Load training data
        val training = spark.read.format("libsvm").load(spark_home.concat("/data/mllib/sample_libsvm_data.txt"))

        val textFile: RDD[String] = cat_coordinator.read_dataset_into_RDD(spark_home.concat("/data/mllib/sample_libsvm_data.txt"))
        val counts = textFile.flatMap(line => line.split(" "))
            .map(word => (word, 1))
            .reduceByKey(_ + _)
        counts.saveAsTextFile("./counts")

        /*
        val lr = new LogisticRegression()
            .setMaxIter(10)
            .setRegParam(0.3)
            .setElasticNetParam(0.8)

        // Fit the model
        val lrModel = lr.fit(training)

        // Print the coefficients and intercept for logistic regression
        println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

        // We can also use the multinomial family for binary classification
        val mlr = new LogisticRegression()
            .setMaxIter(10)
            .setRegParam(0.3)
            .setElasticNetParam(0.8)
            .setFamily("multinomial")

        val mlrModel = mlr.fit(training)

        // Print the coefficients and intercepts for logistic regression with multinomial family
        println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
        println(s"Multinomial intercepts: ${mlrModel.interceptVector}")
    */
    }
}
