import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession

object main {
    def main(args: Array[String]): Unit = {
        val conf = {
            new SparkConf().setAppName("appName").setMaster("local").set("spark.driver.allowMultipleContexts", "true")
        };
        val sc = new SparkContext(conf)

        val data = Array(1, 2, 3, 4, 5)
        val distData = sc.parallelize(data)

        val spark = SparkSession
            .builder()
            .appName("appName")
            .enableHiveSupport()
            .getOrCreate()

        val spark_home = sys.env("SPARK_HOME")

        // Load training data
        val training = spark.read.format("libsvm").load(spark_home.concat("/data/mllib/sample_libsvm_data.txt"))


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

    }
}
