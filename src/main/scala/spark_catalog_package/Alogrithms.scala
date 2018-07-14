package spark_catalog_package

import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Algorithms{
    var spark_session: SparkSession = _
    var dataset_path: String = ""
}
/**
  * classification_LSVM, a singleton object, contains the basic functionalities for a linear support virtual
  * machine training model, for labeled data.
  */
object classification_LSVM {

    def main(args: Array[String]): Unit = {

        val sc = Algorithms.spark_session.sparkContext

        val data = MLUtils.loadLibSVMFile(sc, Algorithms.dataset_path)

        // Split data into training (60%) and test (40%).
        val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
        val training = splits(0).cache()
        val test = splits(1)

        // Run training algorithm to build the model
        val numIterations = 100
        val model = SVMWithSGD.train(training, numIterations)

        // Clear the default threshold.
        model.clearThreshold()

        // Compute raw scores on the test set.
        val scoreAndLabels = test.map { point =>
            val score = model.predict(point.features)
            (score, point.label)
        }

        // Get evaluation metrics.
        val metrics = new BinaryClassificationMetrics(scoreAndLabels)
        val auROC = metrics.areaUnderROC()

        println(s"Area under ROC = $auROC")

        // Save and load model
        model.save(sc, "target/tmp/scalaSVMWithSGDModel")
        val sameModel = SVMModel.load(sc, "target/tmp/scalaSVMWithSGDModel")
        // $example off$

        sc.stop()
    }
}

/*
object classification_LSVM {

    def main(args: Array[String]): Unit = {
        val ss = SparkSession
            .builder() // Create a SparkSession.Builder for constructing a SparkSession.
            .appName("myApp") // Specify the application name
            .master("local") // Specify the master
            .getOrCreate() // Get an exisiting session or Create a new one
        val sc = ss.sparkContext

        val spark_home = sys.env("SPARK_HOME")
        val dataset_path = spark_home.concat("/data/mllib/sample_libsvm_data.txt")


        val data = MLUtils.loadLibSVMFile(sc, dataset_path)

        // Split data into training (60%) and test (40%).
        val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
        val training = splits(0).cache()
        val test = splits(1)

        // Run training algorithm to build the model
        val numIterations = 100
        val model = SVMWithSGD.train(training, numIterations)

        // Clear the default threshold.
        model.clearThreshold()

        // Compute raw scores on the test set.
        val scoreAndLabels = test.map { point =>
            val score = model.predict(point.features)
            (score, point.label)
        }

        // Get evaluation metrics.
        val metrics = new BinaryClassificationMetrics(scoreAndLabels)
        val auROC = metrics.areaUnderROC()

        println(s"Area under ROC = $auROC")

        // Save and load model
        model.save(sc, "target/tmp/scalaSVMWithSGDModel")
        val sameModel = SVMModel.load(sc, "target/tmp/scalaSVMWithSGDModel")
        // $example off$

        sc.stop()
    }
}
// scalastyle:on println
*/