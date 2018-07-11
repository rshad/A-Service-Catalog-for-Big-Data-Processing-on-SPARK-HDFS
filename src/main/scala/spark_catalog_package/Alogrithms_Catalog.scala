package spark_catalog_package

import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

abstract class Alogrithms_Catalog(val spark_session: SparkSession) {

}

/**
  * classification_LSVM, a case class, contains the basic functionalities for a linear support virtual
  * machine training model, for labeled data.
  *
  * @param spark_session, a SparkSession object, which we need to proceed with Spark.
  * @param rdd_libsvn, represents the dataset in the LIBSVM format into an RDD[LabeledPoint]
  */
case class classification_LSVM(spark_session: SparkSession, rdd_libsvn: RDD[LabeledPoint]){
    // number of iterations for the training model, by default we assign 100
    private var training: RDD[LabeledPoint] = _
    private var test: RDD[LabeledPoint] = _

    private var num_iterations: Int = 100

    private var model: SVMModel = _
    /**
      * split_dataset, Randomly splits this RDD with the provided weights.
      *
      * @param training_part, the partition size for training data
      * @param test_part, the partition size for test data
      * @param seed_value random seed value
      *
      */
    def split_dataset(training_part: Double, test_part: Double, seed_value: Long = 11L): Unit = {

        val res: Array[RDD[LabeledPoint]] = rdd_libsvn.randomSplit(Array(training_part,test_part), seed = 11L)

        this.training = res(0)
        this.test = res(1)
    }

    /**
      * set_num_iterations, used to modify the value of num_iterations
      * @param num_iter_param, value to be assigned to num_iterations
      */
    def set_num_iterations(num_iter_param: Int): Unit = {
        this.num_iterations = num_iter_param
    }

    /**
      * getModel, get the predection model by training with training dataset
      *
      * @return a SVMModel object
      */
    def getModel(): Unit = {
        this.model = SVMWithSGD.train(this.training, this.num_iterations).clearThreshold()
    }



}