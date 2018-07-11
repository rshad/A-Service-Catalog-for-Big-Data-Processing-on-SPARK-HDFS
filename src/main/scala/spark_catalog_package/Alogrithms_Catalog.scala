package spark_catalog_package

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import sun.reflect.generics.tree.BaseType

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

    /**
      * split_dataset, Randomly splits this RDD with the provided weights.
      *
      * @param training_part, the partition size for training data
      * @param test_part, the partition size for test data
      * @param seed_value random seed value
      *
      * @return split RDDs in an array
      */
    def split_dataset(training_part: Double, test_part: Double, seed_value: Long = 11L): Array[RDD[LabeledPoint]] = {
        val res: Array[RDD[LabeledPoint]] = rdd_libsvn.randomSplit(Array(training_part,test_part), seed = 11L)
        res
    }


}