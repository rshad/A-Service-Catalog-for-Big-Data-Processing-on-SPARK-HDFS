package spark_catalog_package

import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import sun.reflect.generics.tree.BaseType

/**
  * spark_catalog_package.Catalog_Coordinator class, contains all the required functionalities for our catalog.
  *
  * Functionalities like:
  * --------------------
  * * Start a SparkSession with the provided values for the required parameters { App. Name, Master IP }
  * * Loading data sets in the different available formats.
  * * Selecting the appropriate algorithm. { Initially will be chosen by the user }
  * * Tune the required parameters for the selected algorithm in the previous.
  * * Parallel execution of the selected configuration.
  * * Get the result.
  *
  * @param AppName_param, String, represents application name
  *
  */
class Catalog_Coordinator(val AppName_param: String, val Master_IP_param: String ) {

    /** Class Members **/

    private val AppName: String = AppName_param // represents the application name
    private val Master_IP: String = Master_IP_param // The master node IP
    private var dataset = null // The class associated dataset. Can be adapted to any type of datasets ..
                               //  .. { CSV, txt, ... }
    private var mySparkContext: SparkContext = _ // SparkContext instance
    private var mySparkSession: SparkSession = _ // SparkSession instance


    /**
      * start_spark_session, a function used to start a SparkSession and give us the possibility to
      * work with spark functionalities.
      *
      * Better Understainding for How to Intialize Spark
      * ------------------------------------------------
      *
      * * In order to start a Spark Application, we should follow
      * * -------------------------------------------------------
      *
      * * * 1) Start a SparkSession and then get its SparkContext:
      * * * ------------------------------------------------------
      * * * A SparkSession give a lot more functionalities than a normal SparkContext, like wokring
      * * * with Dataframes, ....
      * * * SparkSessions provide all the functionalities provided by a SparkContext
      *
      * Or
      *
      * * * 2) Create a SparkConf and then create a SparkContext:
      * * * ----------------------------------------------------
      * * * create a SparkConf object,  hat contains information about your application, this called, conf.
      * * * create a SparkContext object which tells Spark how to access a cluster, using "conf"
      *
      * */
    def start_spark_session(): Unit = {
        this.mySparkSession = SparkSession
            .builder() // Create a SparkSession.Builder for constructing a SparkSession.
            .appName(this.AppName) // Specify the application name
            .master(this.Master_IP) // Specify the master
            .getOrCreate() // Get an exisiting session or Create a new one

        // Get the SparkContext of mySparkSession
        this.mySparkContext = mySparkSession.sparkContext
    }

    /**
      * stop_spark_session, finish a spark session, in this case finishes mySparkSession.
      */
    def stop_spark_session(): Unit = {
        this.mySparkSession.close() // equivalent to mySparkSession.sparkContext.stop()
    }

    /** get_SparkContext, a function used to get the instance's SparkSession */
    def get_SparkSession(): SparkSession = {
        this.mySparkSession
    }

    /** get_SparkContext, a function used to get the instance's SparkContext */
    def get_SparkContext(): SparkContext = {
        this.mySparkContext
    }

    /**
      * read_dataset_into_RDD, a funtion used to read a text dataset into RDD of Strings.
      *
      * Note, that reading a dataset into RDD, does not imply to store the read dataset into memory,
      * this way only creates an RDD that says "we will need to load this file". The file is not loaded
      * at this point.
      *
      * @param dataset_path, a String, represents the path to the dataset to read.
      */
    def read_dataset_into_RDD(dataset_path: String): RDD[String]= {
        val textFile: RDD[String] = this.mySparkContext.textFile(dataset_path)
        textFile
    }

    /**
      * load_text_dataset_into_RDD_SVM_file, Load training data in LIBSVM format
      * Loads binary labeled data in the LIBSVM format into an RDD[LabeledPoint], with number of
      * features determined automatically and the default number of partitions.
      *
      * @param dataset_path, a String, represents the path to the dataset to load.
      */
    def load_text_dataset_into_RDD_SVM_file(dataset_path: String): RDD[LabeledPoint] = {
        import org.apache.spark.mllib.util.MLUtils

        val data: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(this.mySparkContext, dataset_path)
        data
    }


    /**
      * load_rdd_in_memory, a function used to load a RDD into memory
      * RDD.cache, is A lazy operation. The file is still not read.
      * In this case, the RDD says "read this file and then cache the contents".
      *
      * If we then run rdd.count the first time, the file will be loaded, cached, and counted.
      * If we call rdd.count a second time, the operation will use the cache.
      * It will just take the data from the cache and count the lines.
      *
      * Note that The cache behavior depends on the available memory.
      * If the file does not fit in the memory, for example, then textFile.count will fall back
      * to the usual behavior and re-read the file.
      *
      * @param rdd, the RDD to be loaded into memory
      */
    def load_rdd_in_memory[T <: BaseType](rdd: RDD[T]): Unit = {
        rdd.cache
    }




}

/** Companion object, offers a functions to work with spark_catalog_package.Catalog_Coordinator's */
object Catalog_Coordinator {

    /**
     * used to create a new object of the class spark_catalog_package.Catalog_Coordinator, with no need to call <new> method
     * @param AppName_param, String, represents the application name
     */
    def apply(AppName_param: String, Master_IP_param: String): Catalog_Coordinator = {
        new Catalog_Coordinator(AppName_param,Master_IP_param)
    }

}


/**
  * auxiliary_csv_loader, header class that uses a parsed version of the first row of a csv dataset
  *
  * @param header, an Array of String's, represents the first line of a csv dataset
  * */
case class auxiliary_csv_loader(header: Array[String]) extends Serializable{
    // creating a map of pairs formed by the header elements and their indexes
    val index: Map[String, Int] = header.zipWithIndex.toMap

    /**
      * apply, used to get a column of the header givin as an array, this column is determined by a key
      *
      * @param array, an Array of String's which contains the columns names found in a header line.
      * @param key, the name of the column to get
      * @return a String, which represents the column name represented by 'key'
      */
    def apply(array:Array[String], key:String):String = array(index(key))
}


