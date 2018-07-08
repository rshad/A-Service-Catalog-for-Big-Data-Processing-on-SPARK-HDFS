package spark_catalog_package

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

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

    def get_SparkSession(): SparkSession = {
        this.mySparkSession
    }

    def get_SparkContext(): SparkContext = {
        this.mySparkContext
    }



}

/**
  * Companion object, offers a functions to work with spark_catalog_package.Catalog_Coordinator's class objects
  */
object Catalog_Coordinator {

    /**
     * used to create a new object of the class spark_catalog_package.Catalog_Coordinator, with no need to call <new> method
     * @param AppName_param, String, represents the application name
     */
    def apply(AppName_param: String, Master_IP_param: String): Catalog_Coordinator = {
        new Catalog_Coordinator(AppName_param,Master_IP_param)
    }



}
