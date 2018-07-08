import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Catalog_Brain class, contains all the required functionalities for our catalog.
  *
  * Functionalities like:
  * --------------------
  * * Loading data sets in the different available formats.
  * * Selecting the appropriate algorithm.
  * * Tune the required parameters for the selected algorithm in the previous.
  * * Parallel execution of the selected configuration.
  * * Get the result.
  *
  * @param AppName_param, String, represents application name
  *
  */
class Catalog_Brain(val AppName_param: String) {

    /** Class Members **/

    private val AppName: String = AppName_param // represents the application name
    private val Master_IP: String = ???
    private var dataset = ??? // The class associated dataset. Can be adapted to any type of datasets ..
                              //  .. { CSV, txt, ... }
    private var mySparkContext: SparkContext = ??? // SparkContext instance

    /**
      * create_spark_context, a function used to create the required spark context so we can proceed
      * with spark functionalities.
      *
      * Intializing Spark
      * -----------------
      * * In order to start a Spark Application, we should follow ...
      * * -----------------------------------------------------------
      * * * create a SparkConf object,  hat contains information about your application, this called, conf.
      * * * create a SparkContext object which tells Spark how to access a cluster, using "conf"
      *
      * */
    def create_spark_context(): Unit = {
        // create a SparkConf instance ...
        val conf = new SparkConf()
            .setAppName(this.AppName)
            .setMaster("local")
            .set("spark.driver.allowMultipleContexts", "true");
        this.mySparkContext = new SparkContext(conf) // Create a SparkContext instance
    }

}

/**
 * Companion object, offers a functions to work with Catalog_Brain's class objects
 * 
 */
object Catalog_Brain {

    /**
     * used to create a new object of the class Catalog_Brain, with no need to call <new> method
     * @param AppName_param, String, represents the application name
     */
    def apply(AppName_param: String): Catalog_Brain = {
        new Catalog_Brain(AppName_param)
    }



}
