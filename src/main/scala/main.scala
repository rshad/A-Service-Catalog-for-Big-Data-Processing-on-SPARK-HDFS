import org.apache.spark.{SparkContext,SparkConf}

object main {
    def main(args: Array[String]): Unit = {
        val conf = {
            new SparkConf().setAppName("appName").setMaster("local").set("spark.driver.allowMultipleContexts", "true")
        };
        val sc = new SparkContext(conf)

        val data = Array(1, 2, 3, 4, 5)
        val distData = sc.parallelize(data)
        println("adasdadas")

    }
}
