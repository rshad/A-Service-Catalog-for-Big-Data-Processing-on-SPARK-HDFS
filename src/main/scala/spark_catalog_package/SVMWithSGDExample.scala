/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples.mllib

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import spark_catalog_package.{Algorithms, Catalog_Coordinator}
// $example on$
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
// $example off$

object SVMWithSGDExample {

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
