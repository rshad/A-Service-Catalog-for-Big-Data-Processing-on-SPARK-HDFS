package spark_catalog_package

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io._



case class HDFS_Coordinator(){
    private val conf = new Configuration()

    /**
      * write, a function used to write a file to hdfs.
      *
      * Example of Execution
      * --------------------
      * @example Hdfs.write("hdfs://0.0.0.0:9000/user/hadoop/", "test.txt", "Hello World".getBytes)
      *
      * @param uri, a String, represents the default filesystem path. Normally it comes as follows:
      *           hostname:port/path
      *           hostname: yarn.resourcemanager.hostname.
      *           port: port that the resource manager runs on.
      *           path: path where the file to be written.
      *
      * @param filePath, a String, represents the path of the file to store.
      * @param data, Array of Bytes, represents the content to write in the file to store.
      * @param hadoop_user, a String, represents Hadoop's user name.
      *
      */
    def write_HDFS(uri: String, filePath: String, data: Array[Byte], hadoop_user: String): Unit = {
        System.setProperty("HADOOP_USER_NAME", hadoop_user) // Setting the name of Hadoop's user.
        val path = new Path(filePath)

        conf.set("fs.defaultFS", uri) // sets the default file system path to the given URI
        val fs = FileSystem.get(conf)
        val os = fs.create(path)
        os.write(data) // write the data into the file
        fs.close() // close the filesysten
    }

    /**
      * saveFile, a function used to save an existing file, in HDFS.
      *
      * @param filepath, a String, represents the path of the file to store.
      */
    def saveFile(filepath: String, srcFilePath:String, destFilePath:String): Unit = {
        val hdfs = FileSystem.get(conf)

        val srcPath = new Path(srcFilePath)
        val destPath = new Path(destFilePath)

        hdfs.copyFromLocalFile(srcPath, destPath)
    }

    /**
      * removeFile, a function used to remove a file stored in HDFS.
      *
      * @param filepath, a String, represents the path of the file to store.
      */
    def removeFile(filepath: String): Boolean = {
        val hdfs = FileSystem.get(conf)

        val path = new Path(filepath)
        hdfs.delete(path, true)
    }

    /**
      * getFile, a function used to get a file stored in HDFS.
      *
      * @param filepath, a String, represents the path of the file to store.
      */
    def getFile(filepath: String): InputStream = {
        val hdfs = FileSystem.get(conf)

        val path = new Path(filepath)
        hdfs.open(path)
    }

    /**
      * createFolder, a function used to create a folder in HDFS file structure.
      *
      * @param folderPath, a String, represents the folder path to created
      */
    def createFolder(folderPath: String): Unit = {
        val hdfs = FileSystem.get(conf)
        val path = new Path(folderPath)

        if (!hdfs.exists(path)) {
            hdfs.mkdirs(path)
        }
    }

}

object HDFS_Coordinator{
    def apply(): HDFS_Coordinator = new HDFS_Coordinator()
}