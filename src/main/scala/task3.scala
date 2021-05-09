// DSCI 553 | Foundations and Applications of Data Mining
// Homework 4
// Matheus Schmitz
// USC ID: 5039286453

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable
import java.io._
import scala.io.Source
import scala.util.Random


object task3 {

  def main(args: Array[String]): Unit = {
    val randomSeed: Random.type = scala.util.Random
    randomSeed.setSeed(553)

    // Read user inputs
    val input_filename = args(0)
    val stream_size = args(1).toInt
    val num_of_asks = args(2).toInt
    val output_filename = args(3)
    //val input_filename = "publicdata/users.txt"
    //val stream_size = 300
    //val num_of_asks = 100
    //val output_filename = "scala3.csv"

    // Initialize Spark with the 4 GB memory parameters from HW1
    val config = new SparkConf().setMaster("local[*]").setAppName("task3").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")//.set("spark.testing.memory", "471859200")
    val sc = SparkContext.getOrCreate(config)
    sc.setLogLevel("ERROR")

    // Sample sized is fixed at 100
    val sample_size = 100

    // Global variable tracking the sequence number of the incoming users
    var sequence_number = 0

    // List to store current items in the reservoir
    val reservoir: mutable.ListBuffer[String] = mutable.ListBuffer.empty[String]

    // Before beginning to iterate, write the column headers
    val pw = new PrintWriter(new File(output_filename))
    pw.write("seqnum,0_id,20_id,40_id,60_id,80_id")

    // Blackbox
    val BB = Blackbox()

    // Iterate over the asks
    for (ask_iteration <- 0 until num_of_asks) {
      var stream_users = BB.ask(input_filename, stream_size)

      // Go over all users for this stream
      for (user <- stream_users) {

        //Update the sequence number for the current user
        sequence_number += 1

        // For all long as the reservoir has less samples that then cap, just keep adding users
        if (reservoir.length < sample_size) {
          reservoir.append(user)
        }
        // Once the reservoir fills, start sampling who gets in and who gets out
        else {
          // Sample if the next user should get in the reservoir
          if (randomSeed.nextFloat() < (sample_size.toFloat/sequence_number.toFloat)) {

            // If the new user was chosen go get in, sample the index of the user to be swapped for the new user
            var swap_idx =  randomSeed.nextInt(sample_size)

            // Then make the swap
            reservoir(swap_idx) = user
          }
        }
       // Every time you receive 100 users, you sohuld print the current stage of your reservoir to a CSV file
        if (sequence_number % 100 == 0) {

          // Then append the results to the output file
          pw.write("\n" + sequence_number.toString + "," + reservoir(0) + "," + reservoir(20) + "," + reservoir(40) + "," + reservoir(60) + "," + reservoir(80))
        }
      }
    }
    pw.close()
    sc.stop()
  }
}
