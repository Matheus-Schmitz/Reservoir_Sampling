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
import scala.math


object task2 {

  // Hyperparameters to create hash functions
  val n_groups = 11
  val n_rows = 10
  val n_hash: Int = (n_groups * n_rows).toInt
  val m: Int = n_hash * n_groups

  // Generate parameters for the hash functions
  val hash_params: mutable.ListBuffer[(Int, Int)] = mutable.ListBuffer.empty[(Int, Int)]
  val random_gen: Random.type = scala.util.Random
  for (a <- 0 until n_hash){
    var hashes = (random_gen.nextInt(100), random_gen.nextInt(100))
    hash_params.append(hashes)
  }

  def myhashs(user: String): mutable.ListBuffer[Int] = {

    // Encode user to int
    val user_int = user.hashCode  //BigInt(user.getBytes()).toInt  //user.map(_.asDigit.toString).mkString.toInt

    // Generate hash values
    val results: mutable.ListBuffer[Int] = mutable.ListBuffer.empty[Int]
    for (f <- hash_params){
      results.append(((f._1 * user_int + f._2) % m).abs)
    }
    return results
  }

  def count_trailing_zeroes(binary_as_string: String): Int = {
    return binary_as_string.length - binary_as_string.replaceAll("0+$", "").length
  }

  def main(args: Array[String]): Unit = {

    // Read user inputs
    val input_filename = args(0)
    val stream_size = args(1).toInt
    val num_of_asks = args(2).toInt
    val output_filename = args(3)
    //val input_filename = "publicdata/users.txt"
    //val stream_size = 300
    //val num_of_asks = 30
    //val output_filename = "scala2.csv"

    // Initialize Spark with the 4 GB memory parameters from HW1
    val config = new SparkConf().setMaster("local[*]").setAppName("task2").set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")//.set("spark.testing.memory", "471859200")
    val sc = SparkContext.getOrCreate(config)
    sc.setLogLevel("ERROR")

    // Before beginning to iterate, write the column headers
    val pw = new PrintWriter(new File(output_filename))
    pw.write("Time,Ground Truth,Estimation")

    // Blackbox
    val BB = Blackbox()

    // Iterate over the asks
    for (ask_iteration <- 0 until num_of_asks) {
      var stream_users = BB.ask(input_filename, stream_size)

      // Set to store all users seen in this iteration
      var seen_users_truth: mutable.Set[String] = mutable.Set.empty[String]

      // Lists to store the hash binary representations generated
      var hash_bin: mutable.ListBuffer[mutable.ListBuffer[String]] = mutable.ListBuffer.empty[mutable.ListBuffer[String]]

      // Go over all users for this stream
      for (user <- stream_users) {

        // Add the user to the set of seen users
        seen_users_truth.add(user)

        // Hash the user into values
        var hashed_idxs = myhashs(user)

        // Store all binary values for the current user (one value per hash function)
        var iter_hash_bin: mutable.ListBuffer[String] = mutable.ListBuffer.empty[String]

        // For the current user, get the hashed index and its binary representation
        for (curr_idx <- hashed_idxs) {
          var user_bin = curr_idx.toBinaryString
          iter_hash_bin.append(user_bin)
        }

        // Add the hashed values from the current iteration (current user) to the list of all hashes
        hash_bin.append(iter_hash_bin)
      }

      // For each of the generated binary encoding of hash values, calculate the distance based on the number of trailing zeroes
      var estimated_size_per_hash: mutable.ListBuffer[Double] = mutable.ListBuffer.empty[Double]

      // Iterate through all hash functions
      for (curr_hash <- 0 until n_hash) {
        var curr_hash_max_zeroes = 0

        // Then, for a given hash function, go over the binary encodings generated for all users
        for (curr_user <- hash_bin.indices) {

          // Count the number of trailing zeroes for the current user with the current hash
          var curr_user_max_zeroes = count_trailing_zeroes(hash_bin(curr_user)(curr_hash))

          // If it is longer than the previous max values for the current hash, then update the max value
          if (curr_user_max_zeroes > curr_hash_max_zeroes)
            curr_hash_max_zeroes = curr_user_max_zeroes
        }
        // Once the largest number of trailing zeroes for a given has hash function has been found, calculate the estimated size and append it to the list of estimates
        estimated_size_per_hash.append(math.pow(2, curr_hash_max_zeroes))
      }

      // Slice the estimated sizes in "n_groups", then for each group calculate the group average
      var group_avgs: mutable.ListBuffer[Double] = mutable.ListBuffer.empty[Double]
      for (group_idx <- 0 until n_groups) {
        var group_sum = 0.toDouble

        // Loop over the rows in the group
        for (curr_row <- 0 until n_rows){

          // Get the row index to be fetched from "estimated_size_per_hash" which has all estimates
          var row_idx = (group_idx * n_rows) + curr_row

          // Fetch the estimate for the current row and add it to the sum of estimates for the current group
          group_sum = group_sum + estimated_size_per_hash(row_idx)
        }
        // Calcualte the average for the current group and append it to the list of all group averages
        var group_avg = group_sum / n_rows
        group_avgs.append(group_avg)
      }

      // Get the median value from the group averages by sorting them and taking the middle number
      group_avgs = group_avgs.sorted
      var distinct_users_prediction = group_avgs(n_groups/2).round

      // Then append the results to the output file
      pw.write("\n" + ask_iteration.toString + "," + seen_users_truth.size.toString + "," + distinct_users_prediction.toString)
    }
    pw.close()
    sc.stop()
  }
}