package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.internal.Logging
import scala.collection.mutable

object AssignTask extends Logging {

  // Counters to track the sequence numbers for each task
  var taskASequence = 1
  var taskBSequence = 1
  var taskCSequence = 1

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      logError("Usage: AssignTask <inputDir> <outputDir> <checkpointDir>")
      System.exit(1)
    }

    // Extract input, output, and checkpoint directories from arguments
    val inputDir = args(0)
    val outputDir = args(1)
    val checkpointDir = args(2)

    // Set up Spark configuration and StreamingContext with a 3-second batch interval
    val conf = new SparkConf().setAppName("StreamingAssignment3").setMaster("yarn")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint(checkpointDir)

    logInfo(s"Monitoring input directory: $inputDir")

    // Monitor the input directory
    val lines = ssc.textFileStream(inputDir)

    // Task A: Filter words and count their frequency
    val words = cleanAndFilterWords(lines)
    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)
    saveIfNotEmptyWithSequence(wordCounts, s"$outputDir/taskA", () => taskASequence)

    // Task B: Generate and count word pairs
    val wordPairs = generateWordPairs(lines)
    val pairCounts = wordPairs.map { case (w1, w2) => (s"$w1 $w2", 1) }.reduceByKey(_ + _)
    saveIfNotEmptyWithSequence(pairCounts, s"$outputDir/taskB", () => taskBSequence)

    // Task C: Continuously update the co-occurrence counts using state
    val cumulativeCounts = wordPairs.map { case (w1, w2) => (s"$w1 $w2", 1) }
      .updateStateByKey(updateFunction)
    saveIfNotEmptyWithSequence(cumulativeCounts, s"$outputDir/taskC", () => taskCSequence)


    // Start the streaming context
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * Clean and filter words based on the assignment rules:
   * - Retain only alphabetic words.
   * - Filter out words shorter than 3 characters.
   */
  def cleanAndFilterWords(lines: DStream[String]): DStream[String] = {
    lines.flatMap(_.split("\\s+")) // Split by whitespace
      .filter(_.forall(_.isLetter)) // Keep only alphabetic words
      .filter(_.length >= 3) // Only words with length >= 3
  }

  /**
   * Generate word pairs for co-occurrence counting.
   */
  def generateWordPairs(lines: DStream[String]): DStream[(String, String)] = {
    lines.flatMap { line =>
      // Step 1: Split the line into words and filter valid ones
      val cleanWords = line.split("\\s+")
        .map(_.replaceAll("[^a-zA-Z]", "")) // Remove non-alphabetic characters
        .filter(word => word.matches("^[a-zA-Z]{3,}$")) // Keep valid words

      // Step 2: Generate all valid word pairs, including reverse pairs
      val pairs = for {
        i <- cleanWords.indices
        j <- cleanWords.indices
        if i != j // Ensure we don't generate the same index pair twice
      } yield (cleanWords(i), cleanWords(j))

      pairs
    }
  }

  /**
   * Update function for updateStateByKey to maintain cumulative counts.
   */
  def updateFunction(newValues: Seq[Int], state: Option[Int]): Option[Int] = {
    val newCount = newValues.sum + state.getOrElse(0)
    Some(newCount)
  }


  /**
   * Save RDDs to HDFS with a sequential number if they are not empty.
   */
  // Store the previous state to compare with
  val previousState = mutable.Map[String, Int]()
  var isFirstBatch = true

  def saveIfNotEmptyWithSequence[T](rdd: DStream[(T, Int)], path: String, getSequence: () => Int): Unit = {
    rdd.foreachRDD { (rdd, _) =>
      if (!rdd.isEmpty()) {
        val currentState = rdd.collect().toMap

        logInfo(s"Current State: $currentState")
        logInfo(s"Previous State: $previousState")

        // Check if it's Task C's first output (allow saving) or state has changed
        val isTaskC = path.contains("taskC")
        val stateChanged = currentState != previousState


        val shouldSave = isTaskC match {
          case true if taskCSequence == 1 => true
          case true => stateChanged
          case false => true // For other tasks, always save
        }

        if (shouldSave) {
          val sequenceNumber = f"${getSequence()}%03d"
          val outputPath = s"$path-$sequenceNumber"

          rdd.saveAsTextFile(outputPath)
          logInfo(s"Saved output at: $outputPath")

          // Increment the sequence counter only if output is saved
          path match {
            case p if p.contains("taskA") => taskASequence += 1
            case p if p.contains("taskB") => taskBSequence += 1
            case p if p.contains("taskC") => taskCSequence += 1
          }

          // Update previous state only for Task C
          if (isTaskC) {
            previousState.clear()
            previousState ++= currentState.map { case (k, v) => (k.toString, v) }
          }
        } else {
          logInfo(s"No change in state. Skipping save for this batch.")
        }
      } else {
        logInfo(s"No data to save for this batch.")
      }
    }
  }


}
