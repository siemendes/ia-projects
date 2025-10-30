// Databricks notebook source
// MAGIC %md
// MAGIC # Project goals : PageRank on Wikipedia’s Language Graph
// MAGIC
// MAGIC Applying PageRank on language network extarct from wikipedia helps us identify the most “influential” or “central” languages in the multilingual structure of Wikipedia.
// MAGIC
// MAGIC | Purpose                                | Explanation                                                                                                                                                         |
// MAGIC | -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
// MAGIC | 🏆 **Measure influence**               | A language edition with many incoming links (from others) is considered more central — it tends to be a source of information reused or referenced across versions. |
// MAGIC | 🌍 **Reveal linguistic hubs**          | PageRank highlights which languages act as bridges or central points in the global network of knowledge (e.g., English usually has the highest PageRank).           |
// MAGIC | 🔁 **Understand translation dynamics** | It can show which language Wikipedias are more often *translated from* or *referenced by* others.                                                                   |
// MAGIC | 🔎 **Detect bias or imbalance**        | If some languages dominate the graph, that suggests knowledge is unevenly distributed across linguistic communities.                                                |
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC PageRank in a Wikipedia language graph identifies which languages are the most “important” nodes — that is, which versions of Wikipedia act as central sources or hubs of multilingual knowledge.

// COMMAND ----------

// MAGIC %md
// MAGIC # 1 : Page Rank with RDDs

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1.1 : Base-line page rank 

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.1.1 : Implementation

// COMMAND ----------

import scala.collection.mutable.ArrayBuffer

object WikipediaLanguagePageRankByLanguage {

  def main(args: Array[String]): Unit = {

    sc.setLogLevel("ERROR")

    val start = System.nanoTime()

    println("===== Spark Context Information =====")
    println(s"App Name: ${sc.appName}")
    println(s"Default Parallelism: ${sc.defaultParallelism}")
    println(s"Default Min Partitions: ${sc.defaultMinPartitions}")

    // --------------------------
    // Load data per language
    // --------------------------
    val path = "/FileStore/tables/"
    val languages = Seq("breton", "basque", "chti", "pt") 
    
    // Global collection for all languages
    var allRanks = Seq.empty[(String, String, Double)] // (lang, page, rank)
    val iterationExecutionTimes = ArrayBuffer[Double]()

    for (lang <- languages) {
      println("\n=============================================")
      println(s"$lang")
      println("=============================================")
      println(s"\n=== Computing PageRank for language: $lang")

      val lines = sc.textFile(s"$path/wiki_$lang.txt")

      val links = lines
        .flatMap { line =>
          val clean = line.trim.replaceAll("\\+\\]", "")
          val parts = clean.split('|').map(_.trim).filter(_.nonEmpty)
          if (parts.length == 2) Some((parts(0), parts(1))) else None
        }
        .distinct()
        .groupByKey()
        .cache()

      var ranks = links.mapValues(_ => 1.0)

      println(s"Links count for $lang = ${links.count()}")

      val iterations = 10
      val damping = 0.85

      // --------------------------
      // PageRank iterations
      // --------------------------
      for (i <- 1 to iterations) {
        val iterationStartTime = System.nanoTime() 

        val contributions = links.join(ranks).flatMap {
          case (pageId, (neighbors, rank)) =>
            val size = neighbors.size
            neighbors.map(dest => (dest, rank / size))
        }

        if (!contributions.isEmpty()) {
          ranks = contributions
            .reduceByKey(_ + _)
            .mapValues(sum => (1 - damping) + damping * sum)
        } else {
          // Create an empty RDD of the same type to keep things consistent
          ranks = spark.sparkContext.emptyRDD[(String, Double)]
        }

        ranks = contributions
          .reduceByKey(_ + _)
          .mapValues(sum => (1 - damping) + damping * sum)


        val iterationEndTime = System.nanoTime()
        val durationMs = (iterationEndTime - iterationStartTime) / 1e9
        iterationExecutionTimes += durationMs
        println(f"Iteration $i completed. Execution Time = $durationMs%.2f s")

      }
  
      // --------------------------
      // Normalize PageRank 0–1 within the language
      // --------------------------
      val normalizedRanks =
        if (ranks.isEmpty()) spark.sparkContext.emptyRDD[(String, Double)]
        else {
          val maxRank = ranks.values.max()
          ranks.mapValues(rank => rank / maxRank)
      }

      // --------------------------
      // Collect top 15 PageRank results
      // --------------------------
      val output = normalizedRanks.collect().sortBy(-_._2)

      println(s"\n=== PageRank Results for $lang (normalized)")
      output.take(15).foreach { case (page, rank) =>
            println(f"$page%s -> $rank%.5f")
      }
      
      // --------------------------
      // Add to global collection
      // --------------------------
      if (output == null || output.isEmpty)
        allRanks = allRanks :+ ((lang, "N/A", 0.0))
      else
        allRanks ++= output.map { case (page, rank) => (lang, page, rank) }
    }

    // --------------------------
    // Aggregate total normalized rank by language
    // --------------------------
    val allRanksRDD = sc.parallelize(allRanks)

    val rankByLang = allRanksRDD
      .map { case (lang, page, rank) => (lang, rank) }
      .reduceByKey(_ + _)
      .sortBy(-_._2)

    println("\n=============================================")
    println("CONCLUSIONS")
    println("=============================================")
    println("\n=== Total Normalized PageRank by Language")
    rankByLang.collect().foreach {
      case (lang, totalRank) =>
        println(f"$lang%s -> $totalRank%.5f")
    }

    // --------------------------
    // Timing info
    // --------------------------
    val end = System.nanoTime()
    val duration = (end - start) / 1e9

    val avgExeIteration = iterationExecutionTimes.sum 

    println("\n===== Duration of base-line page rank")
    println(f"--- Iterations finished in $avgExeIteration%.2f seconds ---")
    println(f"--- All PageRank finished in $duration%.2f seconds ---")

  }
}

// COMMAND ----------

WikipediaLanguagePageRankByLanguage.main(Array())

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.1.2 : Observations with all languages

// COMMAND ----------

// MAGIC %md
// MAGIC We launched our program on the Lamsade cluster to be able to process a French language file of around 1.5 GB

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC **This the conclusions from lamsade cluster : **
// MAGIC <br/>
// MAGIC
// MAGIC =============================================
// MAGIC
// MAGIC **CONCLUSIONS**
// MAGIC
// MAGIC =============================================
// MAGIC
// MAGIC === Total Normalized PageRank by Language
// MAGIC
// MAGIC fr -> 36.97772
// MAGIC
// MAGIC pt -> 28.39464
// MAGIC
// MAGIC basque -> 5.00000
// MAGIC
// MAGIC breton -> 3.00000
// MAGIC
// MAGIC chti -> 0.00000
// MAGIC
// MAGIC <br/>
// MAGIC
// MAGIC ===== Duration of base-line page rank
// MAGIC
// MAGIC --- Iterations finished in 65.61 seconds ---
// MAGIC
// MAGIC --- **All PageRank finished in 117.64** seconds ---

// COMMAND ----------

// MAGIC %md
// MAGIC The larger the number, the more central or influential that node is in the graph you built.

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1.2 : Optimized page rank

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.2.1 : Implementation

// COMMAND ----------

import scala.collection.mutable.ArrayBuffer

object WikipediaLanguagePageRank_OptimizedRDD {

  def main(args: Array[String]): Unit = {

    sc.setLogLevel("ERROR")

    // --------------------------
    // OPTIMIZATION 1. Spark Configuration
    // --------------------------
    val conf = spark.sparkContext.getConf
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // Faster serialization
    conf.set("spark.rdd.compress", "true") // Compress RDD partitions to save memory/network
    conf.set("spark.default.parallelism", "8") // Set parallelism for better CPU utilization

    val start = System.nanoTime()

    println("===== Spark Context Information =====")
    println(s"App Name: ${sc.appName}")
    println(s"Default Parallelism: ${sc.defaultParallelism}")
    println(s"Default Min Partitions: ${sc.defaultMinPartitions}")

    // --------------------------
    // Load data per language
    // --------------------------
    val path = "/FileStore/tables/"
    val languages = Seq("breton", "basque", "chti", "pt") 

    // Global collection for all languages
    var allRanks = Seq.empty[(String, String, Double)] // (lang, page, rank)
    val iterationExecutionTimes = ArrayBuffer[Double]()

    // --------------------------
    // PageRank iterations
    // --------------------------
    for (lang <- languages) {
      println("\n=============================================")
      println(s"$lang")
      println("=============================================")
      println(s"\n=== Computing PageRank for language: $lang")

      val lines = sc.textFile(s"$path/wiki_$lang.txt")

      // --------------------------
      // OPTIMIZATION 2. Build links efficiently
      // replace groupByKey with reduceByKey to reduce shuffle and memory usage
      // --------------------------
      val links = lines
        .flatMap { line =>
          val clean = line.trim.stripSuffix("+")
          val parts = clean.split("[|]").map(_.trim)
          if (parts.length == 2) Some((parts(0), Seq(parts(1)))) else None
        }
        .reduceByKey(_ ++ _) // merge neighbor lists efficiently
        .persist(StorageLevel.MEMORY_AND_DISK) // persist for reuse in iterations

      var ranks = links.mapValues(_ => 1.0)

      println(s"Links count for $lang = ${links.count()}")

      val iterations = 10
      val damping = 0.85
      val baseRank = 1.0 - damping

      for (i <- 1 to iterations) {
        val iterationStartTime = System.nanoTime() 

        // --------------------------
        // OPTIMIZATION 3. Use mapValues to keep key structure and avoid unnecessary shuffles.
        // flatMap inside the join can be CPU-heavy if each neighbors list is large.
        // --------------------------
        val contributions = links
          .join(ranks)
          .flatMapValues { case (neighbors, rank) =>
            val size = neighbors.size
            neighbors.iterator.map(dest => (dest, rank / size))
          }
          .values
      
        // --------------------------
        // OPTIMIZATION 4. Unpersist old ranks for memory management
        // --------------------------
        if (i > 1) ranks.unpersist(blocking = false)

        if (!contributions.isEmpty()) {
          ranks = contributions
            .reduceByKey(_ + _)
            .mapValues(sum => (1 - damping) + damping * sum)
        } else {
          // Create an empty RDD of the same type to keep things consistent
          ranks = spark.sparkContext.emptyRDD[(String, Double)]
        }

        ranks = contributions
          .reduceByKey(_ + _)
          .mapValues(sum => (1 - damping) + damping * sum)


        val iterationEndTime = System.nanoTime()
        val durationMs = (iterationEndTime - iterationStartTime) / 1e9 // Convert nanoseconds to milliseconds
        iterationExecutionTimes += durationMs
        println(f"Iteration $i completed. Execution Time = $durationMs%.2f s")

      }
  
      // --------------------------
      // Normalize PageRank 0–1 within the language
      // --------------------------
      val normalizedRanks =
        if (ranks.isEmpty()) spark.sparkContext.emptyRDD[(String, Double)]
        else {
          val maxRank = ranks.values.max()
          ranks.mapValues(rank => rank / maxRank)
      }

      // --------------------------
      // Collect top 15 PageRank results
      // --------------------------
      val output = normalizedRanks.collect().sortBy(-_._2)

      println(s"\n=== PageRank Results for $lang (normalized) ===")
      output.take(15).foreach { case (page, rank) =>
            println(f"$page%s -> $rank%.5f")
      }
      
      // --------------------------
      // Add to global collection
      // --------------------------
      if (output == null || output.isEmpty)
        allRanks = allRanks :+ ((lang, "N/A", 0.0))
      else
        allRanks ++= output.map { case (page, rank) => (lang, page, rank) }
    }

    // --------------------------
    // Aggregate PageRank by language
    // --------------------------
    val allRanksRDD = sc.parallelize(allRanks)

    val rankByLang = allRanksRDD
      .map { case (lang, page, rank) => (lang, rank) }
      .reduceByKey(_ + _)
      .sortBy(-_._2)

    println("\n=============================================")
    println("CONCLUSIONS")
    println("=============================================")
    println("\n=== Total Normalized PageRank by Language")
    rankByLang.collect().foreach {
      case (lang, totalRank) =>
        println(f"$lang%s -> $totalRank%.5f")
    }

    // --------------------------
    // Timing info
    // --------------------------
    val end = System.nanoTime()
    val duration = (end - start) / 1e9
    val avgExeIteration = iterationExecutionTimes.sum 

    println("\n===== Duration of base-line page rank")
    println(f"--- Iterations finished in $avgExeIteration%.2f seconds ---")
    println(f"--- All PageRank finished in $duration%.2f seconds ---")

  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC What have we done to optimize our rdd implementation of wikipedia language page rank ? 
// MAGIC
// MAGIC All this things : 
// MAGIC
// MAGIC | Purpose                                | Explanation                                                                                                                                                         |
// MAGIC | -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
// MAGIC | **RDD catching**               | Uses persist(StorageLevel.MEMORY_AND_DISK) for critical RDDs reused across iterations instead of using cache.|
// MAGIC | **Parallelism tuning**          | Uses more partitions (spark.default.parallelism) for better cluster utilization.           |
// MAGIC | **Unpersisting old ranks** | Frees memory between iterations to prevent memory pressure.                                                               |
// MAGIC | **Build links efficiently**        | Replace groupByKey with reduceByKey to reduce shuffle and memory usage.                                       |

// COMMAND ----------

WikipediaLanguagePageRank_OptimizedRDD.main(Array())

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.2.2 : Observations with all languages

// COMMAND ----------

// MAGIC %md
// MAGIC **This is our results from lamsade cluster :**
// MAGIC
// MAGIC <br/>
// MAGIC
// MAGIC =============================================
// MAGIC
// MAGIC **CONCLUSIONS**
// MAGIC
// MAGIC =============================================
// MAGIC
// MAGIC === Total Normalized PageRank by Language
// MAGIC
// MAGIC fr -> 41.24579
// MAGIC
// MAGIC pt -> 20.20559
// MAGIC
// MAGIC basque -> 5.00000
// MAGIC
// MAGIC breton -> 3.00000
// MAGIC
// MAGIC chti -> 0.00000
// MAGIC
// MAGIC <br/>
// MAGIC
// MAGIC
// MAGIC ===== Duration of base-line page rank
// MAGIC
// MAGIC --- Iterations finished in **58.18** seconds 
// MAGIC
// MAGIC --- All PageRank finished in **106.15** seconds 

// COMMAND ----------

// MAGIC %md
// MAGIC We can observe a lot of improvements like : 
// MAGIC
// MAGIC
// MAGIC | Metric                               | Baseline / Optimized page rank with RDD                                                                                                                                                      |
// MAGIC | ----------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
// MAGIC | **Iterations duration**               |**Base-line** : 65 s / **Optimized** : 58 s |
// MAGIC | **Global duration**          | **Base-line** : 117 s / **Optimized** : 106 s         |

// COMMAND ----------

// MAGIC %md
// MAGIC # 2 : Page Rank with Dataframes

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2.1 : Implementation

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ArrayBuffer

object WikipediaLanguagePageRank_DF {
  def main(args: Array[String]): Unit = {

    // --------------------------
    // Initialisation de SparkSession 
    // --------------------------
    // Importation des implicites pour utiliser les opérateurs comme "$" et .toDF
    import spark.implicits._
    
    // Définir le niveau de log
    sc.setLogLevel("ERROR")

    val start = System.nanoTime()

    println("===== Spark Session Information =====")
    println(s"App Name: ${spark.conf.get("spark.app.name", "Spark App")}")
    println(s"Default Parallelism: ${sc.defaultParallelism}")
    
    // --------------------------
    // Configuration des données
    // --------------------------
    val path = "/FileStore/tables/"
    // Note: L'ordre des langues peut être différent, mais le résultat de chaque langue est indépendant.
    val languages = Seq("pt","basque","breton","chti") 

    // Collection globale pour tous les résultats normalisés
    var allRanksCollection = Seq.empty[(String, String, Double)] 
    val iterationExecutionTimes = ArrayBuffer[Double]()
    
    val iterations = 10
    val damping = 0.85
    val baseRank = 1.0 - damping

    // --------------------------
    // Boucle PageRank par langue
    // --------------------------
    for (lang <- languages) {
      println("\n=============================================")
      println(s"$lang")
      println("=============================================")
      println(s"\n=== Calcul du PageRank pour la langue: $lang")

      // Charger le fichier texte dans un DataFrame à colonne unique 'value'
      val linesDF = spark.read.textFile(s"$path/wiki_$lang.txt")

        val linksDF = linesDF
        .withColumn("clean_line", regexp_replace($"value", "\\+$", ""))
        .withColumn("parts", split($"clean_line", "\\|"))
        .filter(size($"parts") === 2)
        .select(
          trim($"parts".getItem(0)).alias("source"),
          trim($"parts".getItem(1)).alias("destination")
        )
        .groupBy("source")
        .agg(collect_list("destination").alias("destinations"))
        .persist(StorageLevel.MEMORY_AND_DISK) // Équivalent de RDD persist
       
      println(s"Links count for $lang = ${linksDF.count()}")

      // Ranks initiales : (page, 1.0) pour chaque page source (clé de liens)
      var ranksDF = linksDF.select($"source".alias("page"), lit(1.0).alias("rank")).cache()


      for (i <- 1 to iterations) {
        val iterationStartTime = System.nanoTime()
        
        // Joindre ranksDF (petit, à jour)
        val combinedDF = ranksDF.join(linksDF, ranksDF("page") === linksDF("source"))

        val contributionsDF = combinedDF
          .withColumn("share", $"rank" / size($"destinations"))
          .withColumn("destination", explode($"destinations")) 
          .select($"destination".alias("page"), $"share")
        // --------------------------
        // Mise à jour des Ranks
        // --------------------------
        val newRanksDF = contributionsDF
          .groupBy("page")
          .agg(sum("share").alias("sum_contributions"))
          .withColumn("sum_contributions", coalesce($"sum_contributions", lit(0.0))) // fill nulls
          .withColumn("rank", lit(baseRank) + lit(damping) * $"sum_contributions")
          .select("page", "rank")
          .cache()

        if (i > 1) {
          ranksDF.unpersist(blocking = false) 
        }

        ranksDF = newRanksDF
        contributionsDF.unpersist(blocking = false)

        val iterationEndTime = System.nanoTime()
        val durationMs = (iterationEndTime - iterationStartTime) / 1e9 // Convert nanoseconds to seconds
       
        iterationExecutionTimes += durationMs
        println(f"Iteration $i completed. Execution Time = $durationMs%.2f s")
      }
      
      linksDF

      // --------------------------
      // Normalisation du PageRank 0–1
      // --------------------------
      val normalizedRanksDF = if (ranksDF.isEmpty) {
          spark.createDataFrame(sc.emptyRDD[(String, Double)]).toDF("page", "rank")
      } else {
          // Trouver le rank maximum
          
          val maxRankResult = ranksDF.agg(max("rank").alias("maxRank")).head()
          val maxRank = if (maxRankResult.isNullAt(0) || maxRankResult.getDouble(0) == 0.0) 1.0 else maxRankResult.getDouble(0)

          ranksDF.withColumn("rank", $"rank" / lit(maxRank))

      }

      // --------------------------
      // Collecter les 15 premiers résultats (pour l'affichage)
      // --------------------------
      val outputTop15 = normalizedRanksDF.orderBy(desc("rank")).take(15)

      println(s"\n=== PageRank Results for $lang (normalized) ===")
      outputTop15.foreach { row =>
        val page = row.getString(0)
        val rank = row.getDouble(1)
        println(f"$page%s -> $rank%.5f")
      }
      
      // --------------------------
      // Ajouter TOUS les résultats à la collection globale (MATCHING RDD LOGIC)
      // --------------------------
      val allRanksForAggregation = normalizedRanksDF.collect()
      
      if (allRanksForAggregation.isEmpty) {
        allRanksCollection = allRanksCollection :+ ((lang, "N/A", 0.0))
      } else {
        // Ajouter TOUS les rangs pour que la somme finale soit identique à l'implémentation RDD.
        allRanksCollection ++= allRanksForAggregation.map(row => (lang, row.getString(0), row.getDouble(1)))
      }
      
      // Libérer les liens et le dernier ranksDF
      ranksDF.unpersist(blocking = false)
      linksDF.unpersist(blocking = true)
    }

    // --------------------------
    // Agrégation globale par langue 
    // --------------------------
    val allRanksDF = allRanksCollection.toDF("lang", "page", "rank")

    val rankByLangDF = allRanksDF
      .groupBy("lang")
      .agg(sum("rank").alias("totalRank"))
      .orderBy(desc("totalRank"))

    println("\n=============================================")
    println("CONCLUSIONS")
    println("=============================================")
    println("\n=== Total Normalized PageRank by Language")
    rankByLangDF.collect().foreach { row =>
      val lang = row.getString(0)
      val totalRank = row.getDouble(1)
      println(f"$lang%s -> $totalRank%.5f")
    }

    // --------------------------
    // Informations de temps
    // --------------------------
    val end = System.nanoTime()
    val duration = (end - start) / 1e9
    val avgExeIteration = iterationExecutionTimes.sum  

    println("\n===== Duration of base-line page rank")
    println(f"--- Average iteration finished in $avgExeIteration%.2f seconds ---")
    println(f"--- All PageRank finished in $duration%.2f seconds ---")

  }
}

// COMMAND ----------

WikipediaLanguagePageRank_DF.main(Array())

// COMMAND ----------

// MAGIC %md
// MAGIC I removed the French analysis in Databricks because the file was too large. However, that’s not a problem here since we only need a rough comparison between the PageRank results computed with RDDs and those computed with DataFrames in Spark Scala.
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2.2 : Observations with all languages

// COMMAND ----------

// MAGIC %md
// MAGIC **This is our results from lamsade cluster :**
// MAGIC
// MAGIC
// MAGIC =============================================
// MAGIC
// MAGIC CONCLUSIONS
// MAGIC
// MAGIC =============================================
// MAGIC
// MAGIC
// MAGIC === Total Normalized PageRank by Language
// MAGIC
// MAGIC fr -> 41.24579
// MAGIC
// MAGIC pt -> 20.20559
// MAGIC
// MAGIC basque -> 5.00000
// MAGIC
// MAGIC breton -> 3.00000
// MAGIC
// MAGIC chti -> 0.00000
// MAGIC
// MAGIC
// MAGIC
// MAGIC ===== Duration of base-line page rank
// MAGIC
// MAGIC --- Average iteration finished in 15.53 seconds 
// MAGIC
// MAGIC --- All PageRank finished in 209.29 seconds
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC # Conclusion

// COMMAND ----------

// MAGIC %md
// MAGIC Even without any optimizations, an algorithm using RDDs is significantly more performant than one using DataFrames. There is no comparison between the two; **to analyze graphs in Spark Scala, we need to use RDDs**.

// COMMAND ----------

// MAGIC %md
// MAGIC **In the end, this is the PageRank for our five languages:**

// COMMAND ----------

// MAGIC %md
// MAGIC | Language     | Page Rank Score | Interpretation                                               |
// MAGIC | ----------------- | ------------------- | ------------------------------------------------------- |
// MAGIC | French (fr) | **41**          | Central hub — many languages link to it.                     |
// MAGIC | Portuguese (pt)  | **20**         | Highly connected — often linked by other European languages. |
// MAGIC | Basque   | **5**         | Regional hub — connected within its linguistic group.        |
// MAGIC | Breton | **3**         | Peripheral — few incoming links.                             |
// MAGIC | Chti | **0**          | Peripheral — few incoming links.                             |