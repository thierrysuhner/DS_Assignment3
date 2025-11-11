# Assignment 3 Report

## Team Members

- Karla Ruggaber
- Thierry Suhner

## Responses to questions posed in the assignment

_Note:_ Include the Spark execution history for each task. Name the zip file as `assignment-3-task-<x>-history.zip`.

### Task 1: Word counting

1. If you were given an additional requirement of excluding certain words (for example, conjunctions), at which step you would do this and why? (0.1 pt)


Ans: We would do this in Step A, i.e. in the flatMap transformation / WordMapper call, because this ensures that unwanted words are discarded before Spark materializes them into the next RDD -> We reduce the intermediate data size early (even before creating any key-value pairs) and minimize network I/O during shuffle


2. In Lecture 1, the potential of optimizing the mapping step through combined mapping and reduce was discussed. How would you use this in this task? (in your answer you can either provide a description or a pseudo code). Optional: Implement this optimization and observe the effect on performance (i.e., time taken for completion). (0.1 pt)


Ans: We would add the counting functionality directly into the flatMap transformation / WordMapper call: While iterating over lines in a partition, we would already count the words. After that, we still need to combine the counts across partitions (reduceByKey). Like this, the network shuffle is much smaller as each partition only emits one count per word. Here is some Pseudo-Code for this:

    function CombinedMapReduce(textFile):
    // Step 1: Process each partition
    partitionedCounts = textFile.mapPartitions(partition -> 
        localCounts = empty map
        for line in partition:
            // Remove punctuation and lowercase
            line = clean(line)
            
            // Split into words
            words = split(line, whitespace)
            
            for word in words:
                if isAlphanumeric(word):
                    // Count immediately in local map
                    localCounts[word] = localCounts.getOrDefault(word, 0) + 1
        
        // Emit local counts as (word, count) pairs
        return iterator of (word, count) from localCounts
    )
    
    // Step 2: Combine counts across partitions
    finalCounts = partitionedCounts.reduceByKey((a, b) -> a + b)
    
    return finalCounts

The key step here is the immediate counting in local map, where *localCounts.getOrDefault(word, 0) + 1* either returns the word's current count if its already in the map (and then adds 1 to it), or returns 0 if the word is not yet present (and then adds it to map with initial count of 1).

3. In local execution mode (i.e. standalone mode), change the number of cores that is allocated by the master (.setMaster("local[<n>]") and measure the time it takes for the applicationto complete in each case. For each value of core allocation, run the experiment 5 times (to rule out large variances). Plot a graph showing the time taken for completion (with standard deviation) vs the number of cores allocated. Interpret and explain the results briefly in few sentences. (0.4 pt)

Our results were more or less the same for each number of cores used with an average execution time of around 1000ms - 1080ms. This is because the division of the partition to the executors will always just be 1 executor/partition, so there is no split up whatsoever. That's because one core is powerful enough to do the computation, so no matter how many we assign in the Spark Config, it always decides to just use one. Another interesting finding is that when allocating only one or two cores, but also when allocating 10 cores, the standard deviation is higher than in the other cases. We think that could happen because of small timing differences (like GC, thread startup, etc.) in the case of one or two cores, and due to more overhead induced by creating the extra threads (that are just idling as they have no task) in case of the 10 core allocation.


4. Examine the execution history. Explain your observations regarding the planning of jobs, stages, and tasks. (0.4 pt)

According to the DAG Visualization, there were two stages. 
The first stage contained 3 tasks:
   1. *textFile*: This task first loads the textfile. It does this as a parallel read across the 2 executors/partitions involved as workers.
   2. *flatMap*: The following task takes the data from the textfile and executes the flatMap method on it to create the word list.
   3. *map*: The third task then maps each word in the word list to the value 1 as the initial counter value.
The second stage contained only one task: The reduce function, that is responsible for summing all the occurence counts to get the actual count of each word. To do this, it takes the maps created earlier.


### Task 2

1. For each of the above computation, analyze the execution history and describe the key stages and tasks that were involved. In particular, identify where data shuffling occurred and explain why. (0.5pt)
   First of all, inspecting the Jobs performed by Spark, we see 5 different jobs. The first one was triggered by the pivot operation in the DatasetHelper class, called on line 43 in the TaskRoomSensorTelemetry class through DatasetHelper.getDataset. This is only used to retrieve the data and form the dataframe, which is why we won't go into any more detail about this Job.
   1) Highest Deltas of CO2: This computation is split up into one single job (triggered by monthWiseResults.show() on line 86 in the TaskRoomSensorTelemetry class), and then split up into 6 Stages. In the first stage, the Scan csv is still in the pipeline because even if the scan is not re-executed, with no caching/checkpointing, all downstream DataFrames trace lineage to this scan (this is also there for Spark to know where the data comes from in case of an error/exception so it can reconstruct it). Then, using a WholeStageCodegen, spark fuses multiple operators in a stage to reduce CPU overhead. In the Exchange afterwards, it writes shuffle files to disk, the disk is partitioned and the output is a set of shuffle blocks. In the second stage, firstly the AQEShuffleRead (Adaptive Query Execution Shuffle Read) reads the shuffle files from before, then the WholeStageCodegen and HashAggregate tasks use the data by grouping it and aggregating it. Afterwards, the next shuffle is set up in the exchange. The next stage is quite similar, just that the positions of HashAggregate and WholeStageCodegen are switched. First the shuffle files are read, then the second physical part of the aggregation is done. After that, the WholeStageCodegen orders by month and hour and the next exchange happens as before. In the 4th and 5th stage, the same principles apply, they just don't have any aggregations but just other operations: They read the shuffled data, execute the WholeStageCodegen operation (so multiple operators fused together) on it, and then does the usual exchange afterwards to prepare and exchange the data for the next stage. The last stage then reads this exchanged shuffled data again, orders the data by month and projects/shows them.
   
   2) This correlation compute task is split in 3 different jobs, as we want the correlation of month, hour and day of week separately. Each time we call .corr() (approximately line 92, 98 and 104 in TaskRoomSensorTelementry) we trigger this action. The 3 all work the same: They are split up in 4 stages, where the first stage is similar to task 1 with the read csv, WholeStageCodegen and the Exchange. Afterwards, there are 2 stages where the data is shuffle-read and then the new column (for month, hour or weekday) is created (-> a new DataFrame is created with this column) before the correlation is computed between this new column and the CO2 column. In the end, the whole partition is transformed in the mapPartitionsInternal function of the last stage.
   
   In conclusion, the exchange steps are where the data shuffling happens. The data is (re)distributed across partitions so that operations that require data from multiple partitions can be performed correctly. The shuffle is triggered by various operations, for example the `groupBy()`, `reduceByKey()` or `agg()` operations to compute aggregates, as all rows with the same key must be processed together on the same executor. Furthermore, operations like `orderBy()`or `sortBy()` also trigger the reshuffling, as the data needs to be globally ordered.


2. You had to manually partition the data. Why was this essential? Which feature of the dataset did you use to partition and why?(0.5pt)

   It was essential to optimize performance and reduce shuffle overhead.
   We partitioned using the Window functions/aggregations that depend on logical grouping based on the feature `month` (in the code: `WindowSpec windowSpec = Window.partitionBy("month").orderBy("hour");`), to ensure that all records from the same month were placed within the same partition (-> are "co-located", so we improve data locality). We used month as our feature to partition, because all of our downstream operations (like grouping, ordering and analysis steps, e.g. the window function and the aggregation `groupBy("month")`) depend on this feature by using month as the key, so like this we minimize data transfer since all operations per month can stay within one executor. Because of the partitioning, the executors can compute the aggregates locally (e.g. averages/deltas/grouping) which mitigates unneeded, costly network traffic through data transfers/shuffling (that would otherwise be needed to bring all rows of each month onto the same node). Furthermore, it was essential for correct window function behavior.

3. Optional: Notice that in the already provided pre-processing (in the class DatasetHelper), the long form of timeseries data, i.e., with a column _field that contained values like temperature etc., has been converted to wide form, i.e. individual column for each measurement kind through and operation called pivoting. Analyze the execution log and describe why this happens to be an expensive transformation.

   The pivot operation in the preprocessing step is expensive because it requires Spark to perform a full data shuffle and wide aggregation. Pivoting collects all rows sharing the same key (e.g., timestamp) and converts categorical values in _field (like temperature, humidity, and CO2) into separate columns. This process forces Spark to redistribute data across partitions (visible as Exchange in the execution plan), maintain large in-memory aggregation maps, and produce a much wider dataset. Consequently, it causes significant CPU, memory, and network overhead, making pivot one of the most resource-intensive operations in the pipeline.

### Task 3

1. Explain how the K-Means program you have implemented, specifically the centroid estimation and recalculation, is parallelized by Spark (0.5pt)

   Firstly, using `trainingDataset.javaRDD().map(...).cache();` Spark automatically splits the RDD into partitions, which are processed in parallel across worker nodes in the cluster. `cache()` ensures that the RDD stays in memory to avoid repeated disk reads. 
   Later, using `jsc.broadcast(currentCentroids)` the current centroids are broadcasted to all worker nodes, which avoids sending the full centroid list with every task (-> reduces communication overhead) and enables each partition to work locally without remote lookups. Then, using `.mapToPair()` on line 196 we assign each point to the closest centroid which runs in parallel on all partitions of the trainingDataRDD, so every worker node independently computes the closest centroid for his points in the partition -> no cross-partition communication. Afterwards, aggregation by key on line 207 (`.reduceByKey()`) ensures that each worker sums features in parallel for its assigned cluster IDs, so each worker only sees part of the data and contributes to the sums locally before Spark merges them -> partition work of "sum and count" across cluster. In the end, using `.collect()`on line 226, Spark sends the reduced sums back to the driver that then computes the mean for each cluster and checks for convergence. If it didn't converge yet, these new centroids are then broadcasted again for the next iteration. This means that the driver only handles the computationally small tasks in serial, and the computionally heavy tasks like distance computations or sum aggregations happen in parallel across the cluster.


## Declarations (if any)
We used ChatGPT to verify/double-check the code we wrote after we executed it locally and on the cluster, and we used it to help debug some exceptions. Furthermore, it helped us with formatting in Markdown, especially with code snippets.