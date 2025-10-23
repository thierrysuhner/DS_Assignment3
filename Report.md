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
   1) Highest Deltas of CO2: This computation was split up into 6 Stages. In the first stage, the CSV was firstly read through FileScanRDD and forms the Dataframe with MapPartitionsRDD. Then, using a WholeStageCodegen, spark fuses multiple operators in a stage to reduce CPU overhead. In here, the  


2. You had to manually partition the data. Why was this essential? Which feature of the dataset did you use to partition and why?(0.5pt)


3. Optional: Notice that in the already provided pre-processing (in the class DatasetHelper), the long form of timeseries data, i.e., with a column _field that contained values like temperature etc., has been converted to wide form, i.e. individual column for each measurement kind through and operation called pivoting. Analyze the execution log and describe why this happens to be an expensive transformation.

### Task 3

1. Explain how the K-Means program you have implemented, specifically the centroid estimation and recalculation, is parallelized by Spark (0.5pt)


## Declarations (if any)
