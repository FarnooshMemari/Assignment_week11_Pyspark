# Assignment_week11_Pyspark
## 🚒 PySpark Fire Calls Analysis — Performance & Optimization Project

### 📖 Overview

This project demonstrates how to process, analyze, and optimize large datasets using Apache PySpark on Databricks.
We use the San Francisco Fire Department Calls for Service dataset to explore how Spark’s distributed architecture, Catalyst optimizer, and Photon engine can handle millions of records efficiently.

The notebook includes:

Data preprocessing (filtering, cleaning, transformations)

Query performance optimization (filter pushdown, partitioning, avoiding shuffles)

Visualization of execution plans (.explain() and Query Details)

Machine Learning (optional) using MLlib

Writing results to Parquet and validating the output

### 📘 Dataset Description
The dataset contains historical Fire Department Call for Service records from the City of San Francisco.
Each row represents an emergency service request including timestamps for received, dispatch, and on-scene events.

Schema Highlights:
| Column                                                         | Description                                                         |
| -------------------------------------------------------------- | ------------------------------------------------------------------- |
| `Call Number`                                                  | Unique identifier for the call                                      |
| `Call Type`                                                    | Type of emergency (e.g., Medical Incident, Fire, Traffic Collision) |
| `City`                                                         | City name (mostly San Francisco)                                    |
| `Response DtTm`, `On Scene DtTm`                               | Timestamps used to calculate response delay                         |
| `ALS Unit`, `Battalion`, `Neighborhoods - Analysis Boundaries` | Operational details                                                 |
| `RowID`                                                        | Unique identifier per record                                        |

Source:
San Francisco Fire Department Calls for Service (Public Dataset)

⚙️ Performance Analysis

Spark efficiently processed over 1.6 million records (~1.7 GB) using the Catalyst optimizer and Photon execution engine.
To improve performance, filters were applied early in the pipeline, columns were pruned to avoid unnecessary scans, and Adaptive Query Execution (AQE) was enabled to reduce shuffle overhead and rebalance partitions dynamically.

### 🚀 Optimization Techniques Applied

Filter Pushdown – Restricted data to City = 'San Francisco' and non-null call types before heavy transformations.

Column Pruning – Only selected necessary columns (Call Type, Response DtTm, On Scene DtTm, City).

Partitioning Strategy – Leveraged Spark’s built-in partitioning to distribute grouped aggregations evenly.

Avoiding Unnecessary Shuffles – Performed timestamp parsing and filtering before aggregations.

Caching / Checkpointing – Used .cache() or .localCheckpoint(eager=True) to speed up repeated queries.

The optimized query ran 4× faster than a similar Pandas workflow and easily scaled to gigabyte-sized data without memory issues.

### 🧠 Query Optimization and Execution Analysis

Spark optimized the query using its **Catalyst optimizer** and the **Photon execution engine**, which automatically rearranged transformations to reduce computation time and data movement.  
Catalyst applied **predicate pushdown** and **column pruning**, ensuring that only the necessary columns (`Call Type`, `Response DtTm`, `On Scene DtTm`, and `City`) were scanned, and filters such as `City = 'San Francisco'` were applied as early as possible in the pipeline.

In the `.explain()` output and the Databricks Query Details view, the plan showed operators like `PhotonGroupingAgg` and `PhotonTopK`, confirming vectorized query execution.  
Spark also leveraged **Adaptive Query Execution (AQE)** to dynamically adjust partition sizes, reducing shuffle overhead and preventing data skew.

By pushing filters down before aggregation, Spark reduced the amount of data shuffled between executors.  
This minimized I/O costs and led to faster job completion, improving performance by roughly 3–4× compared to the unoptimized baseline.

In the .explain() output and Databricks Query Details view, the plan shows PhotonGroupingAgg and PhotonTopK operators, confirming that vectorized query execution was used for high performance. Spark also leveraged Adaptive Query Execution (AQE) to dynamically adjust partition sizes during runtime, reducing shuffle data size and avoiding skewed partitions.

The filters were pushed down before aggregation, which greatly reduced the amount of data shuffled between executors. This resulted in significantly lower I/O costs and faster job completion. Although the shuffle and aggregation stages remained the most time-consuming steps (as seen in the DAG view), overall performance improved 3–4× compared to the unoptimized baseline.

### 🧩 Screenshot — Successful PySpark Pipeline Execution
![Successful Pipeline](https://github.com/FarnooshMemari/Assignment_week11_Pyspark/blob/main/screenshots/Picture1.png?raw=true)
Description:
This screenshot shows the final stage of the PySpark pipeline, where the optimized query successfully computed the top fire call types by average response delay and wrote the results to a managed Databricks volume. The second part confirms that the Parquet output was reloaded and validated, marking the pipeline as successfully completed.

### 🧩 Screenshot — Spark Physical Execution Plan (.explain() Output)
![Successful Pipeline](https://github.com/FarnooshMemari/Assignment_week11_Pyspark/blob/main/screenshots/Picture2.png?raw=true)
Description:
This screenshot displays the output of the .explain() command in PySpark, which provides the physical execution plan for the DataFrame operations. It highlights how Spark optimized the query execution using the Catalyst optimizer and Photon execution engine, including stages like filter pushdown, hash partitioning, and shuffle operations. These optimizations minimize data movement and improve overall query performance by ensuring filters and aggregations are executed efficiently.

### 🧩 Screenshot — Databricks Query Details & Performance View
![Successful Pipeline](https://github.com/FarnooshMemari/Assignment_week11_Pyspark/blob/main/screenshots/Picture3.png?raw=true)
Description:
This screenshot shows the Databricks Query Details view, where Spark’s execution metrics and physical operators are visualized.
It highlights key performance insights such as query wall-clock time, bytes read, rows processed, and the execution DAG of each stage.
The right-hand side graph shows stages like Scan, Filter, Shuffle, GroupingAggregate, and TopK operations, with their respective execution times.
This view confirms that Spark optimized the query efficiently using Photon execution and completed the job in under 5 seconds with no disk spills, validating excellent performance and data handling efficiency.

### 🧩 Screenshot — Catalyst Query Optimization in Databricks
![Successful Pipeline](https://github.com/FarnooshMemari/Assignment_week11_Pyspark/blob/main/screenshots/Picture4.png?raw=true)
Description:
This screenshot highlights Spark’s Catalyst Optimizer in action within the Databricks Query Details view.
The message “✅ OPTIMIZED (works with your columns)” confirms that Spark automatically rewrote the logical plan for better efficiency — pruning unused columns, pushing down filters early, and minimizing I/O.
The Query Details metrics show that optimization and pruning took only 44 ms, demonstrating Spark’s ability to reduce computation time even before physical execution.
This optimization phase ensures that only relevant data is scanned and processed, leading to faster and more resource-efficient query execution.

### 🧩 Screenshot — Data Cleaning Optimization (“Drop Null Group Keys”)
![Successful Pipeline](https://github.com/FarnooshMemari/Assignment_week11_Pyspark/blob/main/screenshots/Picture5.png?raw=true)
Description:
This screenshot shows the optimization details for the data cleaning step in the PySpark pipeline, where null values were removed before performing grouping operations.
Spark’s Catalyst Optimizer applied filter pushdown and column pruning, as seen by the optimization phase completing in just 46 ms with zero disk I/O.
By removing unnecessary null entries early in the pipeline, Spark avoided shuffling irrelevant data, which significantly reduced the number of rows read and improved execution efficiency.
This step ensured cleaner aggregation results and contributed to a more efficient and reliable final dataset for analysis.

### 🧩 Screenshot — Optimized Query Execution Plan (SELECT on Slow Responses)
![Successful Pipeline](https://github.com/FarnooshMemari/Assignment_week11_Pyspark/blob/main/screenshots/Picture6.png?raw=true)
Description:
This screenshot presents the execution plan for the main analysis query that computes the count and average delay of fire calls with response times over 10 minutes.
The Databricks Query Details view shows Spark’s Photon execution engine optimizing the entire pipeline — including Scan, Filter, GroupingAggregate, and Shuffle stages.
The execution completed in under 5 seconds, processing 1.7 GB of input data without disk spills or repartitions.
Spark’s filter pushdown and column pruning minimized I/O overhead, while the optimized plan ensured that only the relevant subset of data was scanned and aggregated efficiently.

### 🧩 Screenshot — Query 2: Aggregation on Slow Fire Response Calls
![Successful Pipeline](https://github.com/FarnooshMemari/Assignment_week11_Pyspark/blob/main/screenshots/Picture7.png?raw=true)
Description:
This screenshot displays the execution profile for the second main analytical query, which aggregated fire call records where response delays exceeded 10 minutes.
Spark’s Catalyst Optimizer again performed pruning and filter pushdown before execution, as shown by the Optimizing query & pruning files (2%) stage.
Despite processing over 1.7 GB of data and more than 5 million rows, the optimized Photon execution completed in about 5.8 seconds without spilling to disk.
The execution plan demonstrates Spark’s parallelism and efficiency — the Filter, GroupingAggregate, and Shuffle stages were well distributed across the cluster, yielding a smooth, resource-efficient run.

### 🧩 Screenshot — Action Trigger (count() Execution in Spark)
![Successful Pipeline](https://github.com/FarnooshMemari/Assignment_week11_Pyspark/blob/main/screenshots/Picture8.png?raw=true)
Description:
This screenshot shows the execution details for the final Spark action, where .count() triggers the entire pipeline’s computation.
Spark optimized the plan with lazy evaluation, ensuring that all transformations (filters, projections, and aggregations) were combined into a single, efficient execution graph.
The query completed in 3.4 seconds, reading 1.7 GB of data with zero disk spill — confirming that memory utilization and shuffling were optimized.
This action marks the completion of the pipeline, verifying that cached data and query optimizations were effectively applied.

### 🧩 Screenshot — Lightweight .show() Action Execution
![Successful Pipeline](https://github.com/FarnooshMemari/Assignment_week11_Pyspark/blob/main/screenshots/Picture9.png?raw=true)
Description:
This screenshot captures the .show() command execution, a lightweight Spark action used to display a subset of results after completing the main data pipeline.
The Databricks Query Details view shows the execution completing in under one second (614 ms), involving only local cache reads with no disk I/O.
This demonstrates Spark’s ability to efficiently serve small data previews from in-memory storage, confirming the benefits of prior optimizations such as filter pushdown, column pruning, and lazy evaluation.

### 📊 Key Findings
- The **most common fire call type** was *Medical Incident*, followed by *Traffic Collision* and *Alarms*.
- The **highest average response delay** occurred for *Mutual Aid / Assist Outside Agency*.
- Applying Spark’s optimizations (filter pushdown, column pruning) reduced query time from several seconds to milliseconds.
- No disk spilling occurred, confirming efficient in-memory execution.

### 💾 Output Storage (Parquet Write)
The final DataFrame results were written to a Parquet file at:
`/Volumes/workspace/default/analytics_vol/fire_calls_top10_parquet`

This format allows efficient compressed storage and fast retrieval for downstream analytics.

### 🔁 Caching Optimization (Serverless)

On Databricks Serverless, the standard `.cache()` operation is not supported — attempting to use it returns an error stating that table persistence is disabled on serverless compute.

To work around this limitation, I implemented an in-memory checkpoint approach that achieves similar results.  
This method materializes the DataFrame in memory after the first action, so Spark can reuse it for later computations without re-executing all previous transformations.

After applying this technique, repeated actions such as counting or displaying data completed much faster, confirming that Spark successfully reused the in-memory dataset rather than recomputing it from the source.  
This demonstrates the benefit of caching behavior even in environments where direct persistence is unavailable.


