---
toc: content
order: 3
---


# UDAF implementation principle
You can implement each user-defined aggregation function (UDAF) in a single class that is inherited from the base class AggFunc and implements relevant interfaces. The interfaces are classified into local interfaces and global interfaces. The following sample code describes the required interfaces:

```cpp
// The following four functions can be called in the collect phase.
virtual bool initCollectInput(const TablePtr &inputTable);
virtual bool initAccumulatorOutput(const TablePtr &outputTable) = 0;
virtual bool collect(Row inputRow, Accumulator *acc) = 0;
virtual bool outputAccumulator(Accumulator *acc, Row outputRow) const = 0;

// You can use the following functions in the merge phase:
virtual bool initMergeInput(const TablePtr &inputTable);
virtual bool initResultOutput(const TablePtr &outputTable);
virtual bool merge(Row inputRow, Accumulator *acc);
virtual bool outputResult(Accumulator *acc, Row outputRow) const;
```


OpenSearch Vector Search Edition executes a UDAF in the following phases: Phase 1 that indicates the collect phase and Phase 2 that indicates the merge phase. In the collect phase, OpenSearch Vector Search Edition invokes the collect functions to process the data sets that are obtained by Searcher workers and generates estimated statistics based on the data sets. After all Searcher workers report collected data, the system sends the intermediate data to the Query Result Searcher (QRS) node. Then, QRS workers invoke the merge functions to process the intermediate data and return the final result.

The following uses the built-in function avg as an example to describe the implementation of the two phases of the UDAF based on the running process of havenask.

<a name="E5C96"></a>
### Collect phase
The collect phase runs on Searcher. The engine calls the `collect` method of AggFunc to perform preliminary statistics based on the aggregation results on Searcher, and stores the results in the corresponding Accumulator object. OpenSearch Vector Search Edition uses an accumulator for each data group to store the status information of the group during data processing.

```cpp
// Initialize some 'ColumnData' objects to access data in the input table.
virtual bool initCollectInput(const TablePtr &inputTable);
// Specify the output fields based on which you want the system to sort the corresponding accumulators and send the data of the accumulators to the output table.
virtual bool initAccumulatorOutput(const TablePtr &outputTable) = 0;
// The main collect operation. The system saves statistical results of multiple rows of input data in the corresponding accumulators.
virtual bool collect(Row inputRow, Accumulator *acc) = 0;
// Sort accumulators and send the data of the accumulators to the output table for data transmission. Then, the system sends the output data of the Searcher node to the QRS node.
virtual bool outputAccumulator(Accumulator *acc, Row outputRow) const = 0;

```

Before OpenSearch Vector Search Edition invokes the collect method to process data from each row, OpenSearch calculates a group key for the data of each row, obtains the accumulator of each data group, and then sends the data of each group and the corresponding accumulator to the QRS node. OpenSearch Vector Search Edition identifies data groups based on group keys. After the returned data is obtained, you can change the status of the accumulators based on the data.

In the collect phase, OpenSearch Vector Search Edition collects the number of entries in each group and calculates the sum of the numeric values in each group. The average value is calculated based on these values.

```cpp
template<typename InputType, typename AccumulatorType>
bool AvgAggFunc<InputType, AccumulatorType>::collect(Row inputRow, Accumulator *acc) {
    AvgAccumulator<AccumulatorType> *avgAcc = static_cast<AvgAccumulator<AccumulatorType> *>(acc);
    avgAcc->count++;
    avgAcc->sum += _inputColumn->get(inputRow);
    return true;
}
```

The engine obtains the AggFunc of the current stage by accessing the `AvgAggFuncCreator::createLocalFunction`. Note that the **output** of this stage is all the attributes on the `AvgAccumulator`.
```cpp
AggFunc *AvgAggFuncCreator::createLocalFunction(
        const vector<ValueType> &inputTypes,
        const vector<string> &inputFields,
        const vector<string> &outputFields);
```

<a name="qo9h3"></a>
### merge phase

In the merge phase, OpenSearch Vector Search Edition uses the QRS node to process the data that is reported by each Searcher worker and then returns the final result. The main processing is within the `merge` function of AggFunc

```cpp
// Initialize some 'ColumnData' objects to access data in the input table, including the Accumulator data.
virtual bool initMergeInput(const TablePtr &inputTable);
// Specify output fields.
virtual bool initResultOutput(const TablePtr &outputTable);
// The main merge operation. After Searcher workers report information about accumulators, OpenSearch Vector Search Edition aggregates the information.
virtual bool merge(Row inputRow, Accumulator *acc);
// Calculate and return the final result.
virtual bool outputResult(Accumulator *acc, Row outputRow) const;
```

Example: In a query, multiple Searcher workers return information about multiple accumulators that correspond to the same group whose group key is Apple. In the merge phase, OpenSearch Vector Search Edition aggregates the information about these accumulators and generates a new accumulator. In the settings of the new accumulator, the value of the GroupKey parameter is Apple.

In the merge phase, the AVG function is used to calculate the sum of the number of the accumulators that are collected from each group in the collect phase. The sum value is equal to the number of data entries that are collected by all Searcher workers from the corresponding group. The implementation method that is used in the merge phase is different from the implementation method that is used in the collect phase. You must use different functions in the phases.

```cpp
template<typename InputType, typename AccumulatorType>
bool AvgAggFunc<InputType, AccumulatorType>::merge(Row inputRow, Accumulator *acc) {
    AvgAccumulator<AccumulatorType> *avgAcc = static_cast<AvgAccumulator<AccumulatorType> *>(acc);
    avgAcc->count += _countColumn->get(inputRow);
    avgAcc->sum += _sumColumn->get(inputRow);
    return true;
}
```

After the accumulators of all groups are counted by the `merge` function, the engine sequentially passes the accumulators of each group to the `outputResult` function to obtain the final output. At this stage, the Avg function only needs to execute `avg = sum/count` to obtain the average value of the group.

```cpp
template<typename InputType, typename AccumulatorType>
bool AvgAggFunc<InputType, AccumulatorType>::outputResult(Accumulator *acc, Row outputRow) const {
    AvgAccumulator<AccumulatorType> *avgAcc = static_cast<AvgAccumulator<AccumulatorType> *>(acc);
    assert(avgAcc->count > 0);
    double avg = (double)avgAcc->sum / avgAcc->count;
    _avgColumn->set(outputRow, avg);
    return true;
}
```

The engine obtains the AggFunc of the current stage by accessing the `AvgAggFuncCreator::createGlobalFunction`. In this phase, the **input** field is the Accumulator field.
```cpp
AggFunc *AvgAggFuncCreator::createGlobalFunction(
        const vector<ValueType> &inputTypes,
        const vector<string> &inputFields,
        const vector<string> &outputFields);
```

<a name="JgMWF"></a>
### Special cases

To improve performance, when the execution engine finds that the `GROUP BY` field is the basis for Searchers data (that is, each Searcher does not have the same GroupKey), the merge phase of the UDAF is also sunk to the Searcher. In this case, each Searcher worker performs the operations that are required in the collect phase and merge phase in sequence. The Searcher workers do not perform accumulator serialization and deserialization because these operations do not affect the result. QRS workers only include the aggregated statistics that are reported by each Searcher worker in the response and do not perform merge operations.

In specific scenarios, you can use this optimization method to reduce the loads of the QRS node and improve query performance. You do not need to modify the code that defines UDAFs to use this optimization method.