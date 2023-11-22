<a name="Sm4kB"></a>

### How does a UDAF work?

You can implement each user-defined aggregation function (UDAF) in a single class that is inherited from AggFunc of the base class and supports the required interfaces. The interfaces are classified into local interfaces and global interfaces. The following sample code describes the required interfaces:



```cpp

// You can use the following functions in the collect phase.

virtual bool initCollectInput(const TablePtr &inputTable);

virtual bool initAccumulatorOutput(const TablePtr &outputTable) = 0;

virtual bool collect(Row inputRow, Accumulator *acc) = 0;

virtual bool outputAccumulator(Accumulator *acc, Row outputRow) const = 0;



// You can use the following functions in the merge phase.

virtual bool initMergeInput(const TablePtr &inputTable);

virtual bool initResultOutput(const TablePtr &outputTable);

virtual bool merge(Row inputRow, Accumulator *acc);

virtual bool outputResult(Accumulator *acc, Row outputRow) const;

```





Havenask executes a UDAF in the following phases: Phase 1 that indicates the collect phase and Phase 2 that indicates the merge phase. In the collect phase, Havenask calls the collect functions to process the data sets that are obtained by Searcher workers and generates estimated statistics based on the data sets. After all Searcher workers report collected data, the system sends the intermediate data to the Query Result Searcher (QRS) node. Then, QRS workers call the merge functions to process the intermediate data and return the final result.



The following sections describe how to use the AVG function in Havenask and provide examples to help you understand the two-phase implementation process of a UDAF function.



<a name="E5C96"></a>

#### Collect phase

In the collect phase, Havenask uses the Searcher node to search for the required data. After Searcher workers collect the required data, Havenask calls the `collect` method of AggFunc to process the data, generate estimated statistics, and then save the intermediate data that is obtained in corresponding accumulators. Havenask uses an accumulator for each data group to store the status information of the group during data processing.



```cpp

// Specifies the input fields from which you want to query data.

virtual bool initCollectInput(const TablePtr &inputTable);

// Specifies the output fields based on which you want the system to sort the corresponding accumulators and send the data of the accumulators to the output table.

virtual bool initAccumulatorOutput(const TablePtr &outputTable) = 0;

// The main collect operation. The system saves statistical results of multiple rows of input data in the corresponding accumulators.

virtual bool collect(Row inputRow, Accumulator *acc) = 0;

// Sorts accumulators and sends the data of the accumulators to the output table for data transmission. Then, the system sends the output data of the Searcher node to the QRS node.

virtual bool outputAccumulator(Accumulator *acc, Row outputRow) const = 0;



```



Before Havenask calls the collect method to process data from each row, the engine calculates a group key for the data of each row, obtains the accumulator of each data group, and then sends the data of each group and the corresponding accumulator to the QRS node. Havenask identifies data groups based on group keys. After the returned data is obtained, you can change the states of the accumulators based on the data.



In the collect phase, Havenask collects the number of entries in each group and calculates the sum of the numeric values in each group. The average value is calculated based on these values.



```cpp

template<typename InputType, typename AccumulatorType>

bool AvgAggFunc<InputType, AccumulatorType>::collect(Row inputRow, Accumulator *acc) {

    AvgAccumulator<AccumulatorType> *avgAcc = static_cast<AvgAccumulator<AccumulatorType> *>(acc);

    avgAcc->count++;

    avgAcc->sum += _inputColumn->get(inputRow);

    return true;

}

```



Havenask calls the `AvgAggFuncCreator::createLocalFunction` interface to obtain the aggregation functions that can be used in the current phase. Note: In the collect phase, Havenask **returns** all attributes of the accumulators that are specified by the `AvgAccumulator` parameter.

```cpp

AggFunc *AvgAggFuncCreator::createLocalFunction(

        const vector<ValueType> &inputTypes,

        const vector<string> &inputFields,

        const vector<string> &outputFields);

```



<a name="qo9h3"></a>

#### Merge phase



In the merge phase, Havenask uses the QRS node to process the data that is reported by each Searcher worker and then returns the final result. The main compute process is performed by the `merge` function of AggFunc.



```cpp

// Specifies the input fields from which you want to query data, including the fields that contain information about accumulators.

virtual bool initMergeInput(const TablePtr &inputTable);

// Specifies output fields.

virtual bool initResultOutput(const TablePtr &outputTable);

// The main merge operation. After Searcher workers report information about accumulators, Havenask aggregates the information.

virtual bool merge(Row inputRow, Accumulator *acc);

// Calculates and returns the final result.

virtual bool outputResult(Accumulator *acc, Row outputRow) const;

```



Example: In a query, multiple Searcher workers return information about multiple accumulators that correspond to the same group whose group key is Apple. In the merge phase, Havenask aggregates the information about these accumulators and generates a new accumulator. In the settings of the new accumulator, the value of the GroupKey parameter is Apple.



In the merge phase, the AVG function is used to calculate the sum of the number of the accumulators that are collected from each group in collect phases. The sum value is equal to the number of data entries that are collected by all Searcher workers from the corresponding group. In the merge phase, the implementation method that is used is different from the implementation method that is used in collect phase. You must use different functions in the phases.



```cpp

template<typename InputType, typename AccumulatorType>

bool AvgAggFunc<InputType, AccumulatorType>::merge(Row inputRow, Accumulator *acc) {

    AvgAccumulator<AccumulatorType> *avgAcc = static_cast<AvgAccumulator<AccumulatorType> *>(acc);

    avgAcc->count += _countColumn->get(inputRow);

    avgAcc->sum += _sumColumn->get(inputRow);

    return true;

}

```



After you use the `merge` function to calculate the number of accumulators of each group, Havenask passes the value of the accumulator count of each group to the `outputResult` function in sequence. Then, the outputResult function returns the final result. In this phase, Havenask executes only the `avg = sum/count` expression to obtain the average value of each group.



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



Havenask calls the `AvgAggFuncCreator::createGlobalFunction` interface to obtain the aggregation functions that can be used in the current phase. Note: In this phase, the **input** field is the Accumulator field.

```cpp

AggFunc *AvgAggFuncCreator::createGlobalFunction(

        const vector<ValueType> &inputTypes,

        const vector<string> &inputFields,

        const vector<string> &outputFields);

```



<a name="JgMWF"></a>

#### Special cases



When Havenask detects that the fields that are specified in the `GROUP BY` clause are the same as the group keys based on which Searcher workers identify data groups, Havenask distributes the merge operations of the UDAF to the Searcher node for execution. In this case, each Searcher worker performs the operations that are required in the collect phase and merge phase in sequence. The Searcher workers do not perform accumulator serialization and deserialization because these operations do not affect the result. QRS workers only include the aggregated statistics that are reported by each Searcher worker in the response and do not perform merge operations.



In specific scenarios, you can use this optimization method to reduce the loads of the QRS node and improve query performance. You do not need to modify the code that defines UDAFs to use this optimization method.