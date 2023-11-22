---
toc: content
order: 3
---


# UDAF实现原理
每个UDAF的实现在单一的类中，该类需要继承自基类AggFunc并实现相关接口。主要接口如下（分为local和global两个部分）：

```cpp
// 以下四个函数供collect阶段调用
virtual bool initCollectInput(const TablePtr &inputTable);
virtual bool initAccumulatorOutput(const TablePtr &outputTable) = 0;
virtual bool collect(Row inputRow, Accumulator *acc) = 0;
virtual bool outputAccumulator(Accumulator *acc, Row outputRow) const = 0;

// 以下四个函数供merge阶段使用
virtual bool initMergeInput(const TablePtr &inputTable);
virtual bool initResultOutput(const TablePtr &outputTable);
virtual bool merge(Row inputRow, Accumulator *acc);
virtual bool outputResult(Accumulator *acc, Row outputRow) const;
```


UDAF在运行时主要分为两个阶段：1.collect阶段 2.merge阶段。这两个阶段分别运行在Searcher和Qrs上：执行引擎先会在Searcher环境下调用上述collect阶段的四个函数，在数据集上完成初步统计；待所有Searcher上的collect阶段执行完毕后，会将中间结果汇总到Qrs上，然后在Qrs环境下调用上述merge阶段的四个函数，对collect阶段的输出进一步加工，并输出最终结果。

下面以内置函数avg为例，结合havenask的运行流程，介绍该UDAF在这两个阶段的实现。

<a name="E5C96"></a>
### collect阶段
collect阶段会运行在Searcher上，引擎将根据本Searcher上的聚合结果，调用AggFunc的 `collect` 方法进行初步统计，并将结果保存在相应的Accumulator对象中。其中，每个分组会有自己对应的Accumulator，用于存放该分组统计过程中的状态信息。

```cpp
// 初始化一些`ColumnData`对象，用于访问输入表中数据
virtual bool initCollectInput(const TablePtr &inputTable);
// 初始化一些`ColumnData`对象，用于将Accumulator序列化到输出表
virtual bool initAccumulatorOutput(const TablePtr &outputTable) = 0;
// collect阶段主要过程，将多行输入数据的统计结果存储在对应Accumulator对象上
virtual bool collect(Row inputRow, Accumulator *acc) = 0;
// 将Accumulator序列化到输出表上（方便网络传输），之后会由Searcher汇总到Qrs上
virtual bool outputAccumulator(Accumulator *acc, Row outputRow) const = 0;

```

引擎对每行数据调用collect方法前，会先计算出该行数据的GroupKey作为分组依据，取出该分组对应的Accumulator一同传入。用户在收到当前数据后，根据数据内容修改Accumulator的状态。

对Avg函数来讲，collect阶段要做的事情就是记录当前Group下的数据条目数以及数值总和，为未来均值的计算作准备。

```cpp
template<typename InputType, typename AccumulatorType>
bool AvgAggFunc<InputType, AccumulatorType>::collect(Row inputRow, Accumulator *acc) {
    AvgAccumulator<AccumulatorType> *avgAcc = static_cast<AvgAccumulator<AccumulatorType> *>(acc);
    avgAcc->count++;
    avgAcc->sum += _inputColumn->get(inputRow);
    return true;
}
```

引擎通过访问 `AvgAggFuncCreator::createLocalFunction` ，获得当前阶段的AggFunc。注意，这一阶段的**输出**是 `AvgAccumulator` 上的所有属性。
```cpp
AggFunc *AvgAggFuncCreator::createLocalFunction(
        const vector<ValueType> &inputTypes,
        const vector<string> &inputFields,
        const vector<string> &outputFields);
```

<a name="qo9h3"></a>
### merge阶段

本阶段在Qrs上执行，用于将各个Searcher上返回的初步统计结果进行处理，输出最终的结果。主要的处理过程在AggFunc的 `merge` 函数内

```cpp
// 初始化一些`ColumnData`对象，用于访问输入表中数据，包括Accumulator数据
virtual bool initMergeInput(const TablePtr &inputTable);
// 初始化一些`ColumnData`对象，用于输出统计结果输出结果
virtual bool initResultOutput(const TablePtr &outputTable);
// merge阶段主要过程，将来自多个Searcher的Accumulator信息整合
virtual bool merge(Row inputRow, Accumulator *acc);
// 计算最终结果并输出
virtual bool outputResult(Accumulator *acc, Row outputRow) const;
```

例如单次查询中，多个Searcher中都有GroupKey=Apple的Accumulator上报，则在merge阶段引擎会为GroupKey=Apple新生成一个Accumulator，用于将各个Searcher传入的相关Accumulator信息聚合。

可以注意到Avg函数在merge阶段，需要将所有collect阶段的Accumulator.count累加，才能得到该Group在所有Searcher上的数据条目数。这与collect阶段统计count的实现并不相同，这也是为什么要针对两个阶段分别设置处理函数的原因。

```cpp
template<typename InputType, typename AccumulatorType>
bool AvgAggFunc<InputType, AccumulatorType>::merge(Row inputRow, Accumulator *acc) {
    AvgAccumulator<AccumulatorType> *avgAcc = static_cast<AvgAccumulator<AccumulatorType> *>(acc);
    avgAcc->count += _countColumn->get(inputRow);
    avgAcc->sum += _sumColumn->get(inputRow);
    return true;
}
```

当所有Group的Accumulator通过 `merge` 函数统计完毕后，引擎会依次将每个Group的Accumulator并传入`outputResult`函数获得最终输出输出。Avg函数这个阶段只需要执行 `avg = sum/count` 即可获得该Group的均值

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

引擎通过访问 `AvgAggFuncCreator::createGlobalFunction` ，获得当前阶段的AggFunc。注意，这一阶段的**输入**是Accumulator字段。
```cpp
AggFunc *AvgAggFuncCreator::createGlobalFunction(
        const vector<ValueType> &inputTypes,
        const vector<string> &inputFields,
        const vector<string> &outputFields);
```

<a name="JgMWF"></a>
### 特殊情况

为提升性能，当执行引擎发现 `GROUP BY` 的字段即为Searchers数据分列的依据时（即各个Searcher之间不会有数据的GroupKey相同），会将UDAF的merge阶段也下沉到Searcher上进行。即在单次Query中，每个Searcher会依次执行UDAF的collect阶段、merge两个阶段（但会跳过其中无用的Accumulator序列化和反序列化），Qrs仅仅将各个Searcher返回的聚合统计结果进行简单粘贴，不再执行merge阶段。

此优化可以有效降低某些场景下Qrs的负载，提升对Query的处理性能。该优化对UDAF的编写者是无感知的，无需为此调整UDAF的设计。