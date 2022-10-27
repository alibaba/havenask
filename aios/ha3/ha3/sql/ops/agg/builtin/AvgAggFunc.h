#ifndef ISEARCH_AVGAGGFUNC_H
#define ISEARCH_AVGAGGFUNC_H

#include <ha3/sql/ops/agg/AggFunc.h>
#include <ha3/sql/ops/agg/AggFuncCreator.h>
#include <ha3/sql/data/TableUtil.h>

BEGIN_HA3_NAMESPACE(sql);

template<typename T>
class AvgAccumulator : public Accumulator {
public:
    AvgAccumulator()
        : count(0)
        , sum(0)
    {}
    ~AvgAccumulator() {}
public:
    uint64_t count;
    T sum;
};

template<typename InputType, typename AccumulatorType>
class AvgAggFunc : public AggFunc {
public:
    AvgAggFunc(const std::vector<std::string> &inputs,
               const std::vector<std::string> &outputs,
               bool isLocal)
        : AggFunc(inputs, outputs, isLocal)
        , _inputColumn(NULL)
        , _countColumn(NULL)
        , _sumColumn(NULL)
        , _avgColumn(NULL)
    {}
    ~AvgAggFunc() {}
private:
    AvgAggFunc(const AvgAggFunc &);
    AvgAggFunc& operator=(const AvgAggFunc &);
public:
    DEF_CREATE_ACCUMULATOR_FUNC(AvgAccumulator<AccumulatorType>)
private:
    // local
    bool initCollectInput(const TablePtr &inputTable) override;
    bool initAccumulatorOutput(const TablePtr &outputTable) override;
    bool collect(Row inputRow, Accumulator *acc) override;
    bool outputAccumulator(Accumulator *acc, Row outputRow) const override;
    // global
    bool initMergeInput(const TablePtr &inputTable) override;
    bool initResultOutput(const TablePtr &outputTable) override;
    bool merge(Row inputRow, Accumulator *acc) override;
    bool outputResult(Accumulator *acc, Row outputRow) const override;
private:
    ColumnData<InputType> *_inputColumn;
    ColumnData<uint64_t> *_countColumn;
    ColumnData<AccumulatorType> *_sumColumn;
    ColumnData<double> *_avgColumn;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP_TEMPLATE_2(sql, AvgAggFunc, InputType, AccumulatorType);

// local
template<typename InputType, typename AccumulatorType>
bool AvgAggFunc<InputType, AccumulatorType>::initCollectInput(const TablePtr &inputTable) {
    assert(_inputFields.size() == 1);
    _inputColumn = TableUtil::getColumnData<InputType>(inputTable, _inputFields[0]);
    if (_inputColumn == nullptr) {
        return false;
    }
    return true;
};

template<typename InputType, typename AccumulatorType>
bool AvgAggFunc<InputType, AccumulatorType>::initAccumulatorOutput(const TablePtr &outputTable) {
    assert(_outputFields.size() == 2);
    _countColumn = TableUtil::declareAndGetColumnData<uint64_t>(outputTable, _outputFields[0], false);
    if (_countColumn == nullptr) {
        return false;
    }
    _sumColumn = TableUtil::declareAndGetColumnData<AccumulatorType>(outputTable, _outputFields[1], false);
    if (_sumColumn == nullptr) {
        return false;
    }
    outputTable->endGroup();
    return true;
}

template<typename InputType, typename AccumulatorType>
bool AvgAggFunc<InputType, AccumulatorType>::collect(Row inputRow, Accumulator *acc) {
    AvgAccumulator<AccumulatorType> *avgAcc = static_cast<AvgAccumulator<AccumulatorType> *>(acc);
    avgAcc->count++;
    avgAcc->sum += _inputColumn->get(inputRow);
    return true;
}

template<typename InputType, typename AccumulatorType>
bool AvgAggFunc<InputType, AccumulatorType>::outputAccumulator(Accumulator *acc, Row outputRow) const {
    AvgAccumulator<AccumulatorType> *avgAcc = static_cast<AvgAccumulator<AccumulatorType> *>(acc);
    _countColumn->set(outputRow, avgAcc->count);
    _sumColumn->set(outputRow, avgAcc->sum);
    return true;
}

// global
template<typename InputType, typename AccumulatorType>
bool AvgAggFunc<InputType, AccumulatorType>::initMergeInput(const TablePtr &inputTable) {
    assert(_inputFields.size() == 2);
    _countColumn = TableUtil::getColumnData<uint64_t>(inputTable, _inputFields[0]);
    if (_countColumn == nullptr) {
        return false;
    }
    _sumColumn = TableUtil::getColumnData<AccumulatorType>(inputTable, _inputFields[1]);
    if (_sumColumn == nullptr) {
        return false;
    }
    return true;
}

template<typename InputType, typename AccumulatorType>
bool AvgAggFunc<InputType, AccumulatorType>::initResultOutput(const TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _avgColumn = TableUtil::declareAndGetColumnData<double>(outputTable, _outputFields[0], false);
    if (_avgColumn == nullptr) {
        return false;
    }
    return true;
}

template<typename InputType, typename AccumulatorType>
bool AvgAggFunc<InputType, AccumulatorType>::merge(Row inputRow, Accumulator *acc) {
    AvgAccumulator<AccumulatorType> *avgAcc = static_cast<AvgAccumulator<AccumulatorType> *>(acc);
    avgAcc->count += _countColumn->get(inputRow);
    avgAcc->sum += _sumColumn->get(inputRow);
    return true;
}

template<typename InputType, typename AccumulatorType>
bool AvgAggFunc<InputType, AccumulatorType>::outputResult(Accumulator *acc, Row outputRow) const {
    AvgAccumulator<AccumulatorType> *avgAcc = static_cast<AvgAccumulator<AccumulatorType> *>(acc);
    assert(avgAcc->count > 0);
    double avg = (double)avgAcc->sum / avgAcc->count;
    _avgColumn->set(outputRow, avg);
    return true;
}

class AvgAggFuncCreator : public AggFuncCreator {
private:
    AggFunc *createLocalFunction(const std::vector<ValueType> &inputTypes,
                                 const std::vector<std::string> &inputFields,
                                 const std::vector<std::string> &outputFields) override;
    AggFunc *createGlobalFunction(const std::vector<ValueType> &inputTypes,
                                  const std::vector<std::string> &inputFields,
                                  const std::vector<std::string> &outputFields) override;
private:
    HA3_LOG_DECLARE();

};

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_AVGAGGFUNC_H
