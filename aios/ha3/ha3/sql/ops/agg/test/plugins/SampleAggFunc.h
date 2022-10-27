#pragma once

#include <ha3/sql/ops/agg/AggFunc.h>
#include <ha3/sql/ops/agg/AggFuncCreator.h>
#include <ha3/sql/data/TableUtil.h>

BEGIN_HA3_NAMESPACE(sql);

template<typename T>
class SampleAccumulator : public Accumulator {
public:
    SampleAccumulator()
        : value(0)
    {}
    ~SampleAccumulator() {}
public:
    T value;
};

template<typename InputType, typename AccumulatorType>
class SampleAggFunc : public AggFunc {
public:
    SampleAggFunc(const std::vector<std::string> &inputs,
               const std::vector<std::string> &outputs,
               bool isLocal)
        : AggFunc(inputs, outputs, isLocal)
        , _inputColumn(NULL)
        , _sumColumn(NULL)
    {}
    ~SampleAggFunc() {}
private:
    SampleAggFunc(const SampleAggFunc &);
    SampleAggFunc& operator=(const SampleAggFunc &);
public:
    DEF_CREATE_ACCUMULATOR_FUNC(SampleAccumulator<AccumulatorType>)
private:
    // local
    bool initCollectInput(const TablePtr &inputTable) override;
    bool initAccumulatorOutput(const TablePtr &outputTable) override;
    bool collect(Row inputRow, Accumulator *acc) override;
    bool outputAccumulator(Accumulator *acc, Row outputRow) const override;
private:
    ColumnData<InputType> *_inputColumn;
    ColumnData<AccumulatorType> *_sumColumn;
};

// local
template<typename InputType, typename AccumulatorType>
bool SampleAggFunc<InputType, AccumulatorType>::initCollectInput(const TablePtr &inputTable) {
    assert(_inputFields.size() == 1);
    _inputColumn = TableUtil::getColumnData<InputType>(inputTable, _inputFields[0]);
    if (_inputColumn == nullptr) {
        return false;
    }
    return true;
};

template<typename InputType, typename AccumulatorType>
bool SampleAggFunc<InputType, AccumulatorType>::initAccumulatorOutput(const TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _sumColumn = TableUtil::declareAndGetColumnData<AccumulatorType>(outputTable, _outputFields[0], false);
    if (_sumColumn == nullptr) {
        return false;
    }
    return true;
}

template<typename InputType, typename AccumulatorType>
bool SampleAggFunc<InputType, AccumulatorType>::collect(Row inputRow, Accumulator *acc) {
    SampleAccumulator<AccumulatorType> *sumAcc = static_cast<SampleAccumulator<AccumulatorType> *>(acc);
    sumAcc->value += _inputColumn->get(inputRow) * 2;
    return true;
}

template<typename InputType, typename AccumulatorType>
bool SampleAggFunc<InputType, AccumulatorType>::outputAccumulator(Accumulator *acc, Row outputRow) const {
    SampleAccumulator<AccumulatorType> *sumAcc = static_cast<SampleAccumulator<AccumulatorType> *>(acc);
    _sumColumn->set(outputRow, sumAcc->value);
    return true;
}

class SampleAggFuncCreator : public AggFuncCreator {
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
