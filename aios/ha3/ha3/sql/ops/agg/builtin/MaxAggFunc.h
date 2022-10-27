#ifndef ISEARCH_MAXAGGFUNC_H
#define ISEARCH_MAXAGGFUNC_H

#include <ha3/sql/ops/agg/AggFunc.h>
#include <ha3/sql/ops/agg/AggFuncCreator.h>
#include <ha3/sql/data/TableUtil.h>

BEGIN_HA3_NAMESPACE(sql);

template<typename T>
class MaxAccumulator : public Accumulator {
public:
    MaxAccumulator()
        : isFirstAggregate(true)
    {}
    ~MaxAccumulator() {}
public:
    T value;
    bool isFirstAggregate;
};

template<typename InputType>
class MaxAggFunc : public AggFunc {
public:
    MaxAggFunc(const std::vector<std::string> &inputs,
               const std::vector<std::string> &outputs,
               bool isLocal)
        : AggFunc(inputs, outputs, isLocal)
        , _inputColumn(NULL)
        , _maxColumn(NULL)
    {}
    ~MaxAggFunc() {}
private:
    MaxAggFunc(const MaxAggFunc &);
    MaxAggFunc& operator=(const MaxAggFunc &);
public:
    DEF_CREATE_ACCUMULATOR_FUNC(MaxAccumulator<InputType>)
private:
    // local
    bool initCollectInput(const TablePtr &inputTable) override;
    bool initAccumulatorOutput(const TablePtr &outputTable) override;
    bool collect(Row inputRow, Accumulator *acc) override;
    bool outputAccumulator(Accumulator *acc, Row outputRow) const override;
private:
    ColumnData<InputType> *_inputColumn;
    ColumnData<InputType> *_maxColumn;
};

// local
template<typename InputType>
bool MaxAggFunc<InputType>::initCollectInput(const TablePtr &inputTable) {
    assert(_inputFields.size() == 1);
    _inputColumn = TableUtil::getColumnData<InputType>(inputTable, _inputFields[0]);
    if (_inputColumn == nullptr) {
        return false;
    }
    return true;
};

template<typename InputType>
bool MaxAggFunc<InputType>::initAccumulatorOutput(const TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _maxColumn = TableUtil::declareAndGetColumnData<InputType>(outputTable, _outputFields[0], false);
    if (_maxColumn == nullptr) {
        return false;
    }
    return true;
}

template<typename InputType>
bool MaxAggFunc<InputType>::collect(Row inputRow, Accumulator *acc) {
    MaxAccumulator<InputType> *maxAcc = static_cast<MaxAccumulator<InputType> *>(acc);
    if (maxAcc->isFirstAggregate) {
        maxAcc->value = _inputColumn->get(inputRow);
        maxAcc->isFirstAggregate = false;
    } else {
        maxAcc->value = std::max(maxAcc->value, _inputColumn->get(inputRow));
    }
    return true;
}

template<typename InputType>
bool MaxAggFunc<InputType>::outputAccumulator(Accumulator *acc, Row outputRow) const {
    MaxAccumulator<InputType> *maxAcc = static_cast<MaxAccumulator<InputType> *>(acc);
    _maxColumn->set(outputRow, maxAcc->value);
    return true;
}

class MaxAggFuncCreator : public AggFuncCreator {
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

#endif //ISEARCH_MAXAGGFUNC_H
