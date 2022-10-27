#ifndef ISEARCH_MINAGGFUNC_H
#define ISEARCH_MINAGGFUNC_H

#include <ha3/sql/ops/agg/AggFunc.h>
#include <ha3/sql/ops/agg/AggFuncCreator.h>
#include <ha3/sql/data/TableUtil.h>

BEGIN_HA3_NAMESPACE(sql);

template<typename T>
class MinAccumulator : public Accumulator {
public:
    MinAccumulator()
        : isFirstAggregate(true)
    {}
    ~MinAccumulator() {}
public:
    T value;
    bool isFirstAggregate;
};

template<typename InputType>
class MinAggFunc : public AggFunc {
public:
    MinAggFunc(const std::vector<std::string> &inputs,
               const std::vector<std::string> &outputs,
               bool isLocal)
        : AggFunc(inputs, outputs, isLocal)
        , _inputColumn(NULL)
        , _minColumn(NULL)
    {}
    ~MinAggFunc() {}
private:
    MinAggFunc(const MinAggFunc &);
    MinAggFunc& operator=(const MinAggFunc &);
public:
    DEF_CREATE_ACCUMULATOR_FUNC(MinAccumulator<InputType>)
private:
    // local
    bool initCollectInput(const TablePtr &inputTable) override;
    bool initAccumulatorOutput(const TablePtr &outputTable) override;
    bool collect(Row inputRow, Accumulator *acc) override;
    bool outputAccumulator(Accumulator *acc, Row outputRow) const override;
private:
    ColumnData<InputType> *_inputColumn;
    ColumnData<InputType> *_minColumn;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP_TEMPLATE(sql, MinAggFunc, InputType);

// local
template<typename InputType>
bool MinAggFunc<InputType>::initCollectInput(const TablePtr &inputTable) {
    assert(_inputFields.size() == 1);
    _inputColumn = TableUtil::getColumnData<InputType>(inputTable, _inputFields[0]);
    if (_inputColumn == nullptr) {
        return false;
    }
    return true;
};

template<typename InputType>
bool MinAggFunc<InputType>::initAccumulatorOutput(const TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _minColumn = TableUtil::declareAndGetColumnData<InputType>(outputTable, _outputFields[0], false);
    if (_minColumn == nullptr) {
        return false;
    }
    return true;
}

template<typename InputType>
bool MinAggFunc<InputType>::collect(Row inputRow, Accumulator *acc) {
    MinAccumulator<InputType> *minAcc = static_cast<MinAccumulator<InputType> *>(acc);
    if (minAcc->isFirstAggregate) {
        minAcc->value = _inputColumn->get(inputRow);
        minAcc->isFirstAggregate = false;
    } else {
        minAcc->value = std::min(minAcc->value, _inputColumn->get(inputRow));
    }
    return true;
}

template<typename InputType>
bool MinAggFunc<InputType>::outputAccumulator(Accumulator *acc, Row outputRow) const {
    MinAccumulator<InputType> *minAcc = static_cast<MinAccumulator<InputType> *>(acc);
    _minColumn->set(outputRow, minAcc->value);
    return true;
}

class MinAggFuncCreator : public AggFuncCreator {
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

#endif //ISEARCH_MINAGGFUNC_H
