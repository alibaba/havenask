#ifndef ISEARCH_IDENTITYAGGFUNC_H
#define ISEARCH_IDENTITYAGGFUNC_H

#include <ha3/sql/ops/agg/AggFunc.h>
#include <ha3/sql/ops/agg/AggFuncCreator.h>
#include <ha3/sql/data/TableUtil.h>

BEGIN_HA3_NAMESPACE(sql);

template<typename T>
class IdentityAccumulator : public Accumulator {
public:
    IdentityAccumulator()
        : isFirstAggregate(true)
    {}
    ~IdentityAccumulator() {}
public:
    T value;
    bool isFirstAggregate;
};

template<typename InputType>
class IdentityAggFunc : public AggFunc {
public:
    IdentityAggFunc(const std::vector<std::string> &inputs,
               const std::vector<std::string> &outputs,
               bool isLocal)
        : AggFunc(inputs, outputs, isLocal)
        , _inputColumn(NULL)
        , _identityColumn(NULL)
    {}
    ~IdentityAggFunc() {}
private:
    IdentityAggFunc(const IdentityAggFunc &);
    IdentityAggFunc& operator=(const IdentityAggFunc &);
public:
    DEF_CREATE_ACCUMULATOR_FUNC(IdentityAccumulator<InputType>)
private:
    // local
    bool initCollectInput(const TablePtr &inputTable) override;
    bool initAccumulatorOutput(const TablePtr &outputTable) override;
    bool collect(Row inputRow, Accumulator *acc) override;
    bool outputAccumulator(Accumulator *acc, Row outputRow) const override;
private:
    ColumnData<InputType> *_inputColumn;
    ColumnData<InputType> *_identityColumn;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP_TEMPLATE(sql, IdentityAggFunc, InputType);

// local
template<typename InputType>
bool IdentityAggFunc<InputType>::initCollectInput(const TablePtr &inputTable) {
    assert(_inputFields.size() == 1);
    _inputColumn = TableUtil::getColumnData<InputType>(inputTable, _inputFields[0]);
    if (_inputColumn == nullptr) {
        return false;
    }
    return true;
};

template<typename InputType>
bool IdentityAggFunc<InputType>::initAccumulatorOutput(const TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _identityColumn = TableUtil::declareAndGetColumnData<InputType>(outputTable, _outputFields[0], false);
    if (_identityColumn == nullptr) {
        return false;
    }
    return true;
}

template<typename InputType>
bool IdentityAggFunc<InputType>::collect(Row inputRow, Accumulator *acc) {
    IdentityAccumulator<InputType> *identityAcc = static_cast<IdentityAccumulator<InputType> *>(acc);
    if (identityAcc->isFirstAggregate) {
        identityAcc->value = _inputColumn->get(inputRow);
        identityAcc->isFirstAggregate = false;
    }
    return true;
}

template<typename InputType>
bool IdentityAggFunc<InputType>::outputAccumulator(Accumulator *acc, Row outputRow) const {
    IdentityAccumulator<InputType> *identityAcc = static_cast<IdentityAccumulator<InputType> *>(acc);
    _identityColumn->set(outputRow, identityAcc->value);
    return true;
}

class IdentityAggFuncCreator : public AggFuncCreator {
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

#endif //ISEARCH_IDENTITYAGGFUNC_H
