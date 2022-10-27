#ifndef ISEARCH_LOGICAGGFUNC_H
#define ISEARCH_LOGICAGGFUNC_H

#include <ha3/sql/ops/agg/AggFunc.h>
#include <ha3/sql/ops/agg/AggFuncCreator.h>
#include <ha3/sql/data/TableUtil.h>

BEGIN_HA3_NAMESPACE(sql);

template<typename T>
class LogicAccumulator : public Accumulator {
public:
    LogicAccumulator()
        : value(0)
    {}
    LogicAccumulator(T val)
        : value(val)
    {}

    ~LogicAccumulator() {}
public:
    T value;
};

template<typename InputType>
class LogicAggFunc : public AggFunc {
public:
    typedef void (*OperatorProtoType)(InputType &a, InputType b);
public:
    LogicAggFunc(const std::vector<std::string> &inputs,
                 const std::vector<std::string> &outputs,
                 bool isLocal,
                 OperatorProtoType op,
                 InputType initVal = 0)
        : AggFunc(inputs, outputs, isLocal)
        , _inputColumn(NULL)
        , _sumColumn(NULL)
        , _operator(op)
        , _initAccVal(initVal)
    {}
    ~LogicAggFunc() {}
private:
    LogicAggFunc(const LogicAggFunc &);
    LogicAggFunc& operator=(const LogicAggFunc &);
public:
    //DEF_CREATE_ACCUMULATOR_FUNC(LogicAccumulator<InputType>)
    Accumulator *createAccumulator(autil::mem_pool::Pool *pool) override {
        return (new (pool->allocateUnsafe(sizeof(LogicAccumulator<InputType>)))
                LogicAccumulator<InputType>(_initAccVal));
    }

private:
    // local
    bool initCollectInput(const TablePtr &inputTable) override;
    bool initAccumulatorOutput(const TablePtr &outputTable) override;
    bool collect(Row inputRow, Accumulator *acc) override;
    bool outputAccumulator(Accumulator *acc, Row outputRow) const override;

private:
    ColumnData<InputType> *_inputColumn;
    ColumnData<InputType> *_sumColumn;
    OperatorProtoType _operator;
    InputType _initAccVal;

private:
    HA3_LOG_DECLARE();
};
HA3_LOG_SETUP_TEMPLATE(sql, LogicAggFunc, InputType);

// local
template<typename InputType>
bool LogicAggFunc<InputType>::initCollectInput(const TablePtr &inputTable) {
    assert(_inputFields.size() == 1);
    _inputColumn = TableUtil::getColumnData<InputType>(inputTable, _inputFields[0]);
    if (_inputColumn == nullptr) {
        return false;
    }
    return true;
};

template<typename InputType>
bool LogicAggFunc<InputType>::initAccumulatorOutput(const TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _sumColumn = TableUtil::declareAndGetColumnData<InputType>(outputTable, _outputFields[0], false);
    if (_sumColumn == nullptr) {
        return false;
    }
    return true;
}

template<typename InputType>
bool LogicAggFunc<InputType>::collect(Row inputRow, Accumulator *acc) {
    LogicAccumulator<InputType> *sumAcc = static_cast<LogicAccumulator<InputType> *>(acc);
    _operator(sumAcc->value, _inputColumn->get(inputRow));
    return true;
}

template<typename InputType>
bool LogicAggFunc<InputType>::outputAccumulator(Accumulator *acc, Row outputRow) const {
    LogicAccumulator<InputType> *sumAcc = static_cast<LogicAccumulator<InputType> *>(acc);
    _sumColumn->set(outputRow, sumAcc->value);
    return true;
}


#define DECLARE_LOGIC_AGG_FUNC_CREATOR(opAggFuncCreator)                \
    class Logic##opAggFuncCreator : public AggFuncCreator {             \
    private:                                                            \
    AggFunc *createLocalFunction(const std::vector<ValueType> &inputTypes, \
                                 const std::vector<std::string> &inputFields, \
                                 const std::vector<std::string> &outputFields) override; \
    AggFunc *createGlobalFunction(const std::vector<ValueType> &inputTypes, \
                                  const std::vector<std::string> &inputFields, \
                                  const std::vector<std::string> &outputFields) override; \
    private:                                                            \
    HA3_LOG_DECLARE();                                                  \
    };
DECLARE_LOGIC_AGG_FUNC_CREATOR(AndAggFuncCreator)
DECLARE_LOGIC_AGG_FUNC_CREATOR(OrAggFuncCreator)

#undef DECLARE_LOGIC_AGG_FUNC_CREATOR

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_LOGICAGGFUNC_H
