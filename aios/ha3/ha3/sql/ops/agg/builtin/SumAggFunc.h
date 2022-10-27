#ifndef ISEARCH_SUMAGGFUNC_H
#define ISEARCH_SUMAGGFUNC_H

#include <ha3/sql/ops/agg/AggFunc.h>
#include <ha3/sql/ops/agg/AggFuncCreator.h>
#include <ha3/sql/data/TableUtil.h>

BEGIN_HA3_NAMESPACE(sql);

template<typename T>
class SumAccumulator : public Accumulator {
public:
    SumAccumulator()
        : value(0)
    {}
    ~SumAccumulator() {}
public:
    T value;
};

template<typename InputType, typename AccumulatorType>
class SumAggFunc : public AggFunc {
public:
    SumAggFunc(const std::vector<std::string> &inputs,
               const std::vector<std::string> &outputs,
               bool isLocal)
        : AggFunc(inputs, outputs, isLocal)
        , _inputColumn(NULL)
        , _sumColumn(NULL)
    {}
    ~SumAggFunc() {}
private:
    SumAggFunc(const SumAggFunc &);
    SumAggFunc& operator=(const SumAggFunc &);
public:
    DEF_CREATE_ACCUMULATOR_FUNC(SumAccumulator<AccumulatorType>)
private:
    // local
    bool initCollectInput(const TablePtr &inputTable) override;
    bool initAccumulatorOutput(const TablePtr &outputTable) override;
    bool collect(Row inputRow, Accumulator *acc) override;
    bool outputAccumulator(Accumulator *acc, Row outputRow) const override;
private:
    ColumnData<InputType> *_inputColumn;
    ColumnData<AccumulatorType> *_sumColumn;
private:
    HA3_LOG_DECLARE();
};
HA3_LOG_SETUP_TEMPLATE_2(sql, SumAggFunc, InputType, AccumulatorType);

// local
template<typename InputType, typename AccumulatorType>
bool SumAggFunc<InputType, AccumulatorType>::initCollectInput(const TablePtr &inputTable) {
    assert(_inputFields.size() == 1);
    _inputColumn = TableUtil::getColumnData<InputType>(inputTable, _inputFields[0]);
    if (_inputColumn == nullptr) {
        return false;
    }
    return true;
};

template<typename InputType, typename AccumulatorType>
bool SumAggFunc<InputType, AccumulatorType>::initAccumulatorOutput(const TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _sumColumn = TableUtil::declareAndGetColumnData<AccumulatorType>(outputTable, _outputFields[0], false);
    if (_sumColumn == nullptr) {
        return false;
    }
    return true;
}

template<typename InputType, typename AccumulatorType>
bool SumAggFunc<InputType, AccumulatorType>::collect(Row inputRow, Accumulator *acc) {
    SumAccumulator<AccumulatorType> *sumAcc = static_cast<SumAccumulator<AccumulatorType> *>(acc);
    sumAcc->value += _inputColumn->get(inputRow);
    return true;
}

template<typename InputType, typename AccumulatorType>
bool SumAggFunc<InputType, AccumulatorType>::outputAccumulator(Accumulator *acc, Row outputRow) const {
    SumAccumulator<AccumulatorType> *sumAcc = static_cast<SumAccumulator<AccumulatorType> *>(acc);
    _sumColumn->set(outputRow, sumAcc->value);
    return true;
}

class SumAggFuncCreator : public AggFuncCreator {
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

#endif //ISEARCH_SUMAGGFUNC_H
