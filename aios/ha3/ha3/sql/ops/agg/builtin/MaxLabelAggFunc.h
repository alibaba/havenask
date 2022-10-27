#ifndef ISEARCH_MaxLabelAGGFUNC_H
#define ISEARCH_MaxLabelAGGFUNC_H

#include <ha3/sql/ops/agg/AggFunc.h>
#include <ha3/sql/ops/agg/AggFuncCreator.h>
#include <ha3/sql/data/TableUtil.h>

BEGIN_HA3_NAMESPACE(sql);

template<typename T1, typename T2>
class MaxLabelAccumulator : public Accumulator {
public:
    MaxLabelAccumulator()
        : max(std::numeric_limits<T2>::lowest())
    {}
    ~MaxLabelAccumulator() {}
public:
    T1 label;
    T2 max;
};

template<typename InputType1, typename InputType2>
class MaxLabelAggFunc : public AggFunc {
public:
    using AccumulatorType = MaxLabelAccumulator<InputType1, InputType2>;
public:
    MaxLabelAggFunc(const std::vector<std::string> &inputs,
               const std::vector<std::string> &outputs,
               bool isLocal)
        : AggFunc(inputs, outputs, isLocal)
        , _inputlabelColumn(NULL)
        , _inputValueColumn(NULL)
        , _labelColumn(NULL)
        , _maxColumn(NULL)
    {}
    ~MaxLabelAggFunc() {}
private:
    MaxLabelAggFunc(const MaxLabelAggFunc &);
    MaxLabelAggFunc& operator=(const MaxLabelAggFunc &);
public:
    Accumulator *createAccumulator(autil::mem_pool::Pool *pool) override {
        return POOL_NEW_CLASS(pool, AccumulatorType);
    }
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
    ColumnData<InputType1> *_inputlabelColumn;
    ColumnData<InputType2> *_inputValueColumn;
    ColumnData<InputType1> *_labelColumn;
    ColumnData<InputType2> *_maxColumn;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP_TEMPLATE_2(sql, MaxLabelAggFunc, InputType1, InputType2);

// local
template<typename InputType1, typename InputType2>
bool MaxLabelAggFunc<InputType1, InputType2>::initCollectInput(const TablePtr &inputTable) {
    assert(_inputFields.size() == 2);
    _inputlabelColumn = TableUtil::getColumnData<InputType1>(inputTable, _inputFields[0]);
    if (_inputlabelColumn == nullptr) {
        return false;
    }

    _inputValueColumn = TableUtil::getColumnData<InputType2>(inputTable, _inputFields[1]);
    if (_inputValueColumn == nullptr) {
        return false;
    }
    return true;
};

template<typename InputType1, typename InputType2>
bool MaxLabelAggFunc<InputType1, InputType2>::initAccumulatorOutput(const TablePtr &outputTable) {
    assert(_outputFields.size() == 2);
    _labelColumn = TableUtil::declareAndGetColumnData<InputType1>(outputTable, _outputFields[0], false);
    if (_labelColumn == nullptr) {
        return false;
    }
    
    _maxColumn = TableUtil::declareAndGetColumnData<InputType2>(outputTable, _outputFields[1], false);
    if (_maxColumn == nullptr) {
        return false;
    }

    outputTable->endGroup();
    return true;
}

template<typename InputType1, typename InputType2>
bool MaxLabelAggFunc<InputType1, InputType2>::collect(Row inputRow, Accumulator *acc) {
    AccumulatorType *MaxLabelAcc = static_cast<AccumulatorType *>(acc);
    auto temp = MaxLabelAcc->max;
    MaxLabelAcc->max = std::max(MaxLabelAcc->max, _inputValueColumn->get(inputRow));
    if (temp != MaxLabelAcc->max) {
        MaxLabelAcc->label = _inputlabelColumn->get(inputRow);
    }
    return true;
}

template<typename InputType1, typename InputType2>
bool MaxLabelAggFunc<InputType1, InputType2>::outputAccumulator(Accumulator *acc, Row outputRow) const {
    AccumulatorType *MaxLabelAcc = static_cast<AccumulatorType *>(acc);
    _labelColumn->set(outputRow, MaxLabelAcc->label);
    _maxColumn->set(outputRow, MaxLabelAcc->max);
    return true;
}

// global
template<typename InputType1, typename InputType2>
bool MaxLabelAggFunc<InputType1, InputType2>::initMergeInput(const TablePtr &inputTable) {
    return initCollectInput(inputTable);
}

template<typename InputType1, typename InputType2>
bool MaxLabelAggFunc<InputType1, InputType2>::initResultOutput(const TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _labelColumn = TableUtil::declareAndGetColumnData<InputType1>(outputTable, _outputFields[0], false);
    if (_labelColumn == nullptr) {
        return false;
    }
    return true;
}

template<typename InputType1, typename InputType2>
bool MaxLabelAggFunc<InputType1, InputType2>::merge(Row inputRow, Accumulator *acc) {
    return collect(inputRow, acc);
}

template<typename InputType1, typename InputType2>
bool MaxLabelAggFunc<InputType1, InputType2>::outputResult(Accumulator *acc, Row outputRow) const {
    AccumulatorType *MaxLabelAcc = static_cast<AccumulatorType *>(acc);
    _labelColumn->set(outputRow, MaxLabelAcc->label);
    return true;
}
class  MaxLabelAggFuncCreator : public AggFuncCreator {
private:
    AggFunc *createLocalFunction(const std::vector<ValueType> &inputTypes,
                                 const std::vector<std::string> &inputFields,
                                 const std::vector<std::string> &outputFields) override;
    AggFunc *createGlobalFunction(const std::vector<ValueType> &inputTypes,
                                  const std::vector<std::string> &inputFields,
                                  const std::vector<std::string> &outputFields) override;
    AggFunc *listLabelType(const std::vector<ValueType> &inputTypes,
                           const std::vector<std::string> &inputFields,
                           const std::vector<std::string> &outputFields,
                           bool isLocal);
    template <typename T>
    AggFunc *listValueType(const std::vector<ValueType> &inputTypes,
                           const std::vector<std::string> &inputFields,
                           const std::vector<std::string> &outputFields,
                           bool isLocal);
private:
    HA3_LOG_DECLARE();

};

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_MaxLabelAGGFUNC_H
