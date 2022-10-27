#ifndef ISEARCH_MULTIGATHERAGGFUNC_H
#define ISEARCH_MULTIGATHERAGGFUNC_H
#include <ha3/sql/ops/agg/AggFunc.h>
#include <ha3/sql/ops/agg/AggFuncCreator.h>
#include <ha3/sql/data/TableUtil.h>
#include <autil/mem_pool/PoolVector.h>
#include <autil/MultiValueCreator.h>
BEGIN_HA3_NAMESPACE(sql);

template<typename T>
class MultiGatherAccumulator : public Accumulator {
public:
    MultiGatherAccumulator(autil::mem_pool::Pool *pool)
        : poolVector(pool)
    {}
    ~MultiGatherAccumulator() {}
public:
    autil::mem_pool::PoolVector<T> poolVector;
};

template<>
class MultiGatherAccumulator<autil::MultiChar> : public Accumulator {
public:
    MultiGatherAccumulator(autil::mem_pool::Pool *pool)
        : poolVector(pool)
    {}
    ~MultiGatherAccumulator() {}
public:
    autil::mem_pool::PoolVector<std::string> poolVector;
};

template<typename InputType>
class MultiGatherAggFunc : public AggFunc {
public:
    MultiGatherAggFunc(const std::vector<std::string> &inputs,
               const std::vector<std::string> &outputs,
               bool isLocal)
        : AggFunc(inputs, outputs, isLocal)
        , _inputColumn(NULL)
        , _gatheredColumn(NULL)
        , _outputColumn(NULL)
    {}
    ~MultiGatherAggFunc() {}
private:
    MultiGatherAggFunc(const MultiGatherAggFunc &);
    MultiGatherAggFunc& operator=(const MultiGatherAggFunc &);
public:
    //DEF_CREATE_ACCUMULATOR_FUNC(MultiGatherAccumulator<InputType>)
    Accumulator *createAccumulator(autil::mem_pool::Pool *pool) override {
        return (new (pool->allocateUnsafe(sizeof(MultiGatherAccumulator<InputType>)))MultiGatherAccumulator<InputType>(pool));
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
    ColumnData<autil::MultiValueType<InputType> > *_inputColumn;
    ColumnData<autil::MultiValueType<InputType> > *_gatheredColumn;
    ColumnData<autil::MultiValueType<InputType> > *_outputColumn;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP_TEMPLATE(sql, MultiGatherAggFunc, InputType);

// local
template<typename InputType>
bool MultiGatherAggFunc<InputType>::initCollectInput(const TablePtr &inputTable) {
    assert(_inputFields.size() == 1);
    _inputColumn = TableUtil::getColumnData<autil::MultiValueType<InputType> >(inputTable, _inputFields[0]);
    if (_inputColumn == nullptr) {
        return false;
    }
    return true;
};

template<typename InputType>
bool MultiGatherAggFunc<InputType>::initAccumulatorOutput(const TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _gatheredColumn = TableUtil::declareAndGetColumnData<autil::MultiValueType<InputType> >(outputTable, _outputFields[0], false);
    if (_gatheredColumn == nullptr) {
        return false;
    }
    outputTable->endGroup();
    return true;
}

template<typename InputType>
bool MultiGatherAggFunc<InputType>::collect(Row inputRow, Accumulator *acc) {
    MultiGatherAccumulator<InputType> *gatherAcc = static_cast<MultiGatherAccumulator<InputType> *>(acc);
    autil::MultiValueType<InputType> inputVals = _inputColumn->get(inputRow);
    for (size_t i = 0; i < inputVals.size(); ++i) {
        gatherAcc->poolVector.push_back(inputVals[i]);
    }
    return true;
}

template<typename InputType>
bool MultiGatherAggFunc<InputType>::outputAccumulator(Accumulator *acc, Row outputRow) const {
    MultiGatherAccumulator<InputType> *gatherAcc = static_cast<MultiGatherAccumulator<InputType> *>(acc);
    autil::MultiValueType<InputType> outputRes;
    outputRes.init(autil::MultiValueCreator::createMultiValueBuffer(
                    gatherAcc->poolVector.data(), gatherAcc->poolVector.size(), _pool));
    _gatheredColumn->set(outputRow, outputRes);
    return true;
}

template<>
bool MultiGatherAggFunc<autil::MultiChar>::collect(Row inputRow, Accumulator *acc);

template<>
bool MultiGatherAggFunc<autil::MultiChar>::merge(Row inputRow, Accumulator *acc);

// global
template<typename InputType>
bool MultiGatherAggFunc<InputType>::initMergeInput(const TablePtr &inputTable) {
    assert(_inputFields.size() == 1);
    _gatheredColumn = TableUtil::getColumnData<autil::MultiValueType<InputType> >(inputTable, _inputFields[0]);
    if (_gatheredColumn == nullptr) {
        return false;
    }
    return true;
}

template<typename InputType>
bool MultiGatherAggFunc<InputType>::initResultOutput(const TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _outputColumn = TableUtil::declareAndGetColumnData<autil::MultiValueType<InputType> >(outputTable, _outputFields[0], false);
    if (_outputColumn == nullptr) {
        return false;
    }
    return true;
}

template<typename InputType>
bool MultiGatherAggFunc<InputType>::merge(Row inputRow, Accumulator *acc) {
    MultiGatherAccumulator<InputType> *gatherAcc = static_cast<MultiGatherAccumulator<InputType> *>(acc);
    autil::MultiValueType<InputType> values =  _gatheredColumn->get(inputRow);
    for (size_t i = 0; i < values.size(); i ++) {
        gatherAcc->poolVector.push_back(values[i]);
    }
    return true;
}

template<typename InputType>
bool MultiGatherAggFunc<InputType>::outputResult(Accumulator *acc, Row outputRow) const {
    MultiGatherAccumulator<InputType> *gatherAcc = static_cast<MultiGatherAccumulator<InputType> *>(acc);
    autil::MultiValueType<InputType> outputRes;
    outputRes.init(autil::MultiValueCreator::createMultiValueBuffer(gatherAcc->poolVector.data(), gatherAcc->poolVector.size(), _pool));
    _outputColumn->set(outputRow, outputRes);
    return true;
}

class MultiGatherAggFuncCreator : public AggFuncCreator {
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

#endif //ISEARCH_MULTIGATHERAGGFUNC_H
