#pragma once

#include <assert.h>
#include <stddef.h>
#include <string>
#include <vector>

#include "autil/mem_pool/Pool.h"
#include "navi/common.h"
#include "sql/ops/agg/Accumulator.h"
#include "sql/ops/agg/AggFunc.h"
#include "sql/ops/agg/AggFuncCreatorR.h"
#include "sql/ops/agg/AggFuncMode.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"

namespace navi {
class ResourceInitContext;
} // namespace navi
namespace table {
template <typename T>
class ColumnData;
} // namespace table

namespace sql {

template <typename T>
class SampleAccumulator : public Accumulator {
public:
    SampleAccumulator()
        : value(0) {}
    ~SampleAccumulator() {}

public:
    T value;
};

template <typename InputType, typename AccumulatorType>
class SampleAggFunc : public AggFunc {
public:
    SampleAggFunc(const std::vector<std::string> &inputs,
                  const std::vector<std::string> &outputs,
                  AggFuncMode mode)
        : AggFunc(inputs, outputs, mode)
        , _inputColumn(NULL)
        , _sumColumn(NULL) {}
    ~SampleAggFunc() {}

private:
    SampleAggFunc(const SampleAggFunc &);
    SampleAggFunc &operator=(const SampleAggFunc &);

public:
    DEF_CREATE_ACCUMULATOR_FUNC(SampleAccumulator<AccumulatorType>)
private:
    // local
    bool initCollectInput(const table::TablePtr &inputTable) override;
    bool initAccumulatorOutput(const table::TablePtr &outputTable) override;
    bool collect(table::Row inputRow, Accumulator *acc) override;
    bool outputAccumulator(Accumulator *acc, table::Row outputRow) const override;

private:
    table::ColumnData<InputType> *_inputColumn;
    table::ColumnData<AccumulatorType> *_sumColumn;
};

// local
template <typename InputType, typename AccumulatorType>
bool SampleAggFunc<InputType, AccumulatorType>::initCollectInput(
    const table::TablePtr &inputTable) {
    assert(_inputFields.size() == 1);
    _inputColumn = table::TableUtil::getColumnData<InputType>(inputTable, _inputFields[0]);
    if (_inputColumn == nullptr) {
        return false;
    }
    return true;
};

template <typename InputType, typename AccumulatorType>
bool SampleAggFunc<InputType, AccumulatorType>::initAccumulatorOutput(
    const table::TablePtr &outputTable) {
    assert(_outputFields.size() == 1);
    _sumColumn = table::TableUtil::declareAndGetColumnData<AccumulatorType>(
        outputTable, _outputFields[0], false);
    if (_sumColumn == nullptr) {
        return false;
    }
    return true;
}

template <typename InputType, typename AccumulatorType>
bool SampleAggFunc<InputType, AccumulatorType>::collect(table::Row inputRow, Accumulator *acc) {
    SampleAccumulator<AccumulatorType> *sumAcc
        = static_cast<SampleAccumulator<AccumulatorType> *>(acc);
    sumAcc->value += _inputColumn->get(inputRow) * 2;
    return true;
}

template <typename InputType, typename AccumulatorType>
bool SampleAggFunc<InputType, AccumulatorType>::outputAccumulator(Accumulator *acc,
                                                                  table::Row outputRow) const {
    SampleAccumulator<AccumulatorType> *sumAcc
        = static_cast<SampleAccumulator<AccumulatorType> *>(acc);
    _sumColumn->set(outputRow, sumAcc->value);
    return true;
}

class SampleAggFuncCreator : public AggFuncCreatorR {
public:
    std::string getResourceName() const override {
        return "SQL_AGG_FUNC_sum2";
    }
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;
    std::string getAggFuncName() const override {
        return "sum2";
    }

private:
    AggFunc *createLocalFunction(const std::vector<table::ValueType> &inputTypes,
                                 const std::vector<std::string> &inputFields,
                                 const std::vector<std::string> &outputFields) override;
    AggFunc *createNormalFunction(const std::vector<table::ValueType> &inputTypes,
                                  const std::vector<std::string> &inputFields,
                                  const std::vector<std::string> &outputFields) override {
        return createLocalFunction(inputTypes, inputFields, outputFields);
    }
    AggFunc *createGlobalFunction(const std::vector<table::ValueType> &inputTypes,
                                  const std::vector<std::string> &inputFields,
                                  const std::vector<std::string> &outputFields) override {
        return createLocalFunction(inputTypes, inputFields, outputFields);
    }
};

} // namespace sql
