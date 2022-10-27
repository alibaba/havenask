#ifndef ISEARCH_COUNTAGGFUNC_H
#define ISEARCH_COUNTAGGFUNC_H

#include <ha3/sql/ops/agg/AggFunc.h>
#include <ha3/sql/ops/agg/AggFuncCreator.h>
#include <ha3/sql/data/TableUtil.h>

BEGIN_HA3_NAMESPACE(sql);

class CountAccumulator : public Accumulator {
public:
    CountAccumulator()
        : value(0)
    {}
    ~CountAccumulator() {}
public:
    uint64_t value;
};

class CountAggFunc : public AggFunc {
public:
    CountAggFunc(const std::vector<std::string> &inputs,
               const std::vector<std::string> &outputs,
               bool isLocal)
        : AggFunc(inputs, outputs, isLocal)
        , _inputColumn(NULL)
        , _countColumn(NULL)
    {}
    ~CountAggFunc() {}
private:
    CountAggFunc(const CountAggFunc &);
    CountAggFunc& operator=(const CountAggFunc &);
public:
    DEF_CREATE_ACCUMULATOR_FUNC(CountAccumulator)
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
    ColumnData<uint64_t> *_inputColumn;
    ColumnData<uint64_t> *_countColumn;
    HA3_LOG_DECLARE();
};

class CountAggFuncCreator : public AggFuncCreator {
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

#endif //ISEARCH_COUNTAGGFUNC_H
