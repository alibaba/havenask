#pragma once

#include <ha3/sql/common/common.h>
#include <ha3/sql/data/Table.h>
#include <ha3/sql/ops/agg/Accumulator.h>
#include <autil/mem_pool/Pool.h>

BEGIN_HA3_NAMESPACE(sql);

class AggFunc
{
public:
    AggFunc(const std::vector<std::string> &inputs,
            const std::vector<std::string> &outputs,
            bool isLocal)
        : _inputFields(inputs)
        , _outputFields(outputs)
        , _isLocal(isLocal)
        , _pool(nullptr)
        , _funcPool(nullptr)
    {}
    virtual ~AggFunc() {}
private:
    AggFunc(const AggFunc &);
    AggFunc& operator=(const AggFunc &);
public:
    void setPool(autil::mem_pool::Pool *pool, autil::mem_pool::Pool *funcPool) {
        _pool = pool;
        _funcPool = funcPool;
    }
    std::string getName() const {
        return _name;
    }
    void setName(const std::string &name) {
        _name = name;
    }
    virtual Accumulator *createAccumulator(autil::mem_pool::Pool *pool) = 0;
private:
    // local
    virtual bool initCollectInput(const TablePtr &inputTable) {
        return true;
    };
    virtual bool initAccumulatorOutput(const TablePtr &outputTable) = 0;
    virtual bool collect(Row inputRow, Accumulator *acc) = 0;
    virtual bool outputAccumulator(Accumulator *acc, Row outputRow) const = 0;

    // global
    virtual bool initMergeInput(const TablePtr &inputTable) {
        return false;
    }
    virtual bool initResultOutput(const TablePtr &outputTable) {
        return false;
    }
    virtual bool merge(Row inputRow, Accumulator *acc) {
        return false;
    }
    virtual bool outputResult(Accumulator *acc, Row outputRow) const {
        return false;
    }
public:
    bool initInput(const TablePtr &inputTable) {
        if (_isLocal) {
            return initCollectInput(inputTable);
        } else {
            return initMergeInput(inputTable);
        }
    }
    bool initOutput(const TablePtr &inputTable) {
        if (_isLocal) {
            return initAccumulatorOutput(inputTable);
        } else {
            return initResultOutput(inputTable);
        }
    }
    bool aggregate(Row inputRow, Accumulator *acc) {
        if (_isLocal) {
            return collect(inputRow, acc);
        } else {
            return merge(inputRow, acc);
        }
    }
    bool setResult(Accumulator *acc, Row outputRow) {
        if (_isLocal) {
            return outputAccumulator(acc, outputRow);
        } else {
            return outputResult(acc, outputRow);
        }
    }
protected:
    std::vector<std::string> _inputFields;
    std::vector<std::string> _outputFields;
    bool _isLocal;
    std::string _name;
    autil::mem_pool::Pool *_pool;
    autil::mem_pool::Pool *_funcPool;
};


#define DEF_CREATE_ACCUMULATOR_FUNC(accType)                            \
    Accumulator *createAccumulator(autil::mem_pool::Pool *pool) override { \
        return (new (pool->allocateUnsafe(sizeof(accType)))accType);    \
    }

HA3_TYPEDEF_PTR(AggFunc);

END_HA3_NAMESPACE(sql);
