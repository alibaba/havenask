#pragma once

#include <ha3/sql/common/common.h>
#include <ha3/sql/ops/agg/Aggregator.h>
#include <ha3/sql/data/Table.h>

BEGIN_HA3_NAMESPACE(sql);

class AggBase
{
public:
    AggBase() {}
    virtual ~AggBase() {}
private:
    AggBase(const AggBase &);
    AggBase& operator=(const AggBase &);
public:
    bool init(AggFuncManagerPtr &aggFuncManager,
              const std::vector<std::string> &groupKeyVec,
              const std::vector<std::string> &outputFields,
              const std::vector<AggFuncDesc> &aggFuncDesc);
    virtual bool compute(TablePtr &input) = 0;
    virtual bool finalize() {
        return true;
    }
    virtual TablePtr getTable() = 0;
    virtual void getStatistics(uint64_t &collectTime, uint64_t &outputAccTime,
                               uint64_t &mergeTime, uint64_t &outputResultTime,
                               uint64_t &aggPoolSize) const = 0;
    static bool calculateGroupKeyHash(TablePtr table,
            const std::vector<std::string> &groupKeyVec,
            std::vector<size_t> &hashValue);
protected:
    bool computeAggregator(TablePtr &input,
                           const std::vector<AggFuncDesc> &aggFuncDesc,
                           Aggregator &aggregator,
                           bool &aggregatorReady);
private:
    virtual bool doInit() {
        return true;
    }
protected:
    AggFuncManagerPtr _aggFuncManager;
    std::vector<std::string> _groupKeyVec;
    std::vector<std::string> _outputFields;
    std::vector<AggFuncDesc> _aggFuncDesc;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(AggBase);

END_HA3_NAMESPACE(sql);
