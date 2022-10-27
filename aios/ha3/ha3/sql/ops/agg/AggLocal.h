#pragma once

#include <ha3/sql/ops/agg/AggBase.h>
#include <navi/resource/MemoryPoolResource.h>

BEGIN_HA3_NAMESPACE(sql);

class AggLocal: public AggBase
{
public:
    AggLocal(autil::mem_pool::Pool *pool,
             navi::MemoryPoolResource *memoryPoolResource,
             AggHints aggHints = AggHints())
        : _aggregatorReady(false)
        , _localAggregator(true, pool, memoryPoolResource, aggHints)
    {}
private:
    AggLocal(const AggLocal &);
    AggLocal& operator=(const AggLocal &);
public:
    bool compute(TablePtr &input) override {
        return computeAggregator(input, _aggFuncDesc,
                _localAggregator, _aggregatorReady);
    }
    TablePtr getTable() override {
        return _localAggregator.getTable();
    }

    void getStatistics(uint64_t &collectTime, uint64_t &outputAccTime,
                       uint64_t &mergeTime, uint64_t &outputResultTime,
                       uint64_t &aggPoolSize) const override
    {
        _localAggregator.getStatistics(collectTime, outputAccTime, aggPoolSize);
        mergeTime = 0;
        outputResultTime = 0;
    }
private:
    bool _aggregatorReady;
    Aggregator _localAggregator;
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(AggLocal);

END_HA3_NAMESPACE(sql);
