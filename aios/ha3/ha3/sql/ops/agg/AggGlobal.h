#pragma once

#include <ha3/sql/ops/agg/AggBase.h>
#include <navi/resource/MemoryPoolResource.h>

BEGIN_HA3_NAMESPACE(sql);

class AggGlobal: public AggBase
{
public:
    AggGlobal(autil::mem_pool::Pool *pool,
              navi::MemoryPoolResource *memoryPoolResource,
             AggHints aggHints = AggHints())
        : _aggregatorReady(false)
        , _globalAggregator(false, pool, memoryPoolResource, aggHints)
    {}
private:
    AggGlobal(const AggGlobal &);
    AggGlobal& operator=(const AggGlobal &);
public:
    bool compute(TablePtr &input) override {
        return computeAggregator(input, _aggFuncDesc,
                _globalAggregator, _aggregatorReady);
    }
    TablePtr getTable() override{
        return _globalAggregator.getTable();
    }
    void getStatistics(uint64_t &collectTime, uint64_t &outputAccTime,
                       uint64_t &mergeTime, uint64_t &outputResultTime,
                       uint64_t &aggPoolSize) const override
    {
        collectTime = 0;
        outputAccTime = 0;
        _globalAggregator.getStatistics(mergeTime, outputResultTime, aggPoolSize);
    }
private:
    bool _aggregatorReady;
    Aggregator _globalAggregator;
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(AggGlobal);

END_HA3_NAMESPACE(sql);
