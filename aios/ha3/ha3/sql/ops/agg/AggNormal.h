#pragma once

#include <ha3/sql/ops/agg/AggBase.h>
#include <navi/resource/MemoryPoolResource.h>

BEGIN_HA3_NAMESPACE(sql);

class AggNormal: public AggBase
{
public:
    AggNormal(autil::mem_pool::Pool *pool,
              navi::MemoryPoolResource *memoryPoolResource,
              AggHints aggHints = AggHints());
private:
    AggNormal(const AggNormal &);
    AggNormal& operator=(const AggNormal &);
public:
    bool compute(TablePtr &input) override;
    bool finalize() override;
    TablePtr getTable() override;
    void getStatistics(uint64_t &collectTime, uint64_t &outputAccTime,
                       uint64_t &mergeTime, uint64_t &outputResultTime,
                       uint64_t &aggPoolSize) const override;

private:
    static bool translateAggFuncDesc(
            AggFuncManagerPtr &aggFuncManager,
            bool isLocal,
            std::vector<AggFuncDesc> &aggFuncDesc);
    static void genAggFuncDescFieldNames(size_t funcIdx,
            std::string funcName,
            size_t fieldCount,
            std::vector<std::string> &result);

    bool doInit() override;
private:
    bool _localAggregatorReady;
    bool _globalAggregatorReady;
    Aggregator _localAggregator;
    Aggregator _globalAggregator;
    std::vector<AggFuncDesc> _localAggFuncDesc;
    std::vector<AggFuncDesc> _globalAggFuncDesc;
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(AggNormal);

END_HA3_NAMESPACE(sql);
