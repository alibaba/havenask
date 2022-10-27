#include <ha3/sql/ops/agg/AggNormal.h>

using namespace std;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, AggNormal);

AggNormal::AggNormal(autil::mem_pool::Pool *pool,
                     navi::MemoryPoolResource *memoryPoolResource,
                     AggHints aggHints)
    : _localAggregatorReady(false)
    , _globalAggregatorReady(false)
    , _localAggregator(true, pool, memoryPoolResource, aggHints)
    , _globalAggregator(false, pool, memoryPoolResource, aggHints)
{}

bool AggNormal::doInit() {
    // generate Accumulator field names
    _localAggFuncDesc = _aggFuncDesc;
    _globalAggFuncDesc = _aggFuncDesc;
    if (!translateAggFuncDesc(_aggFuncManager, true, _localAggFuncDesc)) {
        SQL_LOG(ERROR, "translate local AggFuncDesc failed");
        return false;
    }
    if (!translateAggFuncDesc(_aggFuncManager, false, _globalAggFuncDesc)) {
        SQL_LOG(ERROR, "translate global AggFuncDesc failed");
        return false;
    }
    return true;
}

bool AggNormal::translateAggFuncDesc(
        AggFuncManagerPtr &aggFuncManager,
        bool isLocal,
        std::vector<AggFuncDesc> &aggFuncDesc)
{
    for (size_t i = 0; i < aggFuncDesc.size(); ++i) {
        auto &desc = aggFuncDesc[i];
        size_t fieldCount = 0;
        if (!aggFuncManager->getAccSize(desc.funcName, fieldCount)) {
            SQL_LOG(ERROR, "get [%s] accumulator size failed",
                    desc.funcName.c_str());
            return false;
        }
        genAggFuncDescFieldNames(i, desc.funcName, fieldCount,
                isLocal ? desc.outputs : desc.inputs);
    }
    return true;
}

void AggNormal::genAggFuncDescFieldNames(size_t funcIdx,
        std::string funcName,
        size_t fieldCount,
        std::vector<std::string> &result)
{
    std::ostringstream oss;
    oss << "__" << funcIdx << "_" << funcName;
    std::string prefix = oss.str();
    result.clear();
    for (size_t i = 0; i < fieldCount; ++i) {
        std::ostringstream oss;
        oss << prefix << "_idx_" << i;
        result.push_back(oss.str());
    }
}

bool AggNormal::compute(TablePtr &input) {
    if (!computeAggregator(input, _localAggFuncDesc,
                           _localAggregator, _localAggregatorReady))
    {
        SQL_LOG(ERROR, "compute local aggregator failed");
        return false;
    }
    return true;
}

bool AggNormal::finalize() {
    TablePtr intermediateTable = _localAggregator.getTable();
    if (intermediateTable == nullptr) {
        SQL_LOG(ERROR, "get intermediateTable failed");
        return false;
    }
    if (!computeAggregator(intermediateTable, _globalAggFuncDesc,
                           _globalAggregator, _globalAggregatorReady))
    {
        SQL_LOG(ERROR, "compute global aggregator failed");
        return false;
    }
    return true;
}


TablePtr AggNormal::getTable() {
    return _globalAggregator.getTable();
}

void AggNormal::getStatistics(uint64_t &collectTime, uint64_t &outputAccTime,
                              uint64_t &mergeTime, uint64_t &outputResultTime,
                              uint64_t &aggPoolSize) const
{
    uint64_t localAggPoolSize, globalAggPoolSize;
    _localAggregator.getStatistics(collectTime, outputAccTime, localAggPoolSize);
    _globalAggregator.getStatistics(mergeTime, outputResultTime, globalAggPoolSize);
    aggPoolSize = localAggPoolSize + globalAggPoolSize;
}


END_HA3_NAMESPACE(sql);
