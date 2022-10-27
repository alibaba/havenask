#pragma once

#include <unordered_map>
#include <ha3/sql/common/common.h>
#include <ha3/sql/data/Table.h>
#include <ha3/sql/ops/agg/AggFuncDesc.h>
#include <ha3/sql/ops/agg/AggFuncManager.h>
#include <navi/resource/MemoryPoolResource.h>
#include <autil/mem_pool/PoolVector.h>


BEGIN_HA3_NAMESPACE(sql);
struct AggHints {
    AggHints()
        : memoryLimit(DEFAULT_AGG_MEMORY_LIMIT)
        , groupKeyLimit(DEFAULT_GROUP_KEY_COUNT)
        , stopExceedLimit(true)
    {}
    size_t memoryLimit;
    size_t groupKeyLimit;
    bool stopExceedLimit;
};

class Aggregator
{
public:
    Aggregator(bool isLocal, autil::mem_pool::Pool *pool,
               navi::MemoryPoolResource *memoryPoolResource,
               AggHints aggHints = AggHints());
    ~Aggregator();
private:
    Aggregator(const Aggregator &);
    Aggregator& operator=(const Aggregator &);
public:
    bool init(const AggFuncManagerPtr &aggFuncManager,
              const std::vector<AggFuncDesc> &aggFuncDesc,
              const std::vector<std::string> &groupKey,
              const std::vector<std::string> &outputFields,
              const TablePtr &table);
    bool aggregate(const TablePtr &table, const std::vector<size_t> &groupKeys);
    TablePtr getTable();
    void reset();
    void getStatistics(uint64_t &aggregateTime,
                       uint64_t &getTableTime,
                       uint64_t &aggPoolSize) const;
private:
    bool createAggFunc(const AggFuncManagerPtr &aggFuncManager,
                       const TablePtr &table,
                       const std::string &funcName,
                       const std::vector<std::string> &inputs,
                       const std::vector<std::string> &outputs,
                       const int32_t filterArg = -1);
    bool doAggregate(const Row &row, size_t groupKey);
private:
    TablePtr _table;
    bool _isLocal;
    autil::mem_pool::Pool *_pool;
    navi::MemoryPoolResource *_memoryPoolResource;
    AggHints _aggHints;
    uint64_t _aggregateTime;
    uint64_t _getTableTime;
    uint64_t _aggPoolSize;

    std::vector<AggFunc *> _aggFuncVec;
    std::vector<ColumnData<bool> *> _aggFilterColumn;
    std::shared_ptr<autil::mem_pool::Pool> _aggregatorPoolPtr;
    size_t _aggregateCnt;
    autil::mem_pool::PoolVector<Accumulator *> _accumulatorVec;
    std::unordered_map<size_t, size_t> _accumulators;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(Aggregator);

END_HA3_NAMESPACE(sql);
