#include <ha3/sql/ops/agg/Aggregator.h>
#include <autil/StringUtil.h>
#include <autil/TimeUtility.h>

using namespace std;
using namespace autil;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, Aggregator);

static constexpr size_t kCheckIntervalMask = (1 << 7) - 1;
#define UPDATE_AND_CHECK_AGG_POOL()                                     \
    _aggPoolSize = _aggregatorPoolPtr->getAllocatedSize();              \
    if (unlikely(_aggPoolSize > _aggHints.memoryLimit)) {               \
        SQL_LOG(ERROR, "pool used too many bytes, limit=[%lu], actual=[%lu]", \
                _aggHints.memoryLimit, _aggregatorPoolPtr->getAllocatedSize()); \
        return false;                                                   \
    }

Aggregator::Aggregator(bool isLocal,
                       autil::mem_pool::Pool *pool,
                       navi::MemoryPoolResource *memoryPoolResource,
                       AggHints aggHints)
    : _isLocal(isLocal)
    , _pool(pool)
    , _memoryPoolResource(memoryPoolResource)
    , _aggHints(aggHints)
    , _aggregateTime(0)
    , _getTableTime(0)
    , _aggPoolSize(0)
    , _aggregatorPoolPtr(_memoryPoolResource->getPool())
    , _aggregateCnt(0)
    , _accumulatorVec(_aggregatorPoolPtr.get())
{
}

Aggregator::~Aggregator() {
    reset();
}

void Aggregator::reset() {
    for (auto aggFunc : _aggFuncVec) {
        DELETE_AND_SET_NULL(aggFunc);
    }
    _aggFuncVec.clear();
    _aggFilterColumn.clear();
    for (auto *acc : _accumulatorVec) {
        POOL_DELETE_CLASS(acc);
    }
    _accumulators.clear();
    _accumulatorVec.clear();
    _table.reset();
}

bool Aggregator::init(const AggFuncManagerPtr &aggFuncManager,
                      const vector<AggFuncDesc> &aggFuncDesc,
                      const vector<string> &groupKey,
                      const vector<string> &outputFields,
                      const TablePtr &table)
{
    for (const auto &iter : aggFuncDesc) {
        if (!createAggFunc(aggFuncManager, table, iter.funcName,
                           iter.inputs, iter.outputs, iter.filterArg))
        {
            SQL_LOG(ERROR, "create agg function [%s] failed", iter.funcName.c_str());
            return false;
        }
    }
    for (const string &key : groupKey) {
        if (find(outputFields.begin(), outputFields.end(), key) != outputFields.end()) {
            if (!createAggFunc(aggFuncManager, table, "IDENTITY", {key}, {key})) {
                SQL_LOG(ERROR, "create identity function [%s] failed", key.c_str());
                return false;
            }
        }
    }
    _table.reset(new Table(_memoryPoolResource->getPool()));
    for (auto func : _aggFuncVec) {
        if (!func->initOutput(_table)) {
            SQL_LOG(ERROR, "create agg output [%s] failed", func->getName().c_str());
            return false;
        }
    }
    _table->endGroup();
    return true;
}

bool Aggregator::createAggFunc(const AggFuncManagerPtr &aggFuncManager,
                               const TablePtr &table,
                               const std::string &funcName,
                               const vector<std::string> &inputs,
                               const vector<std::string> &outputs,
                               const int32_t filterArg)
{
    vector<ValueType> types;
    for (const string &param : inputs) {
        auto column = table->getColumn(param);
        if (column == nullptr) {
            SQL_LOG(ERROR, "column [%s] does not exist", param.c_str());
            return false;
        }
        auto schema = column->getColumnSchema();
        if (schema == nullptr) {
            SQL_LOG(ERROR, "column schema [%s] does not exist", param.c_str());
            return false;
        }
        types.push_back(schema->getType());
    }
    auto func = aggFuncManager->createAggFunction(funcName, types, inputs, outputs, _isLocal);
    if (func == nullptr) {
        SQL_LOG(ERROR, "create agg function [%s] failed", funcName.c_str());
        return false;
    }
    func->setPool(_pool, _aggregatorPoolPtr.get());
    _aggFuncVec.emplace_back(func);
    if (filterArg >= 0 && _isLocal) {
        auto column = table->getColumn(filterArg)->getColumnData<bool>();
        if (column == nullptr) {
            SQL_LOG(ERROR, "column [%d] is not bool type", filterArg);
            return false;
        }
        _aggFilterColumn.emplace_back(column);
    } else {
        _aggFilterColumn.emplace_back(nullptr);
    }
    return true;
}

bool Aggregator::aggregate(const TablePtr &table, const vector<size_t> &groupKeys) {
    uint64_t beginTime = TimeUtility::currentTime();

    for (auto func : _aggFuncVec) {
        if (!func->initInput(table)) {
            SQL_LOG(ERROR, "init input [%s] failed", func->getName().c_str());
            return false;
        }
    }
    size_t rowCount = table->getRowCount();
    for (size_t i = 0; i < rowCount; i++) {
        if (!doAggregate(table->getRow(i), groupKeys[i])) {
            return false;
        }
    }
    UPDATE_AND_CHECK_AGG_POOL();

    _aggregateTime += TimeUtility::currentTime() - beginTime;
    return true;
}

bool Aggregator::doAggregate(const Row &row, size_t groupKey) {
    auto it = _accumulators.find(groupKey);
    if (it == _accumulators.end()) {
        if (_accumulators.size() >= _aggHints.groupKeyLimit ) {
            if (_aggHints.stopExceedLimit) {
                SQL_LOG(ERROR, "group key size large than limit[%lu]", _aggHints.groupKeyLimit);
                return false;
            } else {
                return true;
            }
        }
        size_t startIdx = _accumulatorVec.size();
        for (size_t i = 0; i < _aggFuncVec.size(); i++) {
            // IMPORTANT: use independent pool for each thread
            auto acc = _aggFuncVec[i]->createAccumulator(_aggregatorPoolPtr.get());
            if (acc == nullptr) {
                SQL_LOG(ERROR, "create accumulator failed");
                return false;
            }
            _accumulatorVec.push_back(acc);
        }
        _accumulators.insert({groupKey, startIdx});
        it = _accumulators.find(groupKey);
    }
    size_t accStartIdx = it->second;
    for (size_t i = 0; i < _aggFuncVec.size(); i++) {
        if (_aggFilterColumn[i] != nullptr && _aggFilterColumn[i]->get(row) == false) {
            continue;
        }
        if (!_aggFuncVec[i]->aggregate(row, _accumulatorVec[accStartIdx + i])) {
            return false;
        }
    }
    if ((++_aggregateCnt & kCheckIntervalMask) == 0) {
        UPDATE_AND_CHECK_AGG_POOL();
    }
    return true;
}

TablePtr Aggregator::getTable() {
    if (_table == nullptr) {
        return _table;
    }
    uint64_t beginTime = TimeUtility::currentTime();

    _table->batchAllocateRow(_accumulators.size());
    size_t rowIndex = 0;
    for (auto &pair : _accumulators) {
        Row row = _table->getRow(rowIndex++);
        for (size_t i = 0; i < _aggFuncVec.size(); i++) {
            auto *acc = _accumulatorVec[pair.second + i];
            if (!_aggFuncVec[i]->setResult(acc, row)) {
                SQL_LOG(ERROR, "set agg result [%s] failed", _aggFuncVec[i]->getName().c_str());
                return nullptr;
            }
        }
    }

    _getTableTime += TimeUtility::currentTime() - beginTime;
    _aggPoolSize = _aggregatorPoolPtr->getAllocatedSize();
    return _table;
}

void Aggregator::getStatistics(uint64_t &aggregateTime,
                               uint64_t &getTableTime,
                               uint64_t &aggPoolSize) const
{
    aggregateTime = _aggregateTime;
    getTableTime = _getTableTime;
    aggPoolSize = _aggPoolSize;
}


END_HA3_NAMESPACE(sql);
