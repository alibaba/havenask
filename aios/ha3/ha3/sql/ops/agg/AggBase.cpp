#include <ha3/sql/ops/agg/AggBase.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/util/ValueTypeSwitch.h>

using namespace std;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, AggBase);


bool AggBase::init(AggFuncManagerPtr &aggFuncManager,
                   const std::vector<std::string> &groupKeyVec,
                   const std::vector<std::string> &outputFields,
                   const std::vector<AggFuncDesc> &aggFuncDesc)
{
    _aggFuncManager = aggFuncManager;
    _groupKeyVec = groupKeyVec;
    _outputFields = outputFields;
    _aggFuncDesc = aggFuncDesc;
    return doInit();
}

bool AggBase::computeAggregator(TablePtr &input,
                                const std::vector<AggFuncDesc> &aggFuncDesc,
                                Aggregator &aggregator,
                                bool &aggregatorReady)
{
    if (!aggregatorReady) {
        if (!aggregator.init(_aggFuncManager, aggFuncDesc,
                             _groupKeyVec, _outputFields, input))
        {
            SQL_LOG(ERROR, "aggregator init failed");
            return false;
        }
        aggregatorReady = true;
    }
    vector<size_t> groupKeys;
    if (!_groupKeyVec.empty()) {
        if (!calculateGroupKeyHash(input, _groupKeyVec,  groupKeys)) {
            SQL_LOG(ERROR, "calculate group key hash failed");
            return false;
        }
    } else {
        groupKeys.resize(input->getRowCount(), 0);
    }
    if (!aggregator.aggregate(input, groupKeys)) {
        SQL_LOG(ERROR, "aggregate failed");
        return false;
    }
    return true;
}

bool AggBase::calculateGroupKeyHash(TablePtr table, const vector<string> &groupKeyVec,
                                    vector<size_t> &hashValue)
{
    size_t rowCount = table->getRowCount();
    hashValue.resize(rowCount, 0);
    for (size_t keyIdx = 0; keyIdx < groupKeyVec.size(); ++keyIdx) {
        const string &key = groupKeyVec[keyIdx];
        ColumnPtr column = table->getColumn(key);
        if (column == nullptr) {
            SQL_LOG(ERROR, "invalid column name [%s]", key.c_str());
            return false;
        }
        auto schema = column->getColumnSchema();
        if (schema == nullptr) {
            SQL_LOG(ERROR, "invalid column schema [%s]", key.c_str());
            return false;
        }
        auto vt = schema->getType();
        auto func = [&](auto a) {
            typedef typename decltype(a)::value_type T;
            ColumnData<T> *columnData = column->getColumnData<T>();
            if (unlikely(!columnData)) {
                SQL_LOG(ERROR, "impossible cast column data failed");
                return false;
            }
            if (keyIdx == 0) {
                for (size_t i = 0; i < rowCount; i++) {
                    const auto &data = columnData->get(i);
                    hashValue[i] = TableUtil::calculateHashValue<T>(data);
                }
            } else {
                for (size_t i = 0; i < rowCount; i++) {
                    const auto &data = columnData->get(i);
                    TableUtil::hash_combine(hashValue[i],
                            TableUtil::calculateHashValue<T>(data));
                }
            }
            return true;
        };
        if (!ValueTypeSwitch::switchType(vt, func, func)) {
            SQL_LOG(ERROR, "calculate hash for group key[%s] failed",
                    key.c_str());
            return false;
        }
    }
    return true;
}


END_HA3_NAMESPACE(sql);
