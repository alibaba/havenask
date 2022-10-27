#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/ops/join/JoinKernelBase.h>
#include <ha3/sql/ops/util/KernelRegister.h>
#include <ha3/sql/ops/util/KernelUtil.h>
#include <ha3/sql/ops/join/HashJoinConditionVisitor.h>
#include <ha3/sql/ops/condition/ConditionParser.h>
#include <ha3/sql/resource/SqlBizResource.h>
#include <ha3/sql/resource/SqlQueryResource.h>
#include <autil/TimeUtility.h>
#include <ha3/sql/ops/join/InnerJoin.h>
#include <ha3/sql/ops/join/LeftJoin.h>
#include <ha3/sql/ops/join/SemiJoin.h>
#include <ha3/sql/ops/join/AntiJoin.h>

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace autil;
using namespace kmonitor;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, JoinKernelBase);

const int JoinKernelBase::DEFAULT_BATCH_SIZE = 100000;

class JoinOpMetrics : public MetricsGroup
{
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_LATENCY_MUTABLE_METRIC(_totalEvaluateTime, "TotalEvaluateTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalJoinTime, "TotalJoinTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalHashTime, "TotalHashTime");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalJoinCount, "TotalJoinCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalRightHashCount, "TotalRightHashCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalLeftHashCount, "TotalLeftHashCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_hashMapSize, "HashMapSize");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalComputeTimes, "TotalComputeTimes");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags, JoinInfo *joinInfo) {
        REPORT_MUTABLE_METRIC(_totalEvaluateTime, joinInfo->totalevaluatetime() / 1000.0);
        REPORT_MUTABLE_METRIC(_totalJoinTime, joinInfo->totaljointime() / 1000.0);
        REPORT_MUTABLE_METRIC(_totalHashTime, joinInfo->totalhashtime() / 1000.0);
        REPORT_MUTABLE_METRIC(_totalJoinCount, joinInfo->totaljoincount());
        REPORT_MUTABLE_METRIC(_totalRightHashCount, joinInfo->totalrighthashcount());
        REPORT_MUTABLE_METRIC(_totalLeftHashCount, joinInfo->totallefthashcount());
        REPORT_MUTABLE_METRIC(_hashMapSize, joinInfo->hashmapsize());
        REPORT_MUTABLE_METRIC(_totalComputeTimes, joinInfo->totalcomputetimes());
    }
private:
    MutableMetric *_totalEvaluateTime = nullptr;
    MutableMetric *_totalJoinTime = nullptr;
    MutableMetric *_totalHashTime = nullptr;
    MutableMetric *_totalJoinCount = nullptr;
    MutableMetric *_totalRightHashCount = nullptr;
    MutableMetric *_totalLeftHashCount = nullptr;
    MutableMetric *_hashMapSize = nullptr;
    MutableMetric *_totalComputeTimes = nullptr;
};

JoinKernelBase::JoinKernelBase()
    : _pool(nullptr)
    , _memoryPoolResource(nullptr)
    , _isEquiJoin(true)
    , _leftIsBuild(false)
    , _shouldClearTable(false)
    , _systemFieldNum(0)
    , _batchSize(DEFAULT_BATCH_SIZE)
    , _queryMetricsReporter(nullptr)
    , _sqlSearchInfoCollector(nullptr)
    , _joinIndex(0)
    , _joinPtr({})
    , _opId(-1)
{
}

JoinKernelBase::~JoinKernelBase() {
    _sqlSearchInfoCollector = NULL;
}

const navi::KernelDef *JoinKernelBase::getDef() const {
    auto def = new navi::KernelDef();
    def->set_kernel_name("JoinKernelBase");
    KERNEL_REGISTER_ADD_INPUT(def, "input0");
    KERNEL_REGISTER_ADD_INPUT(def, "input1");
    KERNEL_REGISTER_ADD_OUTPUT(def, "output0");
    return def;
}

bool JoinKernelBase::config(navi::KernelConfigContext &ctx) {
    auto &json = ctx.getJsonAttrs();
    if (!KernelUtil::toJsonString(json, "condition", _conditionJson)) {
        return false;
    }
    if (!KernelUtil::toJsonString(json, "equi_condition", _equiConditionJson)) {
        return false;
    }
    if (!KernelUtil::toJsonString(json, "remaining_condition", _remainConditionJson)) {
        return false;
    }
    json.Jsonize("join_type", _joinType);
    json.Jsonize("semi_join_type", _semiJoinType, _semiJoinType);
    // for compatible
    if (_joinType == SQL_SEMI_JOIN_TYPE || _semiJoinType == SQL_SEMI_JOIN_TYPE) {
        _joinType = _semiJoinType = SQL_SEMI_JOIN_TYPE;
    }
    json.Jsonize("left_input_fields", _leftInputFields, _leftInputFields);
    json.Jsonize("right_input_fields", _rightInputFields, _rightInputFields);
    json.Jsonize("left_is_build", _leftIsBuild, _leftIsBuild);
    json.Jsonize("system_field_num", _systemFieldNum, _systemFieldNum);
    json.Jsonize("output_fields", _outputFields, _outputFields);
    json.Jsonize("output_fields_internal", _outputFieldsInternal, _outputFieldsInternal);
    json.Jsonize("is_equi_join", _isEquiJoin, _isEquiJoin);
    json.Jsonize("left_index_infos", _leftIndexInfos, _leftIndexInfos);
    json.Jsonize("right_index_infos", _rightIndexInfos, _rightIndexInfos);
    json.Jsonize(IQUAN_OP_ID, _opId, _opId);
    std::map<std::string, std::map<std::string, std::string> > hintsMap;
    json.Jsonize("hints", hintsMap, hintsMap);
    if (hintsMap.find(SQL_JOIN_HINT) != hintsMap.end()) {
        _hashHints = hintsMap[SQL_JOIN_HINT];
    }
    KernelUtil::stripName(_leftInputFields);
    KernelUtil::stripName(_rightInputFields);
    KernelUtil::stripName(_leftIndexInfos);
    KernelUtil::stripName(_rightIndexInfos);
    KernelUtil::stripName(_outputFields);
    KernelUtil::stripName(_outputFieldsInternal);

    if (!convertFields()) {
        SQL_LOG(ERROR, "convert fields failed.");
        return false;
    }
    json.Jsonize("batch_size", _batchSize, _batchSize);
    if (_batchSize == 0) {
        _batchSize = DEFAULT_BATCH_SIZE;
    }
    patchHintInfo(_hashHints);
    return true;
}

navi::ErrorCode JoinKernelBase::init(navi::KernelInitContext& context) {
    SqlQueryResource *queryResource = context.getResource<SqlQueryResource>("SqlQueryResource");
    KERNEL_REQUIRES(queryResource, "get sql query resource failed");
    SqlBizResource *bizResource = context.getResource<SqlBizResource>("SqlBizResource");
    KERNEL_REQUIRES(bizResource, "get sql biz resource failed");
    _pool = queryResource->getPool();
    KERNEL_REQUIRES(_pool, "get pool failed");
    _queryMetricsReporter = queryResource->getQueryMetricsReporter();
    _sqlSearchInfoCollector = queryResource->getSqlSearchInfoCollector();
    _memoryPoolResource = context.getResource<navi::MemoryPoolResource>(
            navi::RESOURCE_MEMORY_POOL_URI);
    KERNEL_REQUIRES(_memoryPoolResource, "get mem pool resource failed");

    ConditionParser parser(_pool);
    ConditionPtr condition;
    if (!_remainConditionJson.empty()) {
        if (!parser.parseCondition(_equiConditionJson, condition)) {
            SQL_LOG(ERROR, "parse condition [%s] failed", _equiConditionJson.c_str());
            return navi::EC_ABORT;
        }
        _calcTable.reset(new CalcTable(_pool, _memoryPoolResource, {}, {},
                        queryResource->getTracer(),
                        queryResource->getSuezCavaAllocator(),
                        bizResource->getCavaPluginManager(),
                        bizResource->getFunctionInterfaceCreator().get()));
        if (!_calcTable->init("", _remainConditionJson)) {
            return navi::EC_ABORT;
        }
    } else {
        if (!parser.parseCondition(_conditionJson, condition)) {
            SQL_LOG(ERROR, "parse condition [%s] failed", _conditionJson.c_str());
            return navi::EC_ABORT;
        }
    }
    HashJoinConditionVisitor visitor(_output2InputMap);
    if (condition) {
        condition->accept(&visitor);
    }
    if (visitor.isError()) {
        SQL_LOG(ERROR, "visit condition failed, error info [%s]",
                        visitor.errorInfo().c_str());
        return navi::EC_ABORT;
    }
    _leftJoinColumns = visitor.getLeftJoinColumns();
    _rightJoinColumns = visitor.getRightJoinColumns();
    if (_leftJoinColumns.size() != _rightJoinColumns.size()) {
        SQL_LOG(ERROR, "left join column size [%zu] not match right one [%zu]",
                        _leftJoinColumns.size(), _rightJoinColumns.size());
        return navi::EC_ABORT;
    }
    _joinInfo.set_kernelname(getKernelName());
    _joinInfo.set_nodename(getNodeName());
    string keyStr = getKernelName() + "__" + _conditionJson + "__"+
                    StringUtil::toString(_outputFields);
    _joinInfo.set_hashkey(autil::HashAlgorithm::hashString(keyStr.c_str(), keyStr.size(), 1));
    JoinBaseParam joinBaseParam(_leftInputFields, _rightInputFields, _outputFieldsInternal,
                                &_joinInfo, _memoryPoolResource->getPool(),
                                _pool, _calcTable);
    if (_joinType == SQL_INNER_JOIN_TYPE) {
        _joinPtr.reset(new InnerJoin(joinBaseParam));
    } else if (_joinType == SQL_LEFT_JOIN_TYPE) {
        _joinPtr.reset(new LeftJoin(joinBaseParam));
        _joinPtr->setDefaultValue(_defaultValue);
    } else if (_joinType == SQL_SEMI_JOIN_TYPE) {
        _joinPtr.reset(new SemiJoin(joinBaseParam));
    } else if (_joinType == SQL_ANTI_JOIN_TYPE) {
        _joinPtr.reset(new AntiJoin(joinBaseParam));
    } else {
        SQL_LOG(ERROR, "join type [%s] is not supported", _joinType.c_str());
        return navi::EC_ABORT;
    }
    if (_opId < 0) {
        string keyStr = getKernelName() + "__" + _conditionJson + "__"+
                        StringUtil::toString(_outputFields);
        _joinInfo.set_hashkey(autil::HashAlgorithm::hashString(keyStr.c_str(), keyStr.size(), 1));
    } else {
        _joinInfo.set_hashkey(_opId);
    }
    return navi::EC_NONE;
}

navi::ErrorCode JoinKernelBase::compute(navi::KernelComputeContext &runContext) {
    return navi::EC_ABORT;
}

bool JoinKernelBase::convertFields() {
    if (_systemFieldNum != 0) {
        return false;
    }
    if (_outputFieldsInternal.empty()) {
        _outputFieldsInternal = _outputFields;
    }
    if (_semiJoinType == SQL_SEMI_JOIN_TYPE || _joinType == SQL_ANTI_JOIN_TYPE) {
        if (_outputFields.size() != _leftInputFields.size()) {
            SQL_LOG(ERROR, "semi join or anti join output fields [%ld] not equal left input fields [%ld]",
                    _outputFields.size(), _leftInputFields.size());
            return false;
        }
    } else {
        if (_outputFields.size() != _leftInputFields.size() + _rightInputFields.size()) {
            return false;
        }
    }

    if (_outputFieldsInternal.size() != _leftInputFields.size() + _rightInputFields.size()) {
        return false;
    }

    for (size_t i = 0; i < _outputFieldsInternal.size(); i++) {
        if (i < _leftInputFields.size()) {
            _output2InputMap[_outputFieldsInternal[i]] = make_pair(_leftInputFields[i], true);
        } else {
            size_t offset = i - _leftInputFields.size();
            _output2InputMap[_outputFieldsInternal[i]] = make_pair(_rightInputFields[offset], false);
        }
    }
    return true;
}

bool JoinKernelBase::createHashMap(const TablePtr &table,size_t offset,
                                   size_t count,  bool hashLeftTable)
{
    _hashJoinMap.clear();
    uint64_t beginHash = TimeUtility::currentTime();
    const auto &joinColumns = hashLeftTable ? _leftJoinColumns : _rightJoinColumns;
    HashValues values;
    if (!getHashValues(table, offset, count, joinColumns, values)) {
        return false;
    }
    if (hashLeftTable) {
        JoinInfoCollector::incLeftHashCount(&_joinInfo, values.size());
    } else {
        JoinInfoCollector::incRightHashCount(&_joinInfo, values.size());
    }
    uint64_t afterHash = TimeUtility::currentTime();
    JoinInfoCollector::incHashTime(&_joinInfo, afterHash - beginHash);
    for (const auto &valuePair : values) {
        const auto &hashKey = valuePair.second;
        const auto &hashValue = valuePair.first;
        auto iter = _hashJoinMap.find(hashKey);
        if (iter == _hashJoinMap.end()) {
            vector<size_t> hashValues = { hashValue };
            _hashJoinMap.insert(make_pair(hashKey, hashValues));
        } else {
            iter->second.push_back(hashValue);
        }
    }
    JoinInfoCollector::incHashMapSize(&_joinInfo, _hashJoinMap.size());
    uint64_t endHash = TimeUtility::currentTime();
    JoinInfoCollector::incCreateTime(&_joinInfo, endHash - afterHash);
    return true;
}

bool JoinKernelBase::getHashValues(const TablePtr &table, size_t offset, size_t count,
                                   const vector<string> &joinColumns,
                                   HashValues &values)
{
    if (!getColumnHashValues(table, offset, count, joinColumns[0], values)) {
        return false;
    }
    for (size_t i = 1; i < joinColumns.size(); ++i) {
        HashValues tmpValues;
        if (!getColumnHashValues(table, offset, count,  joinColumns[i], tmpValues)) {
            return false;
        }
        combineHashValues(tmpValues, values);
    }
    return true;
}

bool JoinKernelBase::getColumnHashValues(const TablePtr &table, size_t offset,
        size_t count, const string &columnName, HashValues &values)
{
    ColumnPtr joinColumn = table->getColumn(columnName);
    if (joinColumn == nullptr) {
        SQL_LOG(ERROR, "invalid join column name [%s]", columnName.c_str());
        return false;
    }
    auto schema = joinColumn->getColumnSchema();
    if (schema == nullptr) {
        SQL_LOG(ERROR, "get join column [%s] schema failed", columnName.c_str());
        return false;
    }
    size_t rowCount = min(offset + count, table->getRowCount());
    auto vt = schema->getType();
    bool isMulti = vt.isMultiValue();
    switch (vt.getBuiltinType()) {
#define CASE_MACRO(ft)                                                  \
        case ft: {                                                      \
            if (isMulti) {                                              \
                typedef MatchDocBuiltinType2CppType<ft, true>::CppType T; \
                auto columnData = joinColumn->getColumnData<T>();       \
                if (unlikely(!columnData)) {                            \
                    SQL_LOG(ERROR, "impossible cast column data failed"); \
                    return false;                                       \
                }                                                       \
                values.reserve(rowCount * 4);                           \
                size_t dataSize = 0;                                    \
                for (size_t i = offset; i < rowCount; i++) {            \
                    const auto &datas = columnData->get(i);             \
                    dataSize = datas.size();                            \
                    for (size_t k = 0; k < dataSize; k++) {             \
                        auto hashKey = TableUtil::calculateHashValue(datas[k]); \
                        values.emplace_back(i, hashKey);                \
                    }                                                   \
                }                                                       \
            } else {                                                    \
                typedef MatchDocBuiltinType2CppType<ft, false>::CppType T; \
                auto columnData = joinColumn->getColumnData<T>();       \
                if (unlikely(!columnData)) {                            \
                    SQL_LOG(ERROR, "impossible cast column data failed"); \
                    return false;                                       \
                }                                                       \
                values.reserve(rowCount);                               \
                for (size_t i = offset; i < rowCount; i++) {                 \
                    const auto &data = columnData->get(i);              \
                    auto hashKey = TableUtil::calculateHashValue(data); \
                    values.emplace_back(i, hashKey);                    \
                }                                                       \
            }                                                           \
            break;                                                      \
        }
        BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
        SQL_LOG(ERROR, "impossible reach this branch");
        return false;
    }
    }
    if (values.empty()) {
        _shouldClearTable = true; // for multi-type datas may be empty
        SQL_LOG(WARN, "table size[%zu], hash values size[%zu], should clear",
                        table->getRowCount(), values.size());
    }
    return true;
}

void JoinKernelBase::combineHashValues(const HashValues &valuesA, HashValues &valuesB) {
    HashValues tmpValues;
    size_t beginA = 0;
    size_t beginB = 0;
    while (beginA < valuesA.size() && beginB < valuesB.size()) {
        size_t rowIndexA = valuesA[beginA].first;
        size_t rowIndexB = valuesB[beginB].first;
        if (rowIndexA < rowIndexB) {
            ++beginA;
        } else if (rowIndexA == rowIndexB) {
            size_t offsetB = beginB;
            for (; offsetB < valuesB.size() && rowIndexA == valuesB[offsetB].first; offsetB++) {
                size_t seed = valuesA[beginA].second;
                TableUtil::hash_combine(seed, valuesB[offsetB].second);
                tmpValues.emplace_back(rowIndexA, seed);
            }
            ++beginA;
        } else {
            ++beginB;
        }
    }
    valuesB = std::move(tmpValues);
}

void JoinKernelBase::reserveJoinRow(size_t rowCount) {
    _tableAIndexes.reserve(rowCount);
    _tableBIndexes.reserve(rowCount);
}

void JoinKernelBase::patchHintInfo(const map<string, string> &hintsMap) {
    if (hintsMap.empty()) {
        return;
    }
    auto iter = hintsMap.find("batchSize");
    if (iter != hintsMap.end()) {
        uint32_t batchSize = 0;
        StringUtil::fromString(iter->second, batchSize);
        if (batchSize > 0) {
            _batchSize = batchSize;
        }
    }
    iter = hintsMap.find("defaultValue");
    if (iter != hintsMap.end()) {
        vector<string> values;
        StringUtil::fromString(iter->second, values, ",");
        for (auto &value : values) {
            auto pair = StringUtil::split(value, ":");
            StringUtil::toUpperCase(pair[0]);
            _defaultValue[pair[0]] = pair[1];
            SQL_LOG(DEBUG, "join use default value [%s] : [%s]",
                    pair[0].c_str(), pair[1].c_str());
        }
    }
}


void JoinKernelBase::reportMetrics() {
    if (_queryMetricsReporter != nullptr) {
        string pathName = "sql.user.ops." + getKernelName();
        auto opMetricsReporter =
            _queryMetricsReporter->getSubReporter(pathName, {});
        opMetricsReporter->report<JoinOpMetrics, JoinInfo>(nullptr, &_joinInfo);
    }
}

END_HA3_NAMESPACE(sql);
