#include <autil/legacy/any.h>
#include <autil/StringUtil.h>
#include <autil/TimeUtility.h>
#include <suez/turing/expression/framework/ExpressionEvaluator.h>
#include <ha3/search/IndexPartitionReaderUtil.h>
#include <ha3/turing/ops/SqlSessionResource.h>
#include <ha3/sql/ops/scan/ScanBase.h>
#include <ha3/sql/ops/scan/ScanIterator.h>
#include <ha3/sql/ops/scan/Collector.h>
#include <ha3/sql/ops/util/KernelRegister.h>
#include <ha3/sql/ops/util/KernelUtil.h>
#include <ha3/sql/resource/SqlBizResource.h>
#include <ha3/sql/resource/SqlQueryResource.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/ops/scan/Ha3ScanConditionVisitor.h>
#include <ha3/sql/ops/util/SqlJsonUtil.h>
#include <suez/turing/expression/syntax/SyntaxParser.h>
#include <suez/turing/expression/util/TypeTransformer.h>

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace suez::turing;
using namespace autil_rapidjson;
using namespace autil;
using namespace kmonitor;

USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(sql);

HA3_LOG_SETUP(sql, ScanBase);
HA3_LOG_SETUP(sql, ScanInitParam);
class ScanOpMetrics : public MetricsGroup
{
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_GAUGE_MUTABLE_METRIC(_totalComputeCount, "TotalComputeCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalOutputCount, "TotalOutputCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalScanCount, "TotalScanCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalSeekCount, "TotalSeekCount");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalSeekTime, "TotalSeekTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalEvaluateTime, "TotalEvaluateTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalOutputTime, "TotalOutputTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalComputeTime, "TotalComputeTime");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags, ScanInfo *scanInfo) {
        REPORT_MUTABLE_METRIC(_totalComputeCount, scanInfo->totalcomputetimes());
        REPORT_MUTABLE_METRIC(_totalOutputCount, scanInfo->totaloutputcount());
        REPORT_MUTABLE_METRIC(_totalScanCount, scanInfo->totalscancount());
        REPORT_MUTABLE_METRIC(_totalSeekCount, scanInfo->totalseekcount());
        REPORT_MUTABLE_METRIC(_totalSeekTime, scanInfo->totalseektime() / 1000);
        REPORT_MUTABLE_METRIC(_totalEvaluateTime, scanInfo->totalevaluatetime() / 1000);
        REPORT_MUTABLE_METRIC(_totalOutputTime, scanInfo->totaloutputtime() / 1000);
        REPORT_MUTABLE_METRIC(_totalComputeTime, scanInfo->totalusetime() / 1000);
    }
private:
    MutableMetric *_totalComputeCount = nullptr;
    MutableMetric *_totalOutputCount = nullptr;
    MutableMetric *_totalScanCount = nullptr;
    MutableMetric *_totalSeekCount = nullptr;
    MutableMetric *_totalSeekTime = nullptr;
    MutableMetric *_totalEvaluateTime = nullptr;
    MutableMetric *_totalOutputTime = nullptr;
    MutableMetric *_totalComputeTime = nullptr;
};

bool ScanInitParam::initFromJson(autil::legacy::Jsonizable::JsonWrapper &wrapper) {
    try {
        wrapper.Jsonize("parallel_num", parallelNum, parallelNum);
        wrapper.Jsonize("parallel_index", parallelIndex, parallelIndex);
        if (0 == parallelNum || parallelIndex >= parallelNum) {
            SQL_LOG(ERROR, "illegal parallel param:parallelNum[%u],parallelIndex[%u]", parallelNum, parallelIndex);
            return false;
        }
        wrapper.Jsonize("table_name", tableName);
        wrapper.Jsonize("output_fields", outputFields);
        wrapper.Jsonize("table_type", tableType);
        wrapper.Jsonize("db_name", dbName, dbName);
        wrapper.Jsonize("catalog_name", catalogName, catalogName);
        wrapper.Jsonize("output_fields_type", outputFieldsType, outputFieldsType);
        wrapper.Jsonize("index_infos", indexInfos, indexInfos);
        wrapper.Jsonize("batch_size", batchSize, batchSize);
        wrapper.Jsonize("use_nest_table", useNest, useNest);
        wrapper.Jsonize("nest_table_attrs", nestTableAttrs, nestTableAttrs);
        wrapper.Jsonize("limit", limit, limit);
        wrapper.Jsonize("hints", hintsMap, hintsMap);
        wrapper.Jsonize("hash_type", hashType, hashType);
        wrapper.Jsonize("hash_fields", hashFields, hashFields);
        wrapper.Jsonize("table_distribution", tableDist, tableDist);
        wrapper.Jsonize(IQUAN_OP_ID, opId, opId);
        if (tableDist.hashMode._hashFields.size() > 0) { // compatible with old plan
            hashType = tableDist.hashMode._hashFunction;
            hashFields = tableDist.hashMode._hashFields;
        }
        if (hashFields.empty()) {
            SQL_LOG(ERROR, "hash fields is empty.");
            return false;
        }
        wrapper.Jsonize("used_fields", usedFields, usedFields);

        if (!KernelUtil::toJsonString(wrapper, "condition", conditionJson)) {
            return false;
        }
        if (!KernelUtil::toJsonString(wrapper, "output_field_exprs", outputExprsJson)) {
            return false;
        }
        if (outputExprsJson.empty()) {
            outputExprsJson = "{}";
        }
    } catch (const autil::legacy::ExceptionBase &e) {
        SQL_LOG(ERROR, "scanInitParam init failed error:[%s].", e.what());
        return false;
    } catch(...) {
        return false;
    }
    return true;
}

ScanBase::ScanBase()
    : _enableTableInfo(true)
    , _useSub(false)
    , _scanOnce(true)
    , _batchSize(0)
    , _limit(-1)
    , _seekCount(0)
    , _parallelNum(1)
    , _parallelIndex(0)
    , _opId(-1)
    , _batchScanBeginTime(0)
    , _pool(nullptr)
    , _queryMetricsReporter(nullptr)
    , _memoryPoolResource(nullptr)
    , _sqlSearchInfoCollector(nullptr)
    , _nestTableJoinType(LEFT_JOIN)

{
}

ScanBase::~ScanBase() {
    _indexPartitionReaderWrapper.reset();
    _memoryPoolResource = NULL;
    _sqlSearchInfoCollector = NULL;
    _attributeExpressionCreator.reset();
    _functionProvider.reset();
}

bool ScanBase::init(const ScanInitParam &param) {
    _tableName = param.tableName;
    _dbName = param.dbName;
    _catalogName = param.catalogName;
    _nodeName = param.nodeName;
    _opName = param.opName;
    _indexInfos = param.indexInfos;
    _outputFields = param.outputFields;
    _outputFieldsType = param.outputFieldsType;
    _batchSize = param.batchSize;
    _limit = param.limit;
    _parallelIndex = param.parallelIndex;
    _opId = param.opId;
    _parallelNum = param.parallelNum;
    _conditionJson = param.conditionJson;
    _outputExprsJson = param.outputExprsJson;
    _hashFunc = param.hashType;
    _hashFields = param.hashFields;
    _usedFields = param.usedFields;
    _nestTableAttrs = param.nestTableAttrs;
    _scanInfo.set_tablename(_tableName);
    _scanInfo.set_kernelname(param.opName);
    _scanInfo.set_nodename(param.nodeName);
    _scanInfo.set_parallelid(_parallelIndex);
    _scanInfo.set_parallelnum(_parallelNum);
    patchHintInfo(param.hintsMap);
    SQL_LOG(DEBUG, "scan table [%s] in ip [%s], part id [%s], limit[%u].", _tableName.c_str(),
            WorkerParam::getEnv(WorkerParam::IP, "").c_str(),
            WorkerParam::getEnv(WorkerParam::PART_ID, "").c_str(), _limit);

    if (_opId < 0) {
        string keyStr = param.opName + "__" + _tableName + "__" + _conditionJson;
        _scanInfo.set_hashkey(autil::HashAlgorithm::hashString(
                        keyStr.c_str(), keyStr.size(), 1));
    } else {
        _scanInfo.set_hashkey(_opId);
    }
    _memoryPoolResource = param.memoryPoolResource;
    if (!_memoryPoolResource) {
        SQL_LOG(ERROR, "get mem pool resource failed.");
        return false;
    }
    SqlBizResource* bizResource = param.bizResource;
    if (!bizResource) {
        SQL_LOG(ERROR, "get sql biz resource failed.");
        return false;
    }
    SqlQueryResource* queryResource = param.queryResource;
    if (!queryResource) {
        SQL_LOG(ERROR, "get sql query resource failed.");
        return false;
    }
    _pool = queryResource->getPool();
    KERNEL_REQUIRES(_pool, "get pool failed");
    _queryMetricsReporter = queryResource->getQueryMetricsReporter();
    _sqlSearchInfoCollector = queryResource->getSqlSearchInfoCollector();

    int64_t timeoutMs = queryResource->getTimeoutMs();
    if (timeoutMs > 0) {
        int64_t leftTimeout = timeoutMs * 1000;
        _timeoutTerminator.reset(new TimeoutTerminator(
                        leftTimeout * SCAN_TIMEOUT_PERCENT,
                        queryResource->getStartTime()));
        _timeoutTerminator->init(1 << 12);
    }

    if (_enableTableInfo) {
        _tableInfo = bizResource->getTableInfo(_tableName);
        if (!_tableInfo) {
            SQL_LOG(ERROR, "table [%s] not exist.", _tableName.c_str());
            return false;
        }
        _indexPartitionReaderWrapper =
            IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
                    queryResource->getPartitionReaderSnapshot(), _tableName);
        if (!_indexPartitionReaderWrapper) {
            SQL_LOG(ERROR, "create index partition reader wrapper failed.");
            return false;
        }
        if (param.useNest) {
            const string &subTableName = _tableInfo->getSubTableName();
            for (auto &nestTableAttr : _nestTableAttrs) {
                if (subTableName == nestTableAttr.tableName) {
                    _useSub = true;
                    SQL_LOG(TRACE1, "table [%s] use sub doc.", _tableName.c_str());
                } else {
                    SQL_LOG(ERROR, "table [%s] need unnest multi value, not support.[%s != %s]",
                            _tableName.c_str(), subTableName.c_str(), nestTableAttr.tableName.c_str());
                    return false;
                }
            }
        }
        _indexPartitionReaderWrapper->setSessionPool(_pool);
        if (!initExpressionCreator(_tableInfo, _useSub,  bizResource, queryResource, param.tableType)) {
            SQL_LOG(WARN, "table [%s] init expression creator failed.",
                    _tableName.c_str());
            return false;
        }
    }
    return true;
}

bool ScanBase::initExpressionCreator(const TableInfoPtr &tableInfo, bool useSub,
                                     SqlBizResource *bizResource,
                                     SqlQueryResource *queryResource,
                                     const std::string& tableType)
{
    bool useSubFlag = useSub && tableInfo->hasSubSchema();
    if (SCAN_KV_TABLE_TYPE == tableType || SCAN_KKV_TABLE_TYPE == tableType) {
        auto partReader = _indexPartitionReaderWrapper->getReader();
        if (unlikely(!partReader)) {
            return false;
        }
        _matchDocAllocator = CollectorUtil::createMountedMatchDocAllocator(partReader->GetSchema(), _pool);
    } else {
        _matchDocAllocator.reset(new MatchDocAllocator(_memoryPoolResource->getPool(), useSubFlag));
    }
    _functionProvider.reset(new FunctionProvider(_matchDocAllocator.get(),
                    _pool, queryResource->getSuezCavaAllocator(), queryResource->getTracer(),
                    queryResource->getPartitionReaderSnapshot(), NULL));
    std::vector<suez::turing::VirtualAttribute*> virtualAttributes;
    _attributeExpressionCreator.reset(new AttributeExpressionCreator(
                    _pool, _matchDocAllocator.get(),
                    _tableName, queryResource->getPartitionReaderSnapshot(),
                    tableInfo, virtualAttributes,
                    bizResource->getFunctionInterfaceCreator().get(),
                    bizResource->getCavaPluginManager(), _functionProvider.get(),
                    tableInfo->getSubTableName()));
    return true;
}

bool ScanBase::batchScan(TablePtr &table, bool &eof) {
    incComputeTime();
    _batchScanBeginTime = TimeUtility::currentTime();
    if (!doBatchScan(table, eof)) {
        return false;
    }
    if (_scanInfo.totalcomputetimes() < 5 && table != nullptr) {
        SQL_LOG(TRACE3, "scan id [%d] output table [%s]: [%s]", _parallelIndex, _tableName.c_str(),
                        TableUtil::toString(table, 10).c_str());
    }
    int64_t endTime = TimeUtility::currentTime();
    incTotalTime(endTime - _batchScanBeginTime);
    incTotalSeekCount(_seekCount);
    if (table) {
        incTotalOutputCount(table->getRowCount());
    }
    if (eof) {
        if (_sqlSearchInfoCollector) {
            _sqlSearchInfoCollector->addScanInfo(_scanInfo);
        }
        reportMetrics();
        SQL_LOG(DEBUG, "scan info: [%s]", _scanInfo.ShortDebugString().c_str());
    }
    return true;
}

void ScanBase::patchHintInfo(const map<string, map<string, string> > &hintsMap) {
    const auto &mapIter = hintsMap.find(SQL_SCAN_HINT);
    if (mapIter == hintsMap.end()) {
        return;
    }
    const map<string, string> &hints = mapIter->second;
    if (hints.empty()) {
        return;
    }
    auto iter = hints.find("localLimit");
    if (iter != hints.end()) {
        uint32_t localLimit = 0;
        StringUtil::fromString(iter->second, localLimit);
        if (localLimit > 0) {
            _limit = localLimit;
        }
    }
    iter = hints.find("batchSize");
    if (iter != hints.end()) {
        uint32_t batchSize = 0;
        StringUtil::fromString(iter->second, batchSize);
        if (batchSize > 0) {
            _batchSize = batchSize;
        }
    }
    iter = hints.find("nestTableJoinType");
    if (iter != hints.end()) {
        if (iter->second == "inner") {
            _nestTableJoinType = INNER_JOIN;
        } else {
            _nestTableJoinType = LEFT_JOIN;
        }
    }
}

string ScanBase::getReportMetricsPath() {
    return  _opName + "/" + _tableName;
}

void ScanBase::reportMetrics() {
    if (_queryMetricsReporter != nullptr) {
        string pathName = "sql.user.ops." + _opName;
        auto opMetricsReporter =
            _queryMetricsReporter->getSubReporter(pathName, {{{"table_name", getTableNameForMetrics()}}});
        opMetricsReporter->report<ScanOpMetrics, ScanInfo>(nullptr, &_scanInfo);
    }
}

void ScanBase::incComputeTime() {
    _scanInfo.set_totalcomputetimes(_scanInfo.totalcomputetimes() + 1);
}

void ScanBase::incInitTime(int64_t time) {
    _scanInfo.set_totalinittime(_scanInfo.totalinittime() + time);
    incTotalTime(time);
}

void ScanBase::incSeekTime(int64_t time) {
    _scanInfo.set_totalseektime(_scanInfo.totalseektime() + time);
}

void ScanBase::incEvaluateTime(int64_t time) {
    _scanInfo.set_totalevaluatetime(_scanInfo.totalevaluatetime() + time);
}

void ScanBase::incOutputTime(int64_t time) {
    _scanInfo.set_totaloutputtime(_scanInfo.totaloutputtime() + time);
}

void ScanBase::incTotalTime(int64_t time) {
    _scanInfo.set_totalusetime(_scanInfo.totalusetime() + time);
}
void ScanBase::incTotalScanCount(int64_t count) {
    _scanInfo.set_totalscancount(_scanInfo.totalscancount() + count);
}
void ScanBase::incTotalSeekCount(int64_t count) {
    _scanInfo.set_totalseekcount(_scanInfo.totalseekcount() + count);
}
void ScanBase::incTotalOutputCount(int64_t count) {
    _scanInfo.set_totaloutputcount(_scanInfo.totaloutputcount() + count);
}


END_HA3_NAMESPACE(sql);
