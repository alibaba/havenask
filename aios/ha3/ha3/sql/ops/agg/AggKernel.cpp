#include <ha3/sql/ops/agg/AggKernel.h>
#include <ha3/sql/ops/agg/AggLocal.h>
#include <ha3/sql/ops/agg/AggGlobal.h>
#include <ha3/sql/ops/agg/AggNormal.h>
#include <ha3/sql/resource/SqlQueryResource.h>
#include <ha3/sql/resource/SqlBizResource.h>
#include <ha3/turing/ops/SqlSessionResource.h>
#include <ha3/sql/ops/util/KernelRegister.h>
#include <ha3/sql/ops/util/KernelUtil.h>
#include <ha3/sql/data/TableUtil.h>
#include <autil/legacy/RapidJsonCommon.h>

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace autil;
using namespace kmonitor;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, AggKernel);

class AggOpMetrics : public MetricsGroup
{
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_GAUGE_MUTABLE_METRIC(_totalComputeTimes, "TotalComputeTimes");
        REGISTER_GAUGE_MUTABLE_METRIC(_outputCount, "OutputCount");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalAggTime, "TotalAggTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_collectTime, "collectTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_outputAccTime, "outputAccTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_mergeTime, "mergeTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_outputResultTime, "outputResultTime");
        REGISTER_GAUGE_MUTABLE_METRIC(_aggPoolSize, "aggPoolSize");
        return true;
    }
    void report(const kmonitor::MetricsTags *tags, AggInfo *aggInfo) {
        REPORT_MUTABLE_METRIC(_totalComputeTimes, aggInfo->totalcomputetimes());
        REPORT_MUTABLE_METRIC(_outputCount, aggInfo->totaloutputcount());
        REPORT_MUTABLE_METRIC(_totalAggTime, aggInfo->totalusetime() / 1000);
        if (_collectTime > 0) {
            REPORT_MUTABLE_METRIC(_collectTime, aggInfo->collecttime() / 1000);
        }
        if (_outputAccTime > 0) {
            REPORT_MUTABLE_METRIC(_outputAccTime, aggInfo->outputacctime() / 1000);
        }
        if (_mergeTime > 0) {
            REPORT_MUTABLE_METRIC(_mergeTime, aggInfo->mergetime() / 1000);
        }
        if (_outputResultTime > 0) {
            REPORT_MUTABLE_METRIC(_outputResultTime, aggInfo->outputresulttime() / 1000);
        }
        REPORT_MUTABLE_METRIC(_aggPoolSize, aggInfo->aggpoolsize());
    }
private:
    MutableMetric *_totalComputeTimes = nullptr;
    MutableMetric *_outputCount = nullptr;
    MutableMetric *_totalAggTime = nullptr;
    MutableMetric *_collectTime = nullptr;
    MutableMetric *_outputAccTime = nullptr;
    MutableMetric *_mergeTime = nullptr;
    MutableMetric *_outputResultTime = nullptr;
    MutableMetric *_aggPoolSize = nullptr;
};

AggKernel::AggKernel()
    : N2OneKernel("AggKernel")
    , _opId(-1)
{
}

std::string AggKernel::inputPort() const {
    return "input0";
}

std::string AggKernel::outputPort() const {
    return "output0";
}

bool AggKernel::config(navi::KernelConfigContext &ctx) {
    auto &json = ctx.getJsonAttrs();
    json.Jsonize(AGG_OUTPUT_FIELD_ATTRIBUTE, _outputFields);
    json.Jsonize(AGG_OUTPUT_FIELD_TYPE_ATTRIBUTE, _outputFieldsType);
    json.Jsonize(AGG_SCOPE_ATTRIBUTE, _scope);
    json.Jsonize(AGG_FUNCTION_ATTRIBUTE, _aggFuncDesc, _aggFuncDesc);
    json.Jsonize(AGG_GROUP_BY_KEY_ATTRIBUTE, _groupKeyVec, _groupKeyVec);
    json.Jsonize(IQUAN_OP_ID, _opId, _opId);
    std::map<std::string, std::map<std::string, std::string> > hintsMap;
    json.Jsonize("hints", hintsMap, hintsMap);
    patchHintInfo(hintsMap);
    KernelUtil::stripName(_outputFields);
    KernelUtil::stripName(_groupKeyVec);
    return true;
}

navi::ErrorCode AggKernel::init(navi::KernelInitContext &initContext) {
    SqlBizResource *bizResource = initContext.getResource<SqlBizResource>("SqlBizResource");
    KERNEL_REQUIRES(bizResource, "get sql biz resource failed");
    auto sqlSessionResource =
        bizResource->getObject<turing::SqlSessionResource>("SqlSessionResource");
    KERNEL_REQUIRES(sqlSessionResource, "get sql session resource failed");
    _aggFuncManager = sqlSessionResource->aggFuncManager;
    KERNEL_REQUIRES(_aggFuncManager, "get agg Func Manager failed");

    SqlQueryResource *queryResource = initContext.getResource<SqlQueryResource>("SqlQueryResource");
    KERNEL_REQUIRES(queryResource, "get sql query resource failed");
    autil::mem_pool::Pool *pool = queryResource->getPool();
    KERNEL_REQUIRES(pool, "get pool failed");
    _queryMetricsReporter = queryResource->getQueryMetricsReporter();
    auto memoryPoolResource = initContext.getResource<navi::MemoryPoolResource>(
            navi::RESOURCE_MEMORY_POOL_URI);
    _calcTable.reset(new CalcTable(pool, memoryPoolResource, _outputFields, _outputFieldsType,
                                   nullptr, nullptr, {}, nullptr));

    if (_scope == "PARTIAL") {
        _aggBase.reset(new AggLocal(pool, memoryPoolResource, _aggHints));
    } else if (_scope == "FINAL") {
        _aggBase.reset(new AggGlobal(pool, memoryPoolResource, _aggHints));
    } else if (_scope == "NORMAL") {
        _aggBase.reset(new AggNormal(pool, memoryPoolResource, _aggHints));
    } else {
        SQL_LOG(ERROR, "scope type [%s] invalid", _scope.c_str());
        return navi::EC_INIT_GRAPH;
    }
    if (!_aggBase->init(_aggFuncManager, _groupKeyVec,
                        _outputFields, _aggFuncDesc))
    {
        SQL_LOG(ERROR, "agg init failed");
        return navi::EC_INIT_GRAPH;
    }

    _sqlSearchInfoCollector = queryResource->getSqlSearchInfoCollector();
    _aggInfo.set_kernelname(getKernelName());
    _aggInfo.set_nodename(getNodeName());
    if (_opId < 0) {
        string keyStr = getKernelName();
        _aggInfo.set_hashkey(autil::HashAlgorithm::hashString(keyStr.c_str(), keyStr.size(), 1));
    } else {
        _aggInfo.set_hashkey(_opId);
    }
    return navi::EC_NONE;
}

navi::ErrorCode AggKernel::collect(const navi::DataPtr &data) {
    incComputeTime();
    uint64_t beginTime = TimeUtility::currentTime();
    TablePtr input = dynamic_pointer_cast<Table>(data);
    if (input == nullptr) {
        SQL_LOG(ERROR, "invalid input table");
        return navi::EC_ABORT;
    }
    if (!_aggBase->compute(input)) {
        SQL_LOG(ERROR, "agg compute failed");
        return navi::EC_ABORT;
    }
    incTotalTime(TimeUtility::currentTime() - beginTime);
    return navi::EC_NONE;
}

navi::ErrorCode AggKernel::finalize(navi::DataPtr &data) {
    uint64_t beginTime = TimeUtility::currentTime();
    if (!_aggBase->finalize()) {
        SQL_LOG(ERROR, "agg finalize failed");
        return navi::EC_ABORT;
    }
    TablePtr table = _aggBase->getTable();
    if (table == nullptr) {
        SQL_LOG(ERROR, "get output table failed");
        return navi::EC_ABORT;
    }
    if (table->getRowCount() == 0) {
        SQL_LOG(TRACE1, "table is empty");
    }
    if (!_calcTable->projectTable(table)) {
        SQL_LOG(WARN, "project table [%s] failed.", TableUtil::toString(table, 5).c_str());
        return navi::EC_ABORT;
    }
    incOutputCount(table->getRowCount());
    data = table;
    incTotalTime(TimeUtility::currentTime() - beginTime);

    uint64_t collectTime, outputAccTime, mergeTime, outputResultTime, aggPoolSize;
    _aggBase->getStatistics(collectTime, outputAccTime, mergeTime, outputResultTime, aggPoolSize);
    _aggInfo.set_collecttime(collectTime);
    _aggInfo.set_outputacctime(outputAccTime);
    _aggInfo.set_mergetime(mergeTime);
    _aggInfo.set_outputresulttime(outputResultTime);
    _aggInfo.set_aggpoolsize(aggPoolSize);

    reportMetrics();

    if (_sqlSearchInfoCollector) {
        _sqlSearchInfoCollector->addAggInfo(_aggInfo);
    }
    SQL_LOG(TRACE3, "agg output table: [%s]", TableUtil::toString(table, 10).c_str());
    SQL_LOG(DEBUG, "agg info: [%s]", _aggInfo.ShortDebugString().c_str());
    return navi::EC_NONE;
}

void AggKernel::patchHintInfo(const map<string, map<string, string> > &hintsMap) {
    const auto &mapIter = hintsMap.find(SQL_AGG_HINT);
    if (mapIter == hintsMap.end()) {
        return;
    }
    const map<string, string> &hints = mapIter->second;
    if (hints.empty()) {
        return;
    }
    auto iter = hints.find("groupKeyLimit");
    if (iter != hints.end()) {
        size_t keyLimit = 0;
        StringUtil::fromString(iter->second, keyLimit);
        if (keyLimit > 0) {
            _aggHints.groupKeyLimit = keyLimit;
        }
    }
    iter = hints.find("stopExceedLimit");
    if (iter != hints.end()) {
        bool stop = true;
        StringUtil::fromString(iter->second, stop);
        _aggHints.stopExceedLimit = stop;
    }
    iter = hints.find("memoryLimit");
    if (iter != hints.end()) {
        size_t memoryLimit = 0;
        StringUtil::fromString(iter->second, memoryLimit);
        _aggHints.memoryLimit = memoryLimit;
    }
}

void AggKernel::reportMetrics() {
    if (_queryMetricsReporter != nullptr) {
        string pathName = "sql.user.ops." + getKernelName();
        auto opMetricsReporter =
            _queryMetricsReporter->getSubReporter(pathName, {{{"scope", _scope}}});
        opMetricsReporter->report<AggOpMetrics, AggInfo>(nullptr, &_aggInfo);
    }
}

void AggKernel::incComputeTime() {
    _aggInfo.set_totalcomputetimes(_aggInfo.totalcomputetimes() + 1);
}

void AggKernel::incOutputCount(uint32_t count) {
    _aggInfo.set_totaloutputcount(_aggInfo.totaloutputcount() + count);
}

void AggKernel::incTotalTime(int64_t time) {
    _aggInfo.set_totalusetime(_aggInfo.totalusetime() + time);
}

REGISTER_KERNEL(AggKernel);

END_HA3_NAMESPACE(sql);
