#include <ha3/sql/ops/exchange/ExchangeKernel.h>
#include <ha3/sql/resource/SqlBizResource.h>
#include <ha3/sql/resource/SqlQueryResource.h>
#include <ha3/sql/ops/util/KernelRegister.h>
#include <ha3/sql/ops/util/KernelUtil.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/config/TypeDefine.h>
#include <ha3/turing/variant/SqlTableVariant.h>
#include <ha3/turing/variant/SqlResultVariant.h>
#include <multi_call/interface/Reply.h>
#include <ha3/service/RpcContextUtil.h>
#include <ha3/turing/qrs/QrsBiz.h>

using namespace std;
using namespace autil;
using namespace tensorflow;
using namespace navi;
using namespace autil;
using namespace kmonitor;

USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(turing);
BEGIN_HA3_NAMESPACE(sql);

class ExchangeOpMetrics : public MetricsGroup
{
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_GAUGE_MUTABLE_METRIC(_tableRowCount, "tableRowCount");
        REGISTER_QPS_MUTABLE_METRIC(_lackQps, "LackQps");
        REGISTER_GAUGE_MUTABLE_METRIC(_lackProviderCount, "LackProviderCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_accessProviderCount, "AccessProviderCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_allocatedPoolSize, "allocatedPoolSize");
        REGISTER_GAUGE_MUTABLE_METRIC(_tablePoolSize, "tablePoolSize");
        REGISTER_LATENCY_MUTABLE_METRIC(_searcherUsedTime, "SearcherUsedTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_fillResultUsedTime, "FillResultUsedTime");
        return true;
    }
    void report(const kmonitor::MetricsTags *tags, ExchangeInfo *exchangeInfo) {
        REPORT_MUTABLE_METRIC(_tableRowCount, exchangeInfo->rowcount());
        if (exchangeInfo->lackcount() > 0) {
            REPORT_MUTABLE_QPS(_lackQps);
        }
        REPORT_MUTABLE_METRIC(_lackProviderCount, exchangeInfo->lackcount());
        REPORT_MUTABLE_METRIC(_accessProviderCount, exchangeInfo->totalaccesscount());
        REPORT_MUTABLE_METRIC(_allocatedPoolSize, exchangeInfo->poolsize());
        REPORT_MUTABLE_METRIC(_tablePoolSize, exchangeInfo->tablepoolsize());
        REPORT_MUTABLE_METRIC(_searcherUsedTime, exchangeInfo->searcherusetime() / 1000);
        REPORT_MUTABLE_METRIC(_fillResultUsedTime, exchangeInfo->fillresultusetime() / 1000);
    }
private:
    MutableMetric *_tableRowCount = nullptr;
    MutableMetric *_lackQps = nullptr;
    MutableMetric *_lackProviderCount = nullptr;
    MutableMetric *_accessProviderCount = nullptr;
    MutableMetric *_tablePoolSize = nullptr;
    MutableMetric *_allocatedPoolSize = nullptr;
    MutableMetric *_searcherUsedTime = nullptr;
    MutableMetric *_fillResultUsedTime = nullptr;
};

HA3_LOG_SETUP(sql, ExchangeKernel);
ExchangeKernel::ExchangeKernel()
    : _lackResultEnable(false)
    , _opId(-1)
{
}

ExchangeKernel::~ExchangeKernel() {
}

// kernel def, see: navi/sql/executor/proto/KernelDef.proto
const navi::KernelDef *ExchangeKernel::getDef() const {
    auto def = new navi::KernelDef();
    def->set_kernel_name("ExchangeKernel");
    KERNEL_REGISTER_ADD_OUTPUT(def, "output0");
    return def;
}

bool ExchangeKernel::config(navi::KernelConfigContext &ctx) {
    _graphStr = ctx.getBinaryAttr(EXCHANGE_GRAPH_ATTR);
    if (_graphStr.empty()) {
        SQL_LOG(ERROR, "empty graph def");
        return false;
    }
    auto &json = ctx.getJsonAttrs();
    json.Jsonize(EXCHANGE_INPUT_PORT_ATTR, _inputPort);
    json.Jsonize(EXCHANGE_INPUT_NODE_ATTR, _inputNode);
    json.Jsonize(SUB_GRAPH_RUN_PARAM_TIMEOUT, _timeout);
    json.Jsonize(SUB_GRAPH_RUN_PARAM_TRACE_LEVEL, _traceLevel);
    json.Jsonize(SUB_GRAPH_RUN_PARAM_THREAD_LIMIT, _threadLimit);
    json.Jsonize(EXCHANGE_LACK_RESULT_ENABLE, _lackResultEnable);
    json.Jsonize(IQUAN_OP_ID, _opId, _opId);
    json.Jsonize("source_id", _sourceId, _sourceId);
    json.Jsonize("source_spec", _sourceSpec, _sourceSpec);
    json.Jsonize("table_distribution", _tableDist, _tableDist);
    json.Jsonize("table_name", _tableName, _tableName);
    json.Jsonize("table_type", _tableType, _tableType);
    string dbName;
    json.Jsonize("db_name", dbName);
    _bizName = getBizName(dbName);
    return true;
}

navi::ErrorCode ExchangeKernel::init(navi::KernelInitContext &initContext) {
    _memoryPoolResource = initContext.getResource<navi::MemoryPoolResource>(
        navi::RESOURCE_MEMORY_POOL_URI);
    if (!_memoryPoolResource) {
        SQL_LOG(ERROR, "get mem pool resource failed");
        return navi::EC_ABORT;
    }
    _tablePool = _memoryPoolResource->getPool();
    KERNEL_REQUIRES(_tablePool, "get table pool failed");
    if (!patchKhronosHashInfo()) {
        return navi::EC_ABORT;
    }
    SqlQueryResource* queryResource = initContext.getResource<SqlQueryResource>("SqlQueryResource");
    KERNEL_REQUIRES(queryResource, "get sql query resource failed");
    _pool = queryResource->getPool();
    KERNEL_REQUIRES(_pool, "get pool failed");
    _gigQuerySession = queryResource->getGigQuerySession();
    KERNEL_REQUIRES(_gigQuerySession, "get gig query session failed");
    _queryMetricsReporter = queryResource->getQueryMetricsReporter();
    _globalSqlSearchInfoCollector = queryResource->getSqlSearchInfoCollector();
    _exchangeInfo.set_kernelname(getKernelName());
    _exchangeInfo.set_nodename(getNodeName());
    if (_opId < 0) {
        string keyStr = getKernelName() + "__" + _graphStr;
        _exchangeInfo.set_hashkey(autil::HashAlgorithm::hashString(keyStr.c_str(), keyStr.size(), 1));
    } else {
        _exchangeInfo.set_hashkey(_opId);
    }
    return navi::EC_NONE;
}

navi::ErrorCode ExchangeKernel::compute(navi::KernelComputeContext &runContext) {
    uint64_t beginTime = TimeUtility::currentTime();
    // todo batch input/output
    multi_call::SourceIdTy sourceId = multi_call::INVALID_SOURCE_ID;
    if (!_sourceId.empty()) {
        sourceId = HashAlgorithm::hashString64(_sourceId.c_str(), _sourceId.size());
    }
    Ha3QrsSqlRequestGeneratorParam generatorParam;
    generatorParam.bizName = _bizName;
    generatorParam.sourceId = sourceId;
    generatorParam.graphStr = _graphStr;
    generatorParam.inputPort = _inputPort;
    generatorParam.inputNode = _inputNode;
    generatorParam.timeout = _timeout;
    generatorParam.traceLevel = _traceLevel;
    generatorParam.threadLimit = _threadLimit;
    generatorParam.tableDist = _tableDist;
    generatorParam.sourceSpec = _sourceSpec;
    Ha3QrsSqlRequestGeneratorPtr requestGenerator(new Ha3QrsSqlRequestGenerator(generatorParam));
    auto rpcContext = _gigQuerySession->getRpcContext();
    string benchMarkKey;
    if (rpcContext) {
        benchMarkKey = rpcContext->getUserData(HA_RESERVED_BENCHMARK_KEY);
    }
    if (HA_RESERVED_BENCHMARK_VALUE_1 == benchMarkKey
        || HA_RESERVED_BENCHMARK_VALUE_2 == benchMarkKey) {
        requestGenerator->setFlowControlStrategy(QrsBiz::getBenchmarkBizName(_bizName));
    } else {
        requestGenerator->setFlowControlStrategy(_bizName);
    }
    multi_call::SearchParam param;
    param.generatorVec.push_back(requestGenerator);
    multi_call::ReplyPtr reply;
    RpcInfo *rpcInfo = nullptr;
    if (_globalSqlSearchInfoCollector) {
        rpcInfo= _globalSqlSearchInfoCollector->addRpcInfo();
        rpcInfo->set_begintime(beginTime);
    }
    _gigQuerySession->call(param, reply);
    uint64_t searchEndTime = TimeUtility::currentTime();
    _exchangeInfo.set_searcherusetime(searchEndTime - beginTime);
    if (!fillResults(reply, rpcInfo)) {
        SQL_LOG(ERROR, "fill results failed");
        return navi::EC_ABORT;
    }
    if (_table == nullptr) {
        SQL_LOG(ERROR, "no merge result");
        return navi::EC_ABORT;
    }
    if (_globalSqlSearchInfoCollector) {
        rpcInfo->set_totalrpccount(requestGenerator->getPartIdSet().size());
        rpcInfo->set_usetime(searchEndTime - beginTime);
        if (_globalSqlSearchInfoCollector->getForbitMerge()) {
            _localSqlSearchInfoCollector.setForbitMerge(true);
        }
        _localSqlSearchInfoCollector.mergeInQrs();
        _globalSqlSearchInfoCollector->mergeSqlSearchInfo(_localSqlSearchInfoCollector.getSqlSearchInfo());
        _exchangeInfo.set_totalaccesscount(rpcInfo->totalrpccount());
        _exchangeInfo.set_lackcount(rpcInfo->lackcount());
    }
    _exchangeInfo.set_fillresultusetime(TimeUtility::currentTime() - searchEndTime);
    _exchangeInfo.set_rowcount(_table->getRowCount());
    _exchangeInfo.set_poolsize(_pool->getAllocatedSize());
    _exchangeInfo.set_tablepoolsize(_tablePool->getAllocatedSize());
    if (_globalSqlSearchInfoCollector) {
        _globalSqlSearchInfoCollector->addExchangeInfo(_exchangeInfo);
    }
    reportMetrics();
    navi::PortIndex outputIndex(0, navi::INVALID_INDEX);
    SQL_LOG(DEBUG, "exchange info: [%s]", _exchangeInfo.ShortDebugString().c_str());
    runContext.setOutput(outputIndex, _table, true);
    return navi::EC_NONE;
}

bool ExchangeKernel::fillResults(multi_call::ReplyPtr &reply, RpcInfo *rpcInfo) {
    if (reply == nullptr) {
        SQL_LOG(ERROR, "run search graph [%s] failed", _bizName.c_str());
        return false;
    }
    size_t lackCount = 0;
    std::vector<multi_call::ResponsePtr> responseVec = reply->getResponses(lackCount);
    if (rpcInfo) {
        rpcInfo->set_lackcount(lackCount);
        fillRpcInfo(responseVec, rpcInfo);
    }
    if (lackCount != 0) {
        if (_lackResultEnable) {
            SQL_LOG(WARN, "lack count is %ld, ignored", lackCount);
        } else {
            SQL_LOG(ERROR, "lack count is %ld", lackCount);
            return false;
        }
    }
    for (const auto &response : responseVec) {
        if (!getDataFromResponse(response, reply)) {
            lackCount ++;
            if (rpcInfo) {
                rpcInfo->set_lackcount(lackCount);
            }
            if (_lackResultEnable) {
                SQL_LOG(WARN, "get data from response failed, biz name [%s], ignored",
                        _bizName.c_str());
                continue;
            }
            SQL_LOG(ERROR, "get data from response failed, biz name [%s]",
                    _bizName.c_str());
            return false;
        }
        size_t curSize = _pool->getAllocatedSize();
        if (curSize >= MAX_SQL_POOL_SIZE) {
            SQL_LOG(WARN, "query pool use size [%ld] larger than [%ld]", curSize, MAX_SQL_POOL_SIZE);
            return false;
        }
    }
    return true;
}

void ExchangeKernel::fillRpcInfo(const vector<multi_call::ResponsePtr> &responseVec, RpcInfo *rpcInfo) {
    for (const auto &response : responseVec) {
        if (response != nullptr) {
            RpcNodeInfo *rpcNodeInfo = rpcInfo->add_rpcnodeinfos();
            rpcNodeInfo->set_partid(response->getPartId());
            rpcNodeInfo->set_errorcode(response->getErrorCode());
            rpcNodeInfo->set_isreturned(response->isReturned());
            rpcNodeInfo->set_specstr(response->getSpecStr());
            rpcNodeInfo->set_rpcusetime(response->rpcUsedTime());
            rpcNodeInfo->set_callusetime(response->callUsedTime());
            rpcNodeInfo->set_rpcbegin(response->getStat().rpcBeginTime);
            rpcNodeInfo->set_rpcend(response->getCallEndTime());
            rpcNodeInfo->set_netlatency(response->netLatency() * 1000);
        }
    }
}

bool ExchangeKernel::getSqlGraphResponse(const multi_call::ResponsePtr &response,
                                         SqlGraphResponse &sqlResponse)
{
    auto qrsResponse = dynamic_cast<QrsResponse *>(response.get());
    if (qrsResponse == NULL) {
        SQL_LOG(WARN, "invalid qrs response");
        return false;
    }
    auto message = dynamic_cast<suez::turing::GraphResponse *>(qrsResponse->getMessage());
    if (!message) {
        SQL_LOG(WARN, "get qrs response message failed");
        return false;
    }
    if (message->has_errorinfo() &&
        message->errorinfo().errorcode() != suez::turing::RS_ERROR_NONE)
    {
        SQL_LOG(WARN, "graph response has error: %s",
                message->errorinfo().errormsg().c_str());
        return false;
    }

    if (message->outputs_size() != 1) {
        SQL_LOG(WARN, "expect message output size 1, actually %d", message->outputs_size());
        return false;
    }

    const TensorProto &tensorProto =  message->outputs(0).tensor();
    if (tensorProto.dtype() != DT_STRING) {
        SQL_LOG(WARN, "invalid sql response tensor type");
        return false;
    }
    auto responseTensor = Tensor(DT_STRING, TensorShape({}));
    if (!responseTensor.FromProto(tensorProto)) {
        SQL_LOG(WARN, "invalid sql response tensor");
        return false;
    }
    const auto &responseStr = responseTensor.scalar<string>()();
    if (!sqlResponse.ParseFromString(responseStr)) {
        SQL_LOG(WARN, "parse sql response failed");
        return false;
    }
    return true;
}

bool ExchangeKernel::getTableFromTensorProto(const NamedTensorProto &tensorProto,
        TablePtr &table)
{
    if (!tensorProto.has_data()) {
        SQL_LOG(WARN, "sql response has no data");
        return false;
    }
    if (tensorProto.data().dtype() != DT_VARIANT) {
        SQL_LOG(WARN, "invalid tensor type");
        return false;
    }
    Tensor tableTensor(DT_VARIANT, {1});
    if (!tableTensor.FromProto(tensorProto.data())) {
        SQL_LOG(WARN, "invalid sql response");
        return false;
    }
    auto tableVariant = tableTensor.scalar<Variant>()().get<SqlTableVariant>();
    if (tableVariant == nullptr) {
        SQL_LOG(WARN, "invalid sql table variant");
        return false;
    }
    if (!tableVariant->construct(_tablePool, _pool)) {
        SQL_LOG(WARN, "construct sql table variant failed");
        return false;
    }
    if (tableVariant->isEmptyTable()) {
        SQL_LOG(WARN, "unexpected table is empty");
        return false;
    }
    table = tableVariant->getTable();
    if (table == nullptr) {
        SQL_LOG(WARN, "get sql table failed");
        return false;
    }
    return true;
}

bool ExchangeKernel::getResultFromTensorProto(const NamedTensorProto &tensorProto,
        navi::NaviResultPtr &result)
{
    if (!tensorProto.has_data()) {
        SQL_LOG(WARN, "sql response has no data");
        return false;
    }
    if (tensorProto.data().dtype() != DT_VARIANT) {
        SQL_LOG(WARN, "invalid tensor type");
        return false;
    }
    Tensor resultTensor(DT_VARIANT, {1});
    if (!resultTensor.FromProto(tensorProto.data())) {
        SQL_LOG(WARN, "invalid sql response");
        return false;
    }
    auto resultVariant = resultTensor.scalar<Variant>()().get<SqlResultVariant>();
    if (resultVariant == nullptr) {
        SQL_LOG(WARN, "invalid sql result variant");
        return false;
    }
    if (!resultVariant->construct(_pool)) {
        SQL_LOG(WARN, "construct sql result variant failed");
        return false;
    }
    result = resultVariant->getResult();
    if (result == nullptr) {
        SQL_LOG(WARN, "get sql result failed");
        return false;
    }
    return true;
}

bool ExchangeKernel::mergeSearchInfoFromTensorProto(const NamedTensorProto &tensorProto)
{
    if (!_globalSqlSearchInfoCollector) {
        return true;
    }
    if (!tensorProto.has_data()) {
        return true;
    }
    if (tensorProto.data().dtype() != DT_STRING) {
        SQL_LOG(WARN, "invalid tensor type");
        return false;
    }
    Tensor resultTensor(DT_STRING, {1});
    if (!resultTensor.FromProto(tensorProto.data())) {
        SQL_LOG(WARN, "invalid sql search info response");
        return false;
    }
    string searchInfoStr = resultTensor.scalar<string>()();
    SqlSearchInfo sqlSearchInfo;
    if (!sqlSearchInfo.ParseFromString(searchInfoStr)) {
        return false;
    }
    _localSqlSearchInfoCollector.mergeSqlSearchInfo(sqlSearchInfo);
    return true;
}

bool ExchangeKernel::getDataFromResponse(
        const multi_call::ResponsePtr &response,
        const multi_call::ReplyPtr &reply)
{
    if (response->isFailed()) {
        auto ec = response->getErrorCode();
        SQL_LOG(WARN, "multi call has error, error code=%d, errorMsg=%s",
                ec, reply->getErrorMessage(ec).c_str());
        return false;
    }

    SqlGraphResponse sqlResponse;
    if (!getSqlGraphResponse(response, sqlResponse)) {
        return false;
    }
    if (3 != sqlResponse.outputs_size()) {
        SQL_LOG(WARN, "expect sql response output size 3, actually %d", sqlResponse.outputs_size());
        return false;
    }

    navi::NaviResultPtr result;
    if (!getResultFromTensorProto(sqlResponse.outputs(1), result)) {
        return false;
    }
    if (navi::NAVI_TLS_LOGGER) {
        navi::NAVI_TLS_LOGGER->appendTrace(result->_trace);
    }
    if (result->_ec != EC_NONE) {
        SQL_LOG(WARN, "%s", result->_msg.c_str());
        return false;
    }

    TablePtr table;
    if (!getTableFromTensorProto(sqlResponse.outputs(0), table)) {
        return false;
    }
    SQL_LOG(TRACE3, "got input: %s", TableUtil::toString(table, 10).c_str());
    if (_table == nullptr) {
        _table = table;
    } else {
        if (!_table->merge(table)) {
            SQL_LOG(WARN, "merge table failed");
            return false;
        }
    }

    if (!mergeSearchInfoFromTensorProto(sqlResponse.outputs(2))) {
        return false;
    }
    return true;
}

string ExchangeKernel::getBizName(const string &dbName) const {
    return dbName + ZONE_BIZ_NAME_SPLITTER + HA3_DEFAULT_SQL;
}

void ExchangeKernel::reportMetrics() {
    if (_queryMetricsReporter != nullptr) {
        string pathName = "sql.user.ops." + getKernelName();
        auto opMetricsReporter =
            _queryMetricsReporter->getSubReporter(pathName, {});
        opMetricsReporter->report<ExchangeOpMetrics, ExchangeInfo>(nullptr, &_exchangeInfo);
    }
}

bool ExchangeKernel::patchKhronosHashInfo() {
    if (_tableType.empty()) {
        return true;
    }
    if (_tableType != "khronos_data" && _tableType != "khronos_series_data") {
        return true;
    }
    if (_tableDist.hashMode._hashFields.empty()) {
        SQL_LOG(WARN, "khronos hash field is empty");
        return false;
    }
    if (_tableName.empty()) {
        SQL_LOG(WARN, "khronos table name is empty");
        return false;
    }
    const string khronosHashField = "$metric";
    if (_tableDist.hashMode._hashFields[0] == khronosHashField) {
        _tableDist.hashValues[khronosHashField].push_back(_tableName);
    }

    return true;
}

REGISTER_KERNEL(ExchangeKernel);

END_HA3_NAMESPACE(sql);
