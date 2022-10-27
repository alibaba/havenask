#include <tensorflow/core/framework/op_kernel.h>
#include <ha3/common/ha3_op_common.h>
#include <ha3/turing/variant/SqlTableVariant.h>
#include <ha3/turing/variant/SqlResultVariant.h>
#include <ha3/turing/ops/SqlSessionResource.h>
#include <ha3/turing/ops/sql/proto/SqlSearch.pb.h>
#include <ha3/sql/resource/SqlBizResource.h>
#include <ha3/sql/resource/SqlQueryResource.h>
#include <ha3/sql/data/TensorData.h>
#include <ha3/sql/common/common.h>
#include <suez/turing/common/SessionResource.h>
#include <navi/engine/Navi.h>
#include <navi/engine/Data.h>
#include <navi/engine/GraphPort.h>
#include <navi/proto/GraphDef.pb.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace autil::mem_pool;
using namespace autil;
using namespace navi;
using namespace kmonitor;

USE_HA3_NAMESPACE(sql);
BEGIN_HA3_NAMESPACE(turing);

struct RunSqlOpCollector {
    int64_t latency = 0;
    uint32_t requestLength = 0;
    uint32_t responseLength = 0;
    uint32_t tableLength = 0;
    uint32_t searchInfoLength = 0;
    uint32_t naviInfoLength = 0;
    uint32_t naviRunCount = 0;
    uint32_t queryPoolTotalSize = 0;
    uint32_t allocatedPoolSize = 0;
    uint32_t rowCount = 0;
};

class RunSqlOpMetrics : public MetricsGroup
{
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_LATENCY_MUTABLE_METRIC(_latency, "sql.user.tf.run_sql.Latency");
        REGISTER_GAUGE_MUTABLE_METRIC(_rowCount, "sql.user.tf.run_sql.RequestLength");
        REGISTER_GAUGE_MUTABLE_METRIC(_responseLength, "sql.user.tf.run_sql.ResponseLength");
        REGISTER_GAUGE_MUTABLE_METRIC(_responseTableLength, "sql.user.tf.run_sql.ResponseTableLength");
        REGISTER_GAUGE_MUTABLE_METRIC(_responseSearchInfoLength, "sql.user.tf.run_sql.ResponseSearchInfoLength");
        REGISTER_GAUGE_MUTABLE_METRIC(_responseNaviInfoLength, "sql.user.tf.run_sql.ResponseNaviInfoLength");
        REGISTER_STATUS_MUTABLE_METRIC(_naviRunCount, "sql.user.tf.run_sql.NaviRunCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_queryPoolTotalSize, "sql.user.tf.run_sql.queryPoolTotalSize");
        REGISTER_GAUGE_MUTABLE_METRIC(_allocatedPoolSize, "sql.user.tf.run_sql.allocatedPoolSize");
        REGISTER_GAUGE_MUTABLE_METRIC(_rowCount, "sql.user.tf.run_sql.rowCount");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags, RunSqlOpCollector *collector) {
        REPORT_MUTABLE_METRIC(_latency, collector->latency);
        REPORT_MUTABLE_METRIC(_requestLength, collector->requestLength);
        REPORT_MUTABLE_METRIC(_responseLength, collector->responseLength);
        REPORT_MUTABLE_METRIC(_responseTableLength, collector->tableLength);
        REPORT_MUTABLE_METRIC(_responseSearchInfoLength, collector->searchInfoLength);
        REPORT_MUTABLE_METRIC(_responseNaviInfoLength, collector->naviInfoLength);
        REPORT_MUTABLE_METRIC(_naviRunCount, collector->naviRunCount);
        REPORT_MUTABLE_METRIC(_queryPoolTotalSize, collector->queryPoolTotalSize);
        REPORT_MUTABLE_METRIC(_allocatedPoolSize, collector->allocatedPoolSize);
        REPORT_MUTABLE_METRIC(_rowCount, collector->rowCount);
    }
private:
    MutableMetric *_latency = nullptr;
    MutableMetric *_requestLength = nullptr;
    MutableMetric *_responseLength = nullptr;
    MutableMetric *_responseTableLength = nullptr;
    MutableMetric *_responseSearchInfoLength = nullptr;
    MutableMetric *_responseNaviInfoLength = nullptr;
    MutableMetric *_naviRunCount = nullptr;
    MutableMetric *_queryPoolTotalSize = nullptr;
    MutableMetric *_allocatedPoolSize = nullptr;
    MutableMetric *_rowCount = nullptr;
};

class RunSqlOp : public tensorflow::OpKernel
{
public:
    explicit RunSqlOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {

    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override {
        int64_t beginTime = TimeUtility::currentTime();
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE_PTR(sessionResource);
        auto *metricsReporter = queryResource->getQueryMetricsReporter();

        map<std::string, navi::Resource *> resourceMap;
        SqlQueryResourcePtr sqlQueryResource(new SqlQueryResource(queryResource));
        resourceMap.insert(make_pair(sqlQueryResource->getResourceName(), sqlQueryResource.get()));
        const string &requestStr = ctx->input(0).scalar<string>()();
        RunSqlOpCollector metricCollector;
        metricCollector.requestLength = requestStr.size();
        SqlGraphRequest sqlRequest;
        OP_REQUIRES(
            ctx, sqlRequest.ParseFromString(requestStr),
            errors::Unavailable("parse sql graph request faield:", requestStr));
        HA3_LOG(DEBUG, "sql request: %s", sqlRequest.DebugString().c_str());
        Pool *pool = sqlQueryResource->getPool();
        SqlSearchInfoCollector *infoCollector = sqlQueryResource->getSqlSearchInfoCollector();
        const isearch::sql::GraphInfo &graphInfo = sqlRequest.graphinfo();
        auto graphDef = new navi::GraphDef();
        graphDef->CopyFrom(graphInfo.graphdef());
        auto params = graphInfo.rungraphparams();
        navi::RunGraphParams runGraphParams;
        int64_t timeoutMs = StringUtil::fromString<int64_t>(params[SUB_GRAPH_RUN_PARAM_TIMEOUT]) ;
        if (timeoutMs > 0 && TimeUtility::currentTime() - queryResource->startTime >= timeoutMs * 1000) {
            HA3_LOG(WARN, "run sql timeout, start [%ld], current [%ld], request [%s]",
                    queryResource->startTime, TimeUtility::currentTime(), sqlRequest.DebugString().c_str());
            OP_REQUIRES(ctx, false, errors::Unavailable("runsqlop timeout."));
        }
        sqlQueryResource->setTimeoutMs(timeoutMs);
        // todo search run graph need timeout before rpc timeout
        runGraphParams.setTimeoutMs(timeoutMs * 0.9);
        if (!params[SUB_GRAPH_RUN_PARAM_THREAD_LIMIT].empty()) {
            runGraphParams.setThreadLimit(StringUtil::fromString<uint32_t>(
                            params[SUB_GRAPH_RUN_PARAM_THREAD_LIMIT]));
        }
        runGraphParams.setTraceLevel(params[SUB_GRAPH_RUN_PARAM_TRACE_LEVEL]);
        navi::GraphInputPortsPtr inputs(new navi::GraphInputPorts());
        prepareInputs(graphInfo, pool, inputs);
        navi::GraphOutputPortsPtr outputs(new navi::GraphOutputPorts());
        prepareOutputs(graphInfo, outputs);
        SqlSessionResource *sqlSessionResource = nullptr;
        sessionResource->sharedObjectMap.get<SqlSessionResource>(SqlSessionResource::name(), sqlSessionResource);
        OP_REQUIRES(ctx, sqlSessionResource != nullptr,
                    errors::Unavailable("get sql session resource failed"));
        navi::NaviPtr naviPtr = sqlSessionResource->naviPtr;
        OP_REQUIRES(ctx, naviPtr != nullptr, errors::Unavailable("get navi failed"));
        auto currentNaviRun = _naviRunCount.fetch_add(1, std::memory_order_relaxed);
        REPORT_USER_MUTABLE_METRIC(metricsReporter, "sql.user.tf.run_sql.NaviRunCount", currentNaviRun);
        naviPtr->runGraph(graphDef, inputs, outputs, runGraphParams, resourceMap);
        outputs->wait();
        currentNaviRun = _naviRunCount.fetch_sub(1, std::memory_order_relaxed);
        string responseStr;
        OP_REQUIRES(ctx, generateResponseStr(outputs.get(), pool, infoCollector, &metricCollector, responseStr),
                    errors::Unavailable("generate response string failed"));
        Tensor* out = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {1}, &out));
        out->scalar<string>()() = std::move(responseStr);
        if (metricsReporter) {
            metricCollector.latency = (TimeUtility::currentTime() - beginTime) / 1000;
            metricCollector.naviRunCount = currentNaviRun;
            metricCollector.queryPoolTotalSize = pool->getTotalBytes();
            metricCollector.allocatedPoolSize = pool->getAllocatedSize();
            metricsReporter->report<RunSqlOpMetrics, RunSqlOpCollector>(nullptr, &metricCollector);
        }
        HA3_LOG(DEBUG, "response total length [%u], table length [%u], naviInfo Length[%u], searchInfo Length[%u]",
                metricCollector.responseLength, metricCollector.tableLength,
                metricCollector.naviInfoLength, metricCollector.searchInfoLength);
    }
private:
    void prepareInputs(const sql::GraphInfo &graphInfo,
                       Pool *pool, const navi::GraphInputPortsPtr &ports)
    {
        for (int i = 0; i < graphInfo.inputs_size(); i++) {
            const auto &info = graphInfo.inputs(i);
            auto port = new navi::GraphPort(info.name(), info.port());
            if (info.has_data()) {
                const TensorProto &dataProto = info.data();
                navi::DataPtr data;
                tensorProtoToData(dataProto, pool, data);
                port->setData(data);
            }
            ports->addPort(port);
        }
    }

    void prepareOutputs(const sql::GraphInfo &graphInfo, navi::GraphOutputPortsPtr &ports) {
        for (int i = 0; i < graphInfo.targets_size(); i++) {
            const auto &target = graphInfo.targets(i);
            ports->addPort(new navi::GraphPort(target.name(), target.port()));
        }
    }

    bool generateResponseStr(const navi::GraphPorts *outputs, Pool *pool,
                             SqlSearchInfoCollector *collector,
                             RunSqlOpCollector *metricCollector,
                             string &responseStr)
    {
        SqlGraphResponse sqlResponse;
        if (outputs->size() != 2) {
            HA3_LOG(WARN, "graph port outputs expect 2, actually %lu", outputs->size());
            return false;
        }

        auto iter = outputs->begin();

        DataPtr tableData;
        TensorProto *tableProto = getOutputData(iter, tableData, sqlResponse);
        if (!dataToSqlTableTensorProto(tableData, pool, tableProto, metricCollector->rowCount)) {
            return false;
        }
        metricCollector->tableLength = tableProto->ByteSizeLong();

        DataPtr sqlResultData;
        TensorProto *sqlResultProto = getOutputData(++iter, sqlResultData, sqlResponse);
        if (!dataToSqlResultTensorProto(sqlResultData, pool, sqlResultProto)) {
            return false;
        }
        metricCollector->naviInfoLength = sqlResultProto->ByteSizeLong();
        string searchInfoStr;
        if (collector) {
            collector->mergeInSearch();
            collector->serializeToString(searchInfoStr);
            metricCollector->searchInfoLength = searchInfoStr.size();
        }
        sql::NamedTensorProto *outputTensor = sqlResponse.add_outputs();
        outputTensor->set_name("sql_search_info");
        outputTensor->set_port("0");
        TensorProto *tensorProto = outputTensor->mutable_data();
        Tensor t(DT_STRING, {1});
        t.scalar<string>()() = std::move(searchInfoStr);
        t.AsProtoField(tensorProto);
        sqlResponse.SerializeToString(&responseStr);
        metricCollector->responseLength = responseStr.size();
        return true;
    }

    TensorProto *getOutputData(vector<GraphPort *>::const_iterator iter, DataPtr &data,
                               SqlGraphResponse &sqlResponse)
    {
        bool eof = false;
        (*iter)->getData(data, eof);
        sql::NamedTensorProto *outputTensor = sqlResponse.add_outputs();
        outputTensor->set_name((*iter)->getNode());
        outputTensor->set_port((*iter)->getPort());
        return outputTensor->mutable_data();
    }

    bool dataToSqlTableTensorProto(const navi::DataPtr &data, Pool *pool,
                                   TensorProto *tensorProto, uint32_t &rowCount)
    {
        TablePtr table = dynamic_pointer_cast<Table>(data);
        if (table) {
            rowCount = table->getRowCount();
            HA3_LOG(DEBUG, "searcher return row count [%d]", rowCount);
        }
        SqlTableVariant sqlTableVariant(table, pool);
        Tensor t(DT_VARIANT, {1});
        t.scalar<Variant>()() = sqlTableVariant;
        t.AsProtoField(tensorProto);
        return true;
    }

    bool dataToSqlResultTensorProto(const navi::DataPtr &data, Pool *pool,
                                    TensorProto *tensorProto)
    {
        navi::NaviResultPtr result = dynamic_pointer_cast<navi::NaviResult>(data);
        if (result) {
            SqlResultVariant sqlResultVariant(result, pool);
            Tensor t(DT_VARIANT, {1});
            t.scalar<Variant>()() = sqlResultVariant;
            t.AsProtoField(tensorProto);
            return true;
        }
        HA3_LOG(WARN, "trans data to sql result tensor proto failed");
        return false;
    }

    bool tensorProtoToData(const TensorProto &tensorProto, Pool *pool, navi::DataPtr &data) {
        tensorflow::Tensor t;
        if (tensorProto.dtype() != DT_VARIANT) {
            return false;
        }
        if (!t.FromProto(tensorProto)) {
            return false;
        }
        data.reset(new sql::TensorData(t, pool));
        return true;
    }

private:
    HA3_LOG_DECLARE();
    std::atomic_int_fast32_t _naviRunCount = {0};

};

HA3_LOG_SETUP(turing, RunSqlOp);

REGISTER_KERNEL_BUILDER(Name("RunSqlOp")
                        .Device(DEVICE_CPU),
                        RunSqlOp);

END_HA3_NAMESPACE(turing);
