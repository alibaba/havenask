#include <ha3/sql/ops/calc/CalcKernel.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/ops/util/KernelRegister.h>
#include <ha3/sql/ops/util/KernelUtil.h>
#include <ha3/sql/resource/SqlQueryResource.h>
#include <autil/TimeUtility.h>
#include <autil/StringUtil.h>
#include <autil/legacy/RapidJsonCommon.h>
#include <autil/legacy/RapidJsonHelper.h>
#include <ha3/sql/ops/util/SqlJsonUtil.h>
#include <ha3/sql/ops/condition/ConditionParser.h>
#include <ha3/sql/ops/condition/AliasConditionVisitor.h>
#include <ha3/sql/ops/calc/CalcConditionVisitor.h>
#include "suez/turing/expression/framework/MatchDocsExpressionCreator.h"

using namespace std;
using namespace matchdoc;
using namespace navi;
using namespace autil;
using namespace autil_rapidjson;
using namespace suez::turing;
using namespace autil;
using namespace kmonitor;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, CalcKernel);

class CalcOpMetrics : public MetricsGroup
{
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_GAUGE_MUTABLE_METRIC(_totalComputeTimes, "TotalComputeTimes");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalOutputCount, "TotalOutputCount");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalUsedTime, "TotalUsedTime");
        return true;
    }
    void report(const kmonitor::MetricsTags *tags, CalcInfo *calcInfo) {
        REPORT_MUTABLE_METRIC(_totalComputeTimes, calcInfo->totalcomputetimes());
        REPORT_MUTABLE_METRIC(_totalOutputCount, calcInfo->totaloutputcount());
        REPORT_MUTABLE_METRIC(_totalUsedTime, calcInfo->totalusetime() / 1000);
    }
private:
    MutableMetric *_totalComputeTimes = nullptr;
    MutableMetric *_totalOutputCount = nullptr;
    MutableMetric *_totalUsedTime = nullptr;
};


CalcKernel::CalcKernel()
    : _queryMetricsReporter(nullptr)
    , _sqlSearchInfoCollector(nullptr)
    , _opId(-1)
{
}

CalcKernel::~CalcKernel() {
}

const KernelDef *CalcKernel::getDef() const {
    auto def = new navi::KernelDef();
    def->set_kernel_name("CalcKernel");
    KERNEL_REGISTER_ADD_INPUT(def, "input0");
    KERNEL_REGISTER_ADD_OUTPUT(def, "output0");
    return def;
}

bool CalcKernel::config(navi::KernelConfigContext &ctx) {
    auto &json = ctx.getJsonAttrs();
    if (!KernelUtil::toJsonString(json, "condition", _conditionJson)) {
        return false;
    }
    if (!KernelUtil::toJsonString(json, "output_field_exprs", _outputExprsJson)) {
        return false;
    }
    json.Jsonize("output_fields", _outputFields);
    json.Jsonize("output_fields_type", _outputFieldsType, _outputFieldsType);
    json.Jsonize(IQUAN_OP_ID, _opId, _opId);
    KernelUtil::stripName(_outputFields);
    return true;
}

navi::ErrorCode CalcKernel::init(navi::KernelInitContext& context) {
    SqlQueryResource *queryResource = context.getResource<SqlQueryResource>("SqlQueryResource");
    KERNEL_REQUIRES(queryResource, "get sql query resource failed");
    SqlBizResource *bizResource = context.getResource<SqlBizResource>("SqlBizResource");
    KERNEL_REQUIRES(bizResource, "get sql biz resource failed");
    autil::mem_pool::Pool *pool = queryResource->getPool();
    KERNEL_REQUIRES(pool, "get pool failed");
    _sqlSearchInfoCollector = queryResource->getSqlSearchInfoCollector();
    _queryMetricsReporter = queryResource->getQueryMetricsReporter();

    auto memoryPoolResource = context.getResource<navi::MemoryPoolResource>(
            navi::RESOURCE_MEMORY_POOL_URI);
    _calcTable.reset(new CalcTable(pool, memoryPoolResource,
                                   _outputFields, _outputFieldsType,
                                   queryResource->getTracer(),
                                   queryResource->getSuezCavaAllocator(),
                                   bizResource->getCavaPluginManager(),
                                   bizResource->getFunctionInterfaceCreator().get()));
    if (!_calcTable->init(_outputExprsJson, _conditionJson)) {
        return navi::EC_ABORT;
    }
    _calcInfo.set_kernelname(getKernelName());
    _calcInfo.set_nodename(getNodeName());
    if (_opId < 0) {
        string keyStr = getKernelName() + "__" + _conditionJson + "__"+
                        StringUtil::toString(_outputFields);
        _calcInfo.set_hashkey(autil::HashAlgorithm::hashString(keyStr.c_str(), keyStr.size(), 1));
    } else {
        _calcInfo.set_hashkey(_opId);
    }
    return navi::EC_NONE;
}

navi::ErrorCode CalcKernel::compute(KernelComputeContext &ctx) {
    incComputeTimes();
    uint64_t beginTime = TimeUtility::currentTime();
    navi::PortIndex inputIndex(0, navi::INVALID_INDEX);
    bool isEof = false;
    navi::DataPtr data;
    ctx.getInput(inputIndex, data, isEof);
    TablePtr table;
    if (data) {
        table = dynamic_pointer_cast<Table>(data);
        if (table == nullptr) {
            SQL_LOG(ERROR, "invalid input table");
            return EC_ABORT;
        }
        if (!_calcTable->calcTable(table)) {
            SQL_LOG(ERROR, "filter table error.");
            return EC_ABORT;
        }
        if (!table) {
            return EC_ABORT;
        }
        incOutputCount(table->getRowCount());
    }

    uint64_t totalComputeTimes = _calcInfo.totalcomputetimes();
    if (totalComputeTimes < 5) {
        SQL_LOG(TRACE3, "calc batch [%lu] output table: [%s]",
                totalComputeTimes, TableUtil::toString(table, 10).c_str());
    }

    navi::PortIndex outputIndex(0, navi::INVALID_INDEX);
    ctx.setOutput(outputIndex, table, isEof);
    uint64_t endTime = TimeUtility::currentTime();
    incTotalTime(endTime - beginTime);

    if (isEof) {
        if (_sqlSearchInfoCollector) {
            _sqlSearchInfoCollector->addCalcInfo(_calcInfo);
        }
        reportMetrics();
        SQL_LOG(DEBUG, "calc info: [%s]", _calcInfo.ShortDebugString().c_str());
    }
    return EC_NONE;
}

void CalcKernel::reportMetrics() {
    if (_queryMetricsReporter != nullptr) {
        string pathName = "sql.user.ops." + getKernelName();
        auto opMetricReporter = _queryMetricsReporter->getSubReporter(pathName, {});
        opMetricReporter->report<CalcOpMetrics, CalcInfo>(nullptr, &_calcInfo);
    }
}

void CalcKernel::incTotalTime(int64_t time) {
    _calcInfo.set_totalusetime(_calcInfo.totalusetime() + time);
}

void CalcKernel::incOutputCount(int64_t count) {
    _calcInfo.set_totaloutputcount(_calcInfo.totaloutputcount() + count);
}

void CalcKernel::incComputeTimes() {
    _calcInfo.set_totalcomputetimes(_calcInfo.totalcomputetimes() + 1);
}


REGISTER_KERNEL(CalcKernel);

END_HA3_NAMESPACE(sql);
