#include <ha3/sql/ops/tvf/TvfKernel.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/ops/util/KernelRegister.h>
#include <ha3/sql/ops/util/KernelUtil.h>
#include <ha3/sql/resource/SqlQueryResource.h>
#include <ha3/sql/resource/SqlBizResource.h>
#include <ha3/turing/ops/SqlSessionResource.h>
#include <autil/TimeUtility.h>
#include <autil/StringUtil.h>
#include <autil/legacy/RapidJsonCommon.h>
#include <autil/legacy/RapidJsonHelper.h>
#include <ha3/sql/ops/util/SqlJsonUtil.h>
#include "suez/turing/expression/framework/MatchDocsExpressionCreator.h"

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace navi;
using namespace suez::turing;
using namespace autil;
using namespace kmonitor;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, InvocationAttr);
HA3_LOG_SETUP(sql, TvfKernel);
static const string TVF_TABLE_PARAM_PREFIX = "@table#";

class TvfOpMetrics : public MetricsGroup
{
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_GAUGE_MUTABLE_METRIC(_totalComputeTimes, "TotalComputeTimes");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalOutputCount, "TotalOutputCount");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalUsedTime, "TotalUsedTime");
        return true;
    }
    void report(const kmonitor::MetricsTags *tags, TvfInfo *tvfInfo) {
        REPORT_MUTABLE_METRIC(_totalComputeTimes, tvfInfo->totalcomputetimes());
        REPORT_MUTABLE_METRIC(_totalOutputCount, tvfInfo->totaloutputcount());
        REPORT_MUTABLE_METRIC(_totalUsedTime, tvfInfo->totalusetime() / 1000);
    }
private:
    MutableMetric *_totalComputeTimes = nullptr;
    MutableMetric *_totalOutputCount = nullptr;
    MutableMetric *_totalUsedTime = nullptr;
};

void InvocationAttr::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    json.Jsonize("op", tvfName, tvfName);
    json.Jsonize("type", type, type);
    KernelUtil::toJsonString(json, "params", tvfParams);
}

bool InvocationAttr::parseStrParams(vector<string> &paramVec,
                                    vector<string> &inputTables)
{
    try {
        autil::legacy::json::JsonArray paramsArray;
        FromJsonString(paramsArray, tvfParams);
        for (auto &paramAny : paramsArray) {
            if (autil::legacy::json::IsJsonString(paramAny)) {
                string param = *autil::legacy::AnyCast<std::string>(&paramAny);
                if (param.find(TVF_TABLE_PARAM_PREFIX) == 0) {
                    inputTables.push_back(param.substr(TVF_TABLE_PARAM_PREFIX.size()));
                } else {
                    paramVec.push_back(param);
                }
            } else if (autil::legacy::json::IsJsonMap(paramAny)) {
                continue;
            } else{
                return false;
            }
        }
    } catch (const autil::legacy::ExceptionBase &e) {
        SQL_LOG(ERROR, "parse tvf param fail. [%s].", e.what());
        return false;
    } catch (...) {
        return false;
    }
    return true;
}

TvfKernel::TvfKernel()
    : _queryMetricsReporter(nullptr)
    , _sqlSearchInfoCollector(nullptr)
    , _queryPool(nullptr)
    , _poolResource(nullptr)
    , _opId(-1)
{}

TvfKernel::~TvfKernel() {
}

const KernelDef *TvfKernel::getDef() const {
    auto def = new navi::KernelDef();
    def->set_kernel_name("TvfKernel");
    KERNEL_REGISTER_ADD_INPUT(def, "input0");
    KERNEL_REGISTER_ADD_OUTPUT(def, "output0");
    return def;
}

bool TvfKernel::config(navi::KernelConfigContext &ctx) {
    auto &json = ctx.getJsonAttrs();
    json.Jsonize("invocation", _invocation, _invocation);
    json.Jsonize("output_fields", _outputFields, _outputFields);
    json.Jsonize("output_fields_type", _outputFieldsType, _outputFieldsType);
    string str;
    json.Jsonize("scope", str, str);
    if (str == "FINAL") {
        _scope = KernelScope::FINAL;
    } else if (str == "PARTIAL") {
        _scope = KernelScope::PARTIAL;
    } else if (str == "NORMAL") {
        _scope = KernelScope::NORMAL;
    } else {
        _scope = KernelScope::UNKNOWN;
    }
    json.Jsonize(IQUAN_OP_ID, _opId, _opId);
    KernelUtil::stripName(_outputFields);
    if (_invocation.type != "TVF" || _invocation.tvfParams.size() == 0) {
        return false;
    }
    return true;
}

navi::ErrorCode TvfKernel::init(navi::KernelInitContext& context) {
    SqlQueryResource *queryResource = context.getResource<SqlQueryResource>("SqlQueryResource");
    KERNEL_REQUIRES(queryResource, "get sql query resource failed");
    SqlBizResource *bizResource = context.getResource<SqlBizResource>("SqlBizResource");
    KERNEL_REQUIRES(bizResource, "get sql biz resource failed");
    auto sqlSessionResource =
        bizResource->getObject<turing::SqlSessionResource>("SqlSessionResource");
    KERNEL_REQUIRES(sqlSessionResource, "get sql session resource failed");
    _tvfFuncManager = sqlSessionResource->tvfFuncManager.get();
    KERNEL_REQUIRES(_tvfFuncManager, "get agg Func Manager failed");
    _queryPool = queryResource->getPool();
    KERNEL_REQUIRES(_queryPool, "get query pool failed");
    _sqlSearchInfoCollector = queryResource->getSqlSearchInfoCollector();
    _queryMetricsReporter = queryResource->getQueryMetricsReporter();

    _poolResource = context.getResource<navi::MemoryPoolResource>(navi::RESOURCE_MEMORY_POOL_URI);
    KERNEL_REQUIRES(_poolResource, "get pool Resource failed");
    _tvfFunc.reset(_tvfFuncManager->createTvfFunction(_invocation.tvfName));
    if (_tvfFunc == nullptr) {
        SQL_LOG(ERROR, "create tvf funcition [%s] failed.", _invocation.tvfName.c_str());
        return navi::EC_ABORT;
    }
    _tvfFunc->setScope(_scope);
    TvfFuncInitContext tvfContext;
    tvfContext.outputFields = _outputFields;
    tvfContext.outputFieldsType = _outputFieldsType;
    vector<string> inputTables;
    if (!_invocation.parseStrParams(tvfContext.params, inputTables)) {
        SQL_LOG(ERROR, "parse tvf param [%s] error.", _invocation.tvfParams.c_str());
        return navi::EC_ABORT;
    }
    tvfContext.tvfResourceContainer = _tvfFuncManager->getTvfResourceContainer();
    tvfContext.queryPool = _queryPool;
    tvfContext.poolResource = _poolResource;
    tvfContext.queryResource = queryResource;
    tvfContext.queryInfo = &sqlSessionResource->queryInfo;
    if (!_tvfFunc->init(tvfContext)) {
        SQL_LOG(ERROR, "init tvf function [%s] failed.", _invocation.tvfName.c_str());
        return navi::EC_ABORT;
    }

    _tvfInfo.set_kernelname(getKernelName() + "__"+ _invocation.tvfName);
    _tvfInfo.set_nodename(getNodeName());
    if (_opId < 0) {
        string keyStr = getKernelName() + "__"+ _invocation.tvfName+ "__"
                        + StringUtil::toString(_outputFields);
        _tvfInfo.set_hashkey(autil::HashAlgorithm::hashString(keyStr.c_str(), keyStr.size(), 1));
    } else {
        _tvfInfo.set_hashkey(_opId);
    }
    return navi::EC_NONE;
}

navi::ErrorCode TvfKernel::compute(KernelComputeContext &ctx) {
    incComputeTimes();
    uint64_t beginTime = TimeUtility::currentTime();
    navi::PortIndex inputIndex(0, navi::INVALID_INDEX);
    bool isEof = false;
    navi::DataPtr data;
    ctx.getInput(inputIndex, data, isEof);
    TablePtr inputTable;
    if (data) {
        inputTable = dynamic_pointer_cast<Table>(data);
        if (inputTable == nullptr) {
            SQL_LOG(ERROR, "invalid input table");
            return EC_ABORT;
        }
    }
    TablePtr outputTable;
    if (!_tvfFunc->compute(inputTable, isEof, outputTable)) {
        SQL_LOG(ERROR, "tvf [%s] compute failed.", _tvfFunc->getName().c_str());
        return EC_ABORT;
    }
    if (isEof && inputTable != nullptr && outputTable == nullptr) {
        SQL_LOG(ERROR, "tvf [%s] output is empty.", _tvfFunc->getName().c_str());
        return EC_ABORT;
    }
    navi::PortIndex outputIndex(0, navi::INVALID_INDEX);
    if (outputTable) {
        uint64_t totalComputeTimes = _tvfInfo.totalcomputetimes();
        if (totalComputeTimes < 5) {
            SQL_LOG(TRACE3, "tvf batch [%lu] output table: [%s]",
                    totalComputeTimes, TableUtil::toString(outputTable, 10).c_str());
        }
        if (!checkOutputTable(outputTable)) {
            SQL_LOG(ERROR, "tvf [%s] output table illegal.", _tvfFunc->getName().c_str());
            return EC_ABORT;
        }
        incOutputCount(outputTable->getRowCount());
        ctx.setOutput(outputIndex, outputTable, isEof);
        uint64_t endTime = TimeUtility::currentTime();
        incTotalTime(endTime - beginTime);
    } else {
        uint64_t endTime = TimeUtility::currentTime();
        incTotalTime(endTime - beginTime);
    }
    if (isEof) {
        if (_sqlSearchInfoCollector) {
            _sqlSearchInfoCollector->addTvfInfo(_tvfInfo);
        }
        reportMetrics();
        SQL_LOG(DEBUG, "tvf info: [%s]", _tvfInfo.ShortDebugString().c_str());
        ctx.setEof();
    }
    return EC_NONE;
}

bool TvfKernel::checkOutputTable(const TablePtr &table) {
    set<string> nameSet;
    size_t colCount = table->getColumnCount();
    for (size_t i = 0; i < colCount; i++) {
        nameSet.insert(table->getColumnName(i));
    }
    for (const auto& field: _outputFields) {
        if (nameSet.count(field) != 1) {
            SQL_LOG(ERROR, "output field [%s] not found.", field.c_str());
            return false;
        }
    }
    return true;
}

void TvfKernel::reportMetrics() {
    if (_queryMetricsReporter != nullptr) {
        string pathName = "sql.user.ops." + getKernelName();
        auto opMetricsReporter = _queryMetricsReporter->getSubReporter(pathName, {});
        opMetricsReporter->report<TvfOpMetrics, TvfInfo>(nullptr, &_tvfInfo);
    }
}


void TvfKernel::incTotalTime(int64_t time) {
    _tvfInfo.set_totalusetime(_tvfInfo.totalusetime() + time);
}

void TvfKernel::incOutputCount(int64_t count) {
    _tvfInfo.set_totaloutputcount(_tvfInfo.totaloutputcount() + count);
}

void TvfKernel::incComputeTimes() {
    _tvfInfo.set_totalcomputetimes(_tvfInfo.totalcomputetimes() + 1);
}


REGISTER_KERNEL(TvfKernel);

END_HA3_NAMESPACE(sql);
