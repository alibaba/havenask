#include <ha3/sql/ops/limit/LimitKernel.h>
#include <ha3/sql/resource/SqlQueryResource.h>
#include <ha3/sql/ops/util/KernelRegister.h>
#include <ha3/sql/ops/util/KernelUtil.h>

using namespace std;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, LimitKernel);

LimitKernel::LimitKernel()
    : _limit(0)
    , _offset(0)
    , _topk(_offset + _limit)
    , _queryMetricsReporter(nullptr)
    , _outputCount(0)
{
}

LimitKernel::~LimitKernel() {
    reportMetrics();
}

// kernel def, see: navi/sql/executor/proto/KernelDef.proto
const navi::KernelDef *LimitKernel::getDef() const {
    auto def = new navi::KernelDef();
    def->set_kernel_name("LimitKernel");
    KERNEL_REGISTER_ADD_INPUT(def, "input0");
    KERNEL_REGISTER_ADD_OUTPUT(def, "output0");
    return def;
}

bool LimitKernel::config(navi::KernelConfigContext &ctx) {
    ctx.getJsonAttrs().Jsonize("limit", _limit);
    ctx.getJsonAttrs().Jsonize("offset", _offset, _offset);
    _topk = _offset + _limit;
    return true;
}

navi::ErrorCode LimitKernel::init(navi::KernelInitContext& context) {
    SqlQueryResource* queryResource = context.getResource<SqlQueryResource>("SqlQueryResource");
    if (queryResource == nullptr) {
        SQL_LOG(ERROR, "get sql query resource failed");
        return navi::EC_INIT_GRAPH;
    }
    _queryMetricsReporter = queryResource->getQueryMetricsReporter();
    return navi::EC_NONE;
}

navi::ErrorCode LimitKernel::compute(navi::KernelComputeContext &runContext) {
    navi::PortIndex inputIndex(0, navi::INVALID_INDEX);
    bool eof = false;
    navi::DataPtr data;
    runContext.getInput(inputIndex, data, eof);
    if (data != nullptr) {
        if (!doCompute(data)) {
            SQL_LOG(ERROR, "compute limit failed");
            return navi::EC_ABORT;
        }
        if (_table->getRowCount() >= _topk) {
            outputResult(runContext);
            return navi::EC_NONE;
        }
    }
    if (eof) {
        outputResult(runContext);
    }
    return navi::EC_NONE;
}

bool LimitKernel::doCompute(const navi::DataPtr &data) {
    TablePtr input = dynamic_pointer_cast<Table>(data);
    if (input == nullptr) {
        SQL_LOG(ERROR, "invalid input table");
        return false;
    }
    if (_table == nullptr) {
        _table = input;
    } else {
        if (!_table->merge(input)) {
            SQL_LOG(ERROR, "merge input table failed");
            return false;
        }
    }
    if (_table->getRowCount() > _topk) {
        for (size_t i = _topk; i < _table->getRowCount(); ++i) {
            _table->markDeleteRow(i);
        }
        _table->deleteRows();
    }
    return true;
}

void LimitKernel::outputResult(navi::KernelComputeContext &runContext) {
    if (_table != nullptr && _offset > 0) {
        size_t offset = std::min(_offset, _table->getRowCount());
        for (size_t i = 0; i < offset; i++) {
            _table->markDeleteRow(i);
        }
        _table->deleteRows();
        _outputCount = _table->getRowCount();
    }
    navi::PortIndex outputIndex(0, navi::INVALID_INDEX);
    runContext.setOutput(outputIndex, _table, true);
}

void LimitKernel::reportMetrics() {
    if (_queryMetricsReporter != nullptr) {
        string pathName = "sql.user.ops." + getKernelName();
        auto opMetricReporter = _queryMetricsReporter->getSubReporter(pathName, {});
        REPORT_USER_MUTABLE_METRIC(opMetricReporter, "output_count", _outputCount);
    }
}

REGISTER_KERNEL(LimitKernel);

END_HA3_NAMESPACE(sql);
