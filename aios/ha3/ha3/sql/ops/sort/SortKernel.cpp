#include <ha3/sql/ops/sort/SortKernel.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/resource/SqlQueryResource.h>
#include <ha3/sql/ops/util/KernelRegister.h>
#include <ha3/sql/ops/util/KernelUtil.h>
#include <ha3/sql/rank/ComparatorCreator.h>

using namespace std;
using namespace autil;
using namespace kmonitor;

BEGIN_HA3_NAMESPACE(sql);

HA3_LOG_SETUP(sql, SortKernel);

class SortOpMetrics : public MetricsGroup
{
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_GAUGE_MUTABLE_METRIC(_totalComputeTimes, "TotalComputeTimes");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalSortTime, "TotalSortTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalMergeTime, "TotalMergeTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalTopKTime, "TotalTopK");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalCompactTime, "totalCompactTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_outputTime, "OutputTime");
        return true;
    }
    void report(const kmonitor::MetricsTags *tags, SortInfo *sortInfo) {
        REPORT_MUTABLE_METRIC(_totalComputeTimes, sortInfo->totalcomputetimes());
        REPORT_MUTABLE_METRIC(_totalSortTime, sortInfo->totalusetime() / 1000);
        REPORT_MUTABLE_METRIC(_totalMergeTime, sortInfo->totalmergetime() / 1000);
        REPORT_MUTABLE_METRIC(_totalTopKTime, sortInfo->totaltopktime() / 1000);
        REPORT_MUTABLE_METRIC(_totalCompactTime, sortInfo->totalcompacttime() / 1000);
        REPORT_MUTABLE_METRIC(_outputTime, sortInfo->totaloutputtime() / 1000);
    }
private:
    MutableMetric *_totalComputeTimes = nullptr;
    MutableMetric *_totalSortTime = nullptr;
    MutableMetric *_totalMergeTime = nullptr;
    MutableMetric *_totalTopKTime = nullptr;
    MutableMetric *_totalCompactTime = nullptr;
    MutableMetric *_outputTime = nullptr;
};

SortKernel::SortKernel()
    : _limit(0)
    , _offset(0)
    , _topk(_limit + _offset)
    , _pool(nullptr)
    , _queryMetricsReporter(nullptr)
    , _sqlSearchInfoCollector(nullptr)
    , _opId(-1)
{
}

SortKernel::~SortKernel() {
    reportMetrics();
}

// kernel def, see: navi/sql/executor/proto/KernelDef.proto
const navi::KernelDef *SortKernel::getDef() const {
    auto def = new navi::KernelDef();
    def->set_kernel_name("SortKernel");
    KERNEL_REGISTER_ADD_INPUT(def, "input0");
    KERNEL_REGISTER_ADD_OUTPUT(def, "output0");
    return def;
}

bool SortKernel::config(navi::KernelConfigContext &ctx) {
    auto &json = ctx.getJsonAttrs();
    json.Jsonize("order_fields", _keys);
    json.Jsonize(IQUAN_OP_ID, _opId, _opId);
    KernelUtil::stripName(_keys);
    vector<string> orderStrVec;
    json.Jsonize("directions", orderStrVec);
    if (_keys.size() != orderStrVec.size()) {
        SQL_LOG(ERROR, "sort key & order size mismatch, "
                        "key size [%lu], order size [%lu]",
                        _keys.size(), orderStrVec.size());
        return false;
    }
    if (_keys.empty()) {
        SQL_LOG(ERROR, "sort key empty");
        return false;
    }
    for (const string &order : orderStrVec) {
        _orders.emplace_back(order == "DESC");
    }
    json.Jsonize("limit", _limit);
    json.Jsonize("offset", _offset);
    _topk = _offset + _limit;
    return true;
}

navi::ErrorCode SortKernel::init(navi::KernelInitContext& context) {
    _sortInfo.set_kernelname(getKernelName());
    _sortInfo.set_nodename(getNodeName());
    if (_opId < 0) {
        string keyStr = getKernelName() + "__" + StringUtil::toString(_keys) + "__" + StringUtil::toString(_orders);
        _sortInfo.set_hashkey(autil::HashAlgorithm::hashString(keyStr.c_str(), keyStr.size(), 1));
    } else {
        _sortInfo.set_hashkey(_opId);
    }
    SqlQueryResource* queryResource = context.getResource<SqlQueryResource>("SqlQueryResource");
    KERNEL_REQUIRES(queryResource, "get sql query resource failed");
    _pool = queryResource->getPool();
    KERNEL_REQUIRES(_pool, "get pool failed");
    _sqlSearchInfoCollector = queryResource->getSqlSearchInfoCollector();
    _queryMetricsReporter = queryResource->getQueryMetricsReporter();
    return navi::EC_NONE;
}

navi::ErrorCode SortKernel::compute(navi::KernelComputeContext &runContext) {
    incComputeTime();
    uint64_t beginTime = TimeUtility::currentTime();
    navi::PortIndex inputIndex(0, navi::INVALID_INDEX);
    bool eof = false;
    navi::DataPtr data;
    runContext.getInput(inputIndex, data, eof);
    if (data != nullptr) {
        if (!doCompute(data)) {
            SQL_LOG(ERROR, "compute sort limit failed");
            return navi::EC_ABORT;
        }
    }
    incTotalTime(TimeUtility::currentTime() - beginTime);
    if (eof) {
        outputResult(runContext);
        SQL_LOG(DEBUG, "sort info: [%s]", _sortInfo.ShortDebugString().c_str());
    }
    return navi::EC_NONE;
}

void SortKernel::outputResult(navi::KernelComputeContext &runContext) {
    uint64_t beginTime = TimeUtility::currentTime();
    navi::PortIndex outputIndex(0, navi::INVALID_INDEX);
    if (_comparator != nullptr && _table != nullptr) {
        TableUtil::sort(_table, _comparator.get());
        if (_offset > 0) {
            size_t offset = std::min(_offset, _table->getRowCount());
            for (size_t i = 0; i < offset; i++) {
                _table->markDeleteRow(i);
            }
            _table->deleteRows();
        }
    }
    SQL_LOG(TRACE3, "sort output table: [%s]", TableUtil::toString(_table, 10).c_str());
    runContext.setOutput(outputIndex, _table, true);
    incOutputTime(TimeUtility::currentTime() - beginTime);
    if (_sqlSearchInfoCollector) {
        _sqlSearchInfoCollector->addSortInfo(_sortInfo);
    }
}

bool SortKernel::doCompute(const navi::DataPtr &data) {
    uint64_t beginTime = TimeUtility::currentTime();
    TablePtr input = dynamic_pointer_cast<Table>(data);
    if (input == nullptr) {
        SQL_LOG(ERROR, "invalid input table");
        return false;
    }
    if (_comparator == nullptr) {
        _table = input;
        _comparator = ComparatorCreator::createComboComparator(_table, _keys, _orders, _pool);
        if (_comparator == nullptr) {
            SQL_LOG(ERROR, "init combo comparator failed");
            return false;
        }
    } else {
        if (!_table->merge(input)) {
            SQL_LOG(ERROR, "merge input table failed");
            return false;
        }
    }
    uint64_t afterMergeTime = TimeUtility::currentTime();
    incMergeTime(afterMergeTime - beginTime);
    TableUtil::topK(_table, _comparator.get(), _topk);
    uint64_t afterTopKTime = TimeUtility::currentTime();
    incTopKTime(afterTopKTime - afterMergeTime);
    _table->compact();
    incCompactTime(TimeUtility::currentTime() - afterTopKTime);
    SQL_LOG(TRACE3, "sort-topk output table: [%s]", TableUtil::toString(_table, 10).c_str());
    return true;
}

void SortKernel::reportMetrics() {
    if (_queryMetricsReporter != nullptr) {
        string pathName = "sql.user.ops." + getKernelName();
        auto opMetricsReporter =
            _queryMetricsReporter->getSubReporter(pathName, {});
        opMetricsReporter->report<SortOpMetrics, SortInfo>(nullptr, &_sortInfo);
    }
}

void SortKernel::incComputeTime() {
    _sortInfo.set_totalcomputetimes(_sortInfo.totalcomputetimes() + 1);
}
void SortKernel::incMergeTime(int64_t time) {
    _sortInfo.set_totalmergetime(_sortInfo.totalmergetime() + time);
}

void SortKernel::incTopKTime(int64_t time) {
    _sortInfo.set_totaltopktime(_sortInfo.totaltopktime() + time);
}

void SortKernel::incCompactTime(int64_t time) {
    _sortInfo.set_totalcompacttime(_sortInfo.totalcompacttime() + time);
}

void SortKernel::incOutputTime(int64_t time) {
    _sortInfo.set_totaloutputtime(_sortInfo.totaloutputtime() + time);
}

void SortKernel::incTotalTime(int64_t time) {
    _sortInfo.set_totalusetime(_sortInfo.totalusetime() + time);
}


REGISTER_KERNEL(SortKernel);

END_HA3_NAMESPACE(sql);
