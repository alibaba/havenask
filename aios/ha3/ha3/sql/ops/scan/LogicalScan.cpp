#include <ha3/sql/ops/scan/LogicalScan.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/ops/util/KernelUtil.h>
#include <ha3/sql/ops/condition/ExprUtil.h>
#include <autil/TimeUtility.h>

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, LogicalScan);

LogicalScan::LogicalScan() {
}

LogicalScan::~LogicalScan() {
}

bool LogicalScan::init(const ScanInitParam &param) {
    uint64_t initBegin = TimeUtility::currentTime();
    _enableTableInfo = false;
    if (!ScanBase::init(param)) {
        return false;
    }

    uint64_t initEnd = TimeUtility::currentTime();
    incInitTime(initEnd - initBegin);
    return true;
}

bool LogicalScan::doBatchScan(TablePtr &table, bool &eof) {
    eof = true;
    uint64_t afterSeek = TimeUtility::currentTime();
    incSeekTime(afterSeek - _batchScanBeginTime);
    uint64_t afterEvaluate = TimeUtility::currentTime();
    incEvaluateTime(afterEvaluate - afterSeek);
    table = createTable();
    int64_t endTime = TimeUtility::currentTime();
    incOutputTime(endTime - afterEvaluate);
    return true;
}

TablePtr LogicalScan::createTable() {
    TablePtr table;
    table.reset(new Table(_memoryPoolResource->getPool()));
    for (size_t i = 0; i < _outputFields.size(); i++) {
        auto bt = ExprUtil::transSqlTypeToVariableType(_outputFieldsType[i]);
        table->declareColumn(_outputFields[i], bt, false);
    }
    return table;
}

END_HA3_NAMESPACE(sql);