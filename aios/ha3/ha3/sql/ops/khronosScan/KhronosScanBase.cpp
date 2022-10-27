#include <ha3/sql/ops/khronosScan/KhronosScanBase.h>
#include <ha3/sql/resource/SqlQueryResource.h>
#include <ha3/sql/ops/condition/ConditionParser.h>

using namespace khronos::search;
using namespace std;
using namespace kmonitor;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, KhronosScanBase);

bool KhronosScanBase::init(const ScanInitParam &param) {
    if (!ScanBase::init(param)) {
        SQL_LOG(ERROR, "init khronos scan base failed");
        return false;
    }

    SqlQueryResource *queryResource = param.queryResource;
    if (!queryResource) {
        SQL_LOG(ERROR, "get sql query resource failed.");
        return false;
    }

    if (_queryMetricsReporter != nullptr) {
        std::string pathName = "sql.user.ops." + _opName;
        _opMetricsReporter = _queryMetricsReporter->getSubReporter(pathName, {});
    }

    if (_timeoutTerminator) {
        _timeoutTerminator->init(1 << 8);
    }

    _tableReader = getKhronosTableInfo(
            queryResource->getPartitionReaderSnapshot(), _catalogName);
    if (_tableReader == nullptr) {
        SQL_LOG(ERROR,
                "get khronos table reader failed, "
                "catalogName: [%s], dbName: [%s], tableName[%s].",
                _catalogName.c_str(), _dbName.c_str(), _tableName.c_str());
        return false;
    }

    return true;
}

SearchInterfacePtr KhronosScanBase::getKhronosTableInfo(
        IE_NAMESPACE(partition)::PartitionReaderSnapshot *partitionReaderSnapshot,
        const std::string &tableName)
{
    SearchInterfacePtr khronosTableReader;
    IndexPartitionReaderPtr partitionReader =
        partitionReaderSnapshot->GetIndexPartitionReader(tableName);
    if (!partitionReader) {
        SQL_LOG(ERROR, "get index partition reader failed, table name: %s",
                tableName.c_str());
        return khronosTableReader;
    }
    auto tableReader = partitionReader->GetTableReader();
    khronosTableReader = std::tr1::dynamic_pointer_cast<SearchInterface>(tableReader);
    if (!khronosTableReader) {
        SQL_LOG(ERROR, "cast KhronosTableReader failed, table name: %s", tableName.c_str());
        return khronosTableReader;
    }
    return khronosTableReader;
}

bool KhronosScanBase::visitAndParseCondition(ConditionVisitor *visitor) {
    ConditionParser parser(_pool);
    ConditionPtr condition;
    SQL_LOG(TRACE2, "start parsing condition [%s]", _conditionJson.c_str());
    if (!parser.parseCondition(_conditionJson, condition)) {
        SQL_LOG(ERROR, "parse condition [%s] failed", _conditionJson.c_str());
        return false;
    }
    // select metric from meta
    if (condition == nullptr) {
        return true;
    }
    condition->accept(visitor);
    if (visitor->isError()) {
        SQL_LOG(ERROR, "visit condition [%s] failed: %s",
                _conditionJson.c_str(), visitor->errorInfo().c_str());
        return false;
    }
    return true;
}

void KhronosScanBase::reportSearchMetrics(bool success, const SearchMetric& searchMetric) {
    reportUserMetrics<SearchMetricsGroup>({{{"status", success ? "success" : "failed"}}}, &searchMetric);
}
void KhronosScanBase::reportSimpleQps(const string &metricName, kmonitor::MetricsTags tags) {
    tags.AddTag("catalog", _catalogName);
    REPORT_USER_MUTABLE_QPS_TAGS(_opMetricsReporter, metricName, &tags);
}

END_HA3_NAMESPACE(sql);
