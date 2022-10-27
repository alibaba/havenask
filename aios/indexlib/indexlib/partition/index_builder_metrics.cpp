#include "indexlib/partition/index_builder_metrics.h"
#include "indexlib/document/document.h"

using namespace std;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_USE(document);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, IndexBuilderMetrics);

IndexBuilderMetrics::IndexBuilderMetrics(MetricProviderPtr metricProvider)
    : mMetricProvider(metricProvider)
    , mSuccessDocCount(0)
    , mAddToUpdateDocCount(0)
    , mSchemaVersionMismatchCount(0)
{
}

void IndexBuilderMetrics::RegisterMetrics(TableType tableType)
{
    if (tableType == tt_kv || tableType == tt_kkv)
    {
        return;
    }

    IE_INIT_METRIC_GROUP(mMetricProvider, addToUpdateQps,
                         "build/addToUpdateQps", kmonitor::QPS, "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, addToUpdateDocCount,
                         "build/addToUpdateDocCount", kmonitor::STATUS, "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, schemaVersionMissMatchQps,
                         "build/schemaVersionMissMatchQps", kmonitor::QPS, "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, schemaVersionMissMatchDocCount,
                         "build/schemaVersionMissMatchDocCount", kmonitor::STATUS, "count");
}

void IndexBuilderMetrics::ReportSchemaVersionMismatch()
{
    mSchemaVersionMismatchCount++;
    IE_REPORT_METRIC(schemaVersionMissMatchDocCount, mSchemaVersionMismatchCount);
    IE_INCREASE_QPS(schemaVersionMissMatchQps);
}

void IndexBuilderMetrics::ReportMetrics(size_t consumedDocCount, const DocumentPtr& doc)
{
    if (!doc)
    {
        // TODO
        return;
    }
    if (consumedDocCount == 0)
    {
        return;
    }
    mSuccessDocCount += consumedDocCount;
    DocOperateType operation = doc->GetDocOperateType();
    switch (operation)
    {
        case ADD_DOC:
        case DELETE_DOC:
        case DELETE_SUB_DOC:
        case SKIP_DOC: 
        {
            // TODO
            break;
        }
        case UPDATE_FIELD:
        {
            ReportUpdateField(consumedDocCount, doc);
            break;
        }
        default:
        {
            break;
        }
    }
}

void IndexBuilderMetrics::ReportUpdateField(size_t consumedDocCount, const DocumentPtr& doc)
{
    assert(doc->GetMessageCount() == 1u); // only support buildin table
    assert(consumedDocCount <= 1);
    DocOperateType originalOperation = doc->GetOriginalOperateType();
    if (originalOperation == ADD_DOC && consumedDocCount > 0)
    {
        mAddToUpdateDocCount++;
        IE_REPORT_METRIC(addToUpdateDocCount, mAddToUpdateDocCount);
        IE_INCREASE_QPS(addToUpdateQps);
    }
}

IE_NAMESPACE_END(monitor);

