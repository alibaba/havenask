#include "indexlib/merger/document_reclaimer/obsolete_doc_reclaimer.h"
#include "indexlib/merger/document_reclaimer/document_deleter.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/config/index_partition_schema.h"
#include <autil/TimeUtility.h>
#include <beeper/beeper.h>

using namespace std;
using namespace autil;
using namespace kmonitor;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, ObsoleteDocReclaimer);

ObsoleteDocReclaimer::ObsoleteDocReclaimer(const IndexPartitionSchemaPtr& schema,
                                           misc::MetricProviderPtr metricProvider)
    : mSchema(schema)
    , mMetricProvider(metricProvider)
{
    mCurrentTime = TimeUtility::currentTimeInSeconds();
    IE_INIT_METRIC_GROUP(mMetricProvider, obsoleteDoc,
                         "docReclaim/obsoleteDoc", kmonitor::GAUGE, "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, obsoleteSegment,
                         "docReclaim/obsoleteSegment", kmonitor::GAUGE, "count");
}

ObsoleteDocReclaimer::~ObsoleteDocReclaimer() 
{
}

void ObsoleteDocReclaimer::Reclaim(const DocumentDeleterPtr& docDeleter,
                                   const merger::SegmentDirectoryPtr& segDir)
{
    string ttlFieldName = mSchema->GetTTLFieldName();
    auto attrConfig =
        mSchema->GetAttributeSchema()->GetAttributeConfig(ttlFieldName);
    assert(attrConfig);
    PartitionDataPtr partitionData = segDir->GetPartitionData();
    for (auto iter = partitionData->Begin(); iter != partitionData->End(); iter++)
    {
        segmentid_t segmentId = iter->GetSegmentId();
        exdocid_t baseDocid = iter->GetBaseDocId();
        SingleValueDataIterator<uint32_t> attrIterator(attrConfig);
        attrIterator.Init(partitionData, segmentId);
        auto delCount = DeleteObsoleteDocs(attrIterator, baseDocid, docDeleter);
        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                "ObsoleteDocReclaimer delete [%lu] docs in segment [%d]", delCount, segmentId);
        MetricsTags tags;
        tags.AddTag("segment", StringUtil::toString(segmentId));
        IE_REPORT_METRIC_WITH_TAGS(obsoleteDoc, &tags, delCount);
    }
}

size_t ObsoleteDocReclaimer::DeleteObsoleteDocs(
    SingleValueDataIterator<uint32_t>& iterator,
    exdocid_t baseDocid, const DocumentDeleterPtr& docDeleter)
{
    size_t count = 0;
    exdocid_t innerDocid = 0;
    while (iterator.IsValid())
    {
        uint32_t timestamp = iterator.GetValue();
        if (timestamp < mCurrentTime)
        {
            docDeleter->Delete(innerDocid + baseDocid);
            ++count;
        }
        innerDocid++;
        iterator.MoveToNext();
    }
    return count;
}

void ObsoleteDocReclaimer::RemoveObsoleteSegments(
        const merger::SegmentDirectoryPtr& segDir)
{
    PartitionDataPtr partitionData = segDir->GetPartitionData();
    for (auto iter = partitionData->Begin(); iter != partitionData->End(); iter++)
    {
        auto segmentInfo = iter->GetSegmentInfo();
        if (segmentInfo.maxTTL < mCurrentTime &&
            segmentInfo.docCount != 0)
        {
            segDir->RemoveSegment(iter->GetSegmentId());
            BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                    "ObsoleteDocReclaimer remove segment [%d] for over maxTTL [%u]",
                    iter->GetSegmentId(), segmentInfo.maxTTL);
            MetricsTags tags;
            tags.AddTag("segment", StringUtil::toString(iter->GetSegmentId()));
            tags.AddTag("maxTTL", StringUtil::toString(segmentInfo.maxTTL));
            IE_REPORT_METRIC_WITH_TAGS(obsoleteSegment, &tags, 1);
        }
    }
    segDir->CommitVersion();
}

IE_NAMESPACE_END(merger);

