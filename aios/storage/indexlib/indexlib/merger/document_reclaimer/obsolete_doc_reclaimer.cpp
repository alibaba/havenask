/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/merger/document_reclaimer/obsolete_doc_reclaimer.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <string>
#include <vector>

#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "beeper/beeper.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/common/AttributeValueTypeTraits.h"
#include "indexlib/index/common/numeric_compress/EquivalentCompressSessionReader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_iterator.h"
#include "indexlib/index/normal/deletionmap/deletion_map_writer.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/document_reclaimer/document_deleter.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/util/metrics/Metric.h"
#include "kmonitor/client/MetricLevel.h"
#include "kmonitor/client/MetricType.h"
#include "kmonitor/client/core/MetricsTags.h"

using namespace std;
using namespace autil;
using namespace kmonitor;

using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, ObsoleteDocReclaimer);

ObsoleteDocReclaimer::ObsoleteDocReclaimer(const IndexPartitionSchemaPtr& schema,
                                           util::MetricProviderPtr metricProvider)
    : mSchema(schema)
    , mMetricProvider(metricProvider)
{
    mCurrentTime = TimeUtility::currentTimeInSeconds();
    IE_INIT_METRIC_GROUP(mMetricProvider, obsoleteDoc, "docReclaim/obsoleteDoc", kmonitor::GAUGE, "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, obsoleteSegment, "docReclaim/obsoleteSegment", kmonitor::GAUGE, "count");
}

ObsoleteDocReclaimer::~ObsoleteDocReclaimer() {}

void ObsoleteDocReclaimer::Reclaim(const DocumentDeleterPtr& docDeleter, const merger::SegmentDirectoryPtr& segDir)
{
    string ttlFieldName = mSchema->GetTTLFieldName();
    auto attrConfig = mSchema->GetAttributeSchema()->GetAttributeConfig(ttlFieldName);
    assert(attrConfig);
    PartitionDataPtr partitionData = segDir->GetPartitionData();
    for (auto iter = partitionData->Begin(); iter != partitionData->End(); iter++) {
        segmentid_t segmentId = iter->GetSegmentId();
        exdocid_t baseDocid = iter->GetBaseDocId();
        SingleValueDataIterator<uint32_t> attrIterator(attrConfig);
        attrIterator.Init(partitionData, segmentId);
        auto delCount = DeleteObsoleteDocs(attrIterator, baseDocid, docDeleter);
        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                                          "ObsoleteDocReclaimer delete [%lu] docs in segment [%d]", delCount,
                                          segmentId);
        MetricsTags tags;
        tags.AddTag("segment", StringUtil::toString(segmentId));
        IE_REPORT_METRIC_WITH_TAGS(obsoleteDoc, &tags, delCount);
    }
}

size_t ObsoleteDocReclaimer::DeleteObsoleteDocs(SingleValueDataIterator<uint32_t>& iterator, exdocid_t baseDocid,
                                                const DocumentDeleterPtr& docDeleter)
{
    size_t count = 0;
    exdocid_t innerDocid = 0;
    while (iterator.IsValid()) {
        uint32_t timestamp = iterator.GetValue();
        if (timestamp < mCurrentTime) {
            docDeleter->Delete(innerDocid + baseDocid);
            ++count;
        }
        innerDocid++;
        iterator.MoveToNext();
    }
    return count;
}

void ObsoleteDocReclaimer::RemoveObsoleteSegments(const merger::SegmentDirectoryPtr& segDir)
{
    PartitionDataPtr partitionData = segDir->GetPartitionData();
    for (auto iter = partitionData->Begin(); iter != partitionData->End(); iter++) {
        const std::shared_ptr<const SegmentInfo>& segmentInfo = iter->GetSegmentInfo();
        if (segmentInfo->maxTTL < mCurrentTime && segmentInfo->docCount != 0) {
            segDir->RemoveSegment(iter->GetSegmentId());
            BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                                              "ObsoleteDocReclaimer remove segment [%d] for over maxTTL [%u]",
                                              iter->GetSegmentId(), segmentInfo->maxTTL);
            MetricsTags tags;
            tags.AddTag("segment", StringUtil::toString(iter->GetSegmentId()));
            tags.AddTag("maxTTL", StringUtil::toString(segmentInfo->maxTTL));
            IE_REPORT_METRIC_WITH_TAGS(obsoleteSegment, &tags, 1);
        }
    }
    segDir->CommitVersion();
}
}} // namespace indexlib::merger
