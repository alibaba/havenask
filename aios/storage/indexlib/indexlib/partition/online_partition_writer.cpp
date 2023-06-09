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
#include "indexlib/partition/online_partition_writer.h"

#include "autil/TimeUtility.h"
#include "indexlib/document/document.h"
#include "indexlib/document/document_collector.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/online_join_policy.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/util/KeyHasherTyped.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::document;
using namespace indexlib::util;
using namespace indexlib::index_base;

DECLARE_REFERENCE_CLASS(partition, FlushedLocatorContainer);

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OnlinePartitionWriter);

#define IE_LOG_PREFIX mPartitionName.c_str()

OnlinePartitionWriter::OnlinePartitionWriter(const IndexPartitionOptions& options,
                                             const DumpSegmentContainerPtr& container,
                                             OnlinePartitionMetrics* onlinePartMetrics, const string& partitionName,
                                             document::SrcSignature src)
    : PartitionWriter(options, partitionName)
    , mDumpSegmentContainer(container)
    , mRtFilterTimestamp(INVALID_TIMESTAMP)
    , mLastDumpTs(INVALID_TIMESTAMP)
    , mOnlinePartMetrics(onlinePartMetrics)
    , mIsClosed(true)
    , mSrcSignature(src)
{
    mLastDumpTs = TimeUtility::currentTimeInSeconds();
    if (options.GetOnlineConfig().EnableIntervalDump()) {
        int64_t shuffleTime = GetShuffleTime(options, mLastDumpTs);
        IE_PREFIX_LOG(INFO, "OnlinePartitionWriter use shuffleTime: [%ld] seconds", shuffleTime);
        mLastDumpTs -= shuffleTime;
    }
}

OnlinePartitionWriter::~OnlinePartitionWriter() {}

// for open and inc data open
void OnlinePartitionWriter::Open(const IndexPartitionSchemaPtr& schema, const PartitionDataPtr& partitionData,
                                 const PartitionModifierPtr& modifier)
{
    autil::ScopedTime2 timer;
    IE_PREFIX_LOG(INFO, "OnlinePartitionWriter open begin");
    assert(schema);
    mSchema = schema;

    mPartitionData = partitionData;

    MetricProviderPtr metricProvider = NULL;
    if (mOnlinePartMetrics) {
        metricProvider = mOnlinePartMetrics->GetMetricProvider();
    }

    ScopedLock lock(mWriterLock);
    mWriter.reset(new OfflinePartitionWriter(mOptions, mDumpSegmentContainer, FlushedLocatorContainerPtr(),
                                             metricProvider, mPartitionName));
    mWriter->Open(mSchema, partitionData, modifier);
    Version version = partitionData->GetOnDiskVersion();
    OnlineJoinPolicy joinPolicy(version, mSchema->GetTableType(), partitionData->GetSrcSignature());
    mRtFilterTimestamp = joinPolicy.GetRtFilterTimestamp();
    mIsClosed = false;
    IE_PREFIX_LOG(INFO, "OnlinePartitionWriter open end, used[%.3f]s", timer.done_sec());
}

void OnlinePartitionWriter::ReOpenNewSegment(const PartitionModifierPtr& modifier)
{
    mWriter->ReOpenNewSegment(modifier);
}

bool OnlinePartitionWriter::BuildDocument(const DocumentPtr& doc)
{
    assert(doc);
    mLastConsumedMessageCount = 0;

    if (!CheckTimestamp(doc)) {
        return false;
    }

    if (!mWriter->BuildDocument(doc)) {
        return false;
    }

    mLastConsumedMessageCount = mWriter->GetLastConsumedMessageCount();
    mPartitionData->UpdatePartitionInfo();
    return true;
}

bool OnlinePartitionWriter::BatchBuild(std::unique_ptr<document::DocumentCollector> docCollector)
{
    const std::vector<document::DocumentPtr>& docs = docCollector->GetDocuments();
    for (size_t i = 0; i < docs.size(); ++i) {
        if (!CheckTimestamp(docs[i])) {
            docCollector->LogicalDelete(i);
        }
    }

    bool res = mWriter->BatchBuild(std::move(docCollector));
    mLastConsumedMessageCount = mWriter->GetLastConsumedMessageCount();
    mPartitionData->UpdatePartitionInfo();
    return res;
}

PartitionWriter::BuildMode OnlinePartitionWriter::GetBuildMode() { return mWriter->GetBuildMode(); }

void OnlinePartitionWriter::SwitchBuildMode(PartitionWriter::BuildMode buildMode)
{
    mWriter->SwitchBuildMode(buildMode);
}

bool OnlinePartitionWriter::NeedDump(size_t memoryQuota, const DocumentCollector* docCollector) const
{
    return mWriter->NeedDump(memoryQuota, docCollector);
}

void OnlinePartitionWriter::EndIndex() { mWriter->EndIndex(); }

bool OnlinePartitionWriter::DumpSegmentWithMemLimit()
{
    IE_PREFIX_LOG(INFO, "dump segment begin");
    if (mWriter && mWriter->DumpSegmentWithMemLimit()) {
        // one segment with one version
        mLastDumpTs = TimeUtility::currentTimeInSeconds();
        IE_PREFIX_LOG(INFO, "dump segment end");
        return true;
    }
    IE_PREFIX_LOG(INFO, "dump segment fail, no writer or enough memory");
    return false;
}

void OnlinePartitionWriter::Reset()
{
    ScopedLock lock(mWriterLock);
    mIsClosed = true;
    MEMORY_BARRIER();
    mWriter.reset();
    mPartitionData.reset();
    mRtFilterTimestamp = INVALID_TIMESTAMP;
}

void OnlinePartitionWriter::DumpSegment()
{
    IE_PREFIX_LOG(INFO, "dump segment begin");
    if (mWriter) {
        // one segment with one version
        mWriter->DumpSegment();
        mLastDumpTs = TimeUtility::currentTimeInSeconds();
    }
    IE_PREFIX_LOG(INFO, "dump segment end");
}

void OnlinePartitionWriter::Close()
{
    ScopedLock lock(mWriterLock);
    DumpSegment();
    mIsClosed = true;
    MEMORY_BARRIER();
    mWriter.reset();
    mPartitionData.reset();
}

void OnlinePartitionWriter::CloseDiscardData()
{
    ScopedLock lock(mWriterLock);
    mIsClosed = true;
    MEMORY_BARRIER();
    mWriter.reset();
    mPartitionData.reset();
}

void OnlinePartitionWriter::RewriteDocument(const DocumentPtr& doc) { mWriter->RewriteDocument(doc); }

bool OnlinePartitionWriter::CheckTimestamp(const DocumentPtr& doc) const
{
    bool ret = doc->CheckOrTrim(mRtFilterTimestamp);
    if (!ret) {
        IE_PREFIX_LOG(DEBUG, "rt doc with ts[%ld], incTs[%ld], drop rt doc", doc->GetTimestamp(), mRtFilterTimestamp);
        if (mOnlinePartMetrics) {
            mOnlinePartMetrics->IncreateObsoleteDocQps();
        }
    }
    return ret;
}

uint64_t OnlinePartitionWriter::GetTotalMemoryUse() const
{
    if (mIsClosed) {
        return 0;
    }

    ScopedLock lock(mWriterLock);
    return mWriter ? mWriter->GetTotalMemoryUse() : 0;
}

uint64_t OnlinePartitionWriter::GetBuildingSegmentDumpExpandSize() const
{
    if (mIsClosed) {
        return 0;
    }
    if (mWriterLock.trylock() == 0) {
        using UnlockerType = std::unique_ptr<autil::ThreadMutex, std::function<void(ThreadMutex*)>>;

        UnlockerType unlocker(&mWriterLock, [](ThreadMutex* lock) { lock->unlock(); });
        if (mWriter) {
            return mWriter->GetBuildingSegmentDumpExpandSize();
        }
    }
    return 0;
}

void OnlinePartitionWriter::ReportMetrics() const
{
    if (mIsClosed) {
        return;
    }
    if (mWriterLock.trylock() == 0) {
        using UnlockerType = std::unique_ptr<autil::ThreadMutex, std::function<void(ThreadMutex*)>>;

        UnlockerType unlocker(&mWriterLock, [](ThreadMutex* lock) { lock->unlock(); });
        if (mWriter) {
            mWriter->ReportMetrics();
        }
    }
}

int64_t OnlinePartitionWriter::GetShuffleTime(const IndexPartitionOptions& options, int64_t randomSeed)
{
    string str = mPartitionName + "." + StringUtil::toString(randomSeed);
    uint64_t hashValue = 0;
    MurmurHasher::GetHashKey(str.c_str(), str.size(), hashValue);

    uint32_t totalRange = options.GetOnlineConfig().maxRealtimeDumpInterval;
    uint32_t shuffleRange = (uint32_t)(totalRange * DEFAULT_SHUFFLE_RANGE_RATIO);
    return shuffleRange == 0 ? 0 : (hashValue % shuffleRange);
}

void OnlinePartitionWriter::SetEnableReleaseOperationAfterDump(bool releaseOperations)
{
    if (mWriter) {
        mWriter->SetEnableReleaseOperationAfterDump(releaseOperations);
    }
}

bool OnlinePartitionWriter::NeedRewriteDocument(const DocumentPtr& doc) { return mWriter->NeedRewriteDocument(doc); }

util::Status OnlinePartitionWriter::GetStatus() const { return mWriter->GetStatus(); }
#undef IE_LOG_PREFIX
}} // namespace indexlib::partition
