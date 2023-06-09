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
#ifndef __ONLINE_PARTITION_WRITER_H
#define __ONLINE_PARTITION_WRITER_H

#include <memory>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/offline_partition_writer.h"
#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace partition {

class OnlinePartitionWriter : public PartitionWriter
{
public:
    OnlinePartitionWriter(const config::IndexPartitionOptions& options, const DumpSegmentContainerPtr& container,
                          OnlinePartitionMetrics* onlinePartMetrics = NULL, const std::string& partitionName = "",
                          document::SrcSignature src = document::SrcSignature());

    ~OnlinePartitionWriter();

public:
    void Open(const config::IndexPartitionSchemaPtr& schema, const index_base::PartitionDataPtr& partitionData,
              const PartitionModifierPtr& modifier) override;
    void ReOpenNewSegment(const PartitionModifierPtr& modifier) override;
    bool BuildDocument(const document::DocumentPtr& doc) override;
    bool BatchBuild(std::unique_ptr<document::DocumentCollector> docCollector) override;
    PartitionWriter::BuildMode GetBuildMode() override;
    void SwitchBuildMode(PartitionWriter::BuildMode buildMode) override;
    bool NeedDump(size_t memoryQuota, const document::DocumentCollector* docCollector) const override;
    void EndIndex() override;
    void DumpSegment() override;
    void Close() override;
    void CloseDiscardData();
    void RewriteDocument(const document::DocumentPtr& doc) override;
    uint64_t GetTotalMemoryUse() const override;
    void ReportMetrics() const override;

    void SetEnableReleaseOperationAfterDump(bool releaseOperations) override;
    uint64_t GetBuildingSegmentDumpExpandSize() const override;
    bool IsClosed() const { return mIsClosed; }
    bool DumpSegmentWithMemLimit();
    bool NeedRewriteDocument(const document::DocumentPtr& doc) override final;
    util::Status GetStatus() const override final;
    bool EnableAsyncDump() const override { return mWriter->EnableAsyncDump(); }
    void Reset();

    int64_t GetLastDumpTimestamp() const { return mLastDumpTs; }

private:
    virtual bool CheckTimestamp(const document::DocumentPtr& doc) const;
    int64_t GetShuffleTime(const config::IndexPartitionOptions& options, int64_t randomSeed);

protected:
    config::IndexPartitionSchemaPtr mSchema;
    index_base::PartitionDataPtr mPartitionData;
    DumpSegmentContainerPtr mDumpSegmentContainer;
    OfflinePartitionWriterPtr mWriter;
    int64_t mRtFilterTimestamp;
    int64_t mLastDumpTs;
    OnlinePartitionMetrics* mOnlinePartMetrics;
    volatile bool mIsClosed;
    document::SrcSignature mSrcSignature;
    mutable autil::ThreadMutex mWriterLock;

    static constexpr float DEFAULT_SHUFFLE_RANGE_RATIO = 0.7;

private:
    friend class OnlinePartitionWriterTest;
    friend class IndexPartitionMaker;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnlinePartitionWriter);
}} // namespace indexlib::partition

#endif //__ONLINE_PARTITION_WRITER_H
