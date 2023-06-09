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
#include "indexlib/partition/segment/multi_sharding_segment_writer.h"

#include "indexlib/document/document.h"
#include "indexlib/document/kv_document/kv_document.h"
#include "indexlib/index/kkv/kkv_sharding_partitioner.h"
#include "indexlib/index/kv/kv_sharding_partitioner.h"
#include "indexlib/index/util/shard_partitioner.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/partition/segment/kkv_segment_writer.h"
#include "indexlib/partition/segment/kv_segment_writer.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::index;
using namespace indexlib::common;
using namespace indexlib::document;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, MultiShardingSegmentWriter);

MultiShardingSegmentWriter::MultiShardingSegmentWriter(const IndexPartitionSchemaPtr& schema,
                                                       const IndexPartitionOptions& options,
                                                       const vector<SegmentWriterPtr>& shardingWriters)
    : SegmentWriter(schema, options)
    , mShardingWriters(shardingWriters)
    , mNeedForceDump(false)
{
}

MultiShardingSegmentWriter::~MultiShardingSegmentWriter() {}

MultiShardingSegmentWriter::MultiShardingSegmentWriter(const MultiShardingSegmentWriter& other,
                                                       BuildingSegmentData& segmentData, bool isShared)
    : SegmentWriter(other)
    , mPartitioner(other.mPartitioner)
    , mCurrentSegmentInfo(other.mCurrentSegmentInfo)
    , mSegmentData(other.mSegmentData)
    , mNeedForceDump(other.mNeedForceDump)
{
    for (size_t i = 0; i < other.mShardingWriters.size(); ++i) {
        const auto& segWriter = other.mShardingWriters[i];
        BuildingSegmentData shardingSegmentData = segmentData.CreateShardingSegmentData(i);
        SegmentWriter* newSegWriter = segWriter->CloneWithNewSegmentData(shardingSegmentData, isShared);
        mShardingWriters.push_back(SegmentWriterPtr(newSegWriter));
    }
}

ShardPartitioner* MultiShardingSegmentWriter::Create(const IndexPartitionSchemaPtr& schema, uint32_t shardCount)
{
    TableType type = schema->GetTableType();
    switch (type) {
    case tt_kv: {
        auto partitioner = std::make_unique<KvShardingPartitioner>();
        const auto& s = partitioner->Init(schema, shardCount);
        if (!s.IsOK()) {
            IE_LOG(ERROR, "failed to init KvShardingPartitioner with error:[%s]", s.ToString().c_str());
            return nullptr;
        }
        return partitioner.release();
    }
    case tt_kkv: {
        auto partitioner = std::make_unique<KkvShardingPartitioner>();
        const auto& s = partitioner->Init(schema, shardCount);
        if (!s.IsOK()) {
            IE_LOG(ERROR, "failed to init KkvShardingPartitioner with error:[%s]", s.ToString().c_str());
            return nullptr;
        }
        return partitioner.release();
    }
    default:
        IE_LOG(ERROR, "unsupported table type:[%d]", type);
        assert(false);
    }
    return nullptr;
}

void MultiShardingSegmentWriter::Init(BuildingSegmentData& segmentData,
                                      const std::shared_ptr<indexlib::framework::SegmentMetrics>& metrics,
                                      const QuotaControlPtr& buildMemoryQuotaControler,
                                      const BuildResourceMetricsPtr& buildResourceMetrics)
{
    for (size_t i = 0; i < mShardingWriters.size(); ++i) {
        BuildingSegmentData shardingSegmentData = segmentData.CreateShardingSegmentData(i);
        mShardingWriters[i]->Init(shardingSegmentData, metrics, buildMemoryQuotaControler, buildResourceMetrics);
    }
    assert(buildResourceMetrics);
    mNeedForceDump = false;
    mSegmentData = segmentData;
    mBuildResourceMetrics = buildResourceMetrics;
    mSegmentMetrics = metrics;
    mCurrentSegmentInfo.reset(new SegmentInfo(*segmentData.GetSegmentInfo()));
    mCurrentSegmentInfo->shardCount = mShardingWriters.size();
    mPartitioner.reset(Create(mSchema, mShardingWriters.size()));
}

MultiShardingSegmentWriter* MultiShardingSegmentWriter::CloneWithNewSegmentData(BuildingSegmentData& segmentData,
                                                                                bool isShared) const
{
    return new MultiShardingSegmentWriter(*this, segmentData, isShared);
}

bool MultiShardingSegmentWriter::AddDocument(const DocumentPtr& doc)
{
    assert(mSchema->GetTableType() == tt_kv || mSchema->GetTableType() == tt_kkv);
    auto kvDoc = dynamic_cast<document::KVDocument*>(doc.get());
    if (!kvDoc) {
        IE_LOG(ERROR, "doc is not KVDocument");
        return false;
    }
    bool ret = false;
    for (auto iter = kvDoc->begin(); iter != kvDoc->end(); ++iter) {
        auto& kvIndexDoc = *iter;
        uint32_t shardIdx = 0;
        if (!mPartitioner->GetShardIdx(&kvIndexDoc, shardIdx)) {
            ret = false;
            continue;
        }
        ret = mShardingWriters[shardIdx].get()->AddKVIndexDocument(&kvIndexDoc);
        if (!ret) {
            SetStatus(mShardingWriters[shardIdx]->GetStatus());
            if (GetStatus() == util::NO_SPACE) {
                return false;
            }
        }
        MEMORY_BARRIER();
        if (ret /*shard writer add success*/) {
            mCurrentSegmentInfo->docCount = mCurrentSegmentInfo->docCount + 1;
        }
        mNeedForceDump = mNeedForceDump || mShardingWriters[shardIdx]->NeedForceDump();
    }
    return ret;
}

bool MultiShardingSegmentWriter::IsDirty() const { return mCurrentSegmentInfo->docCount > 0; }

bool MultiShardingSegmentWriter::NeedForceDump() { return mNeedForceDump; }

void MultiShardingSegmentWriter::CollectSegmentMetrics()
{
    for (auto& shardWriter : mShardingWriters) {
        shardWriter->CollectSegmentMetrics();
    }
}

void MultiShardingSegmentWriter::EndSegment()
{
    uint64_t docCount = 0;
    for (auto& shardWriter : mShardingWriters) {
        shardWriter->EndSegment();
        docCount += shardWriter->GetSegmentInfo()->docCount;
    }
    mCurrentSegmentInfo->docCount = docCount;
}

void MultiShardingSegmentWriter::CreateDumpItems(const DirectoryPtr& directory,
                                                 vector<std::unique_ptr<common::DumpItem>>& dumpItems)
{
    for (auto& shardWriter : mShardingWriters) {
        BuildingSegmentData shardSegData = mSegmentData.CreateShardingSegmentData(shardWriter->GetColumnIdx());
        shardWriter->CreateDumpItems(shardSegData.GetDirectory(), dumpItems);
    }
}

InMemorySegmentReaderPtr MultiShardingSegmentWriter::CreateInMemSegmentReader() { return InMemorySegmentReaderPtr(); }

const SegmentInfoPtr& MultiShardingSegmentWriter::GetSegmentInfo() const { return mCurrentSegmentInfo; }

InMemorySegmentModifierPtr MultiShardingSegmentWriter::GetInMemorySegmentModifier()
{
    assert(false);
    return InMemorySegmentModifierPtr();
}
}} // namespace indexlib::partition
