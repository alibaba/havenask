#include "indexlib/partition/segment/multi_sharding_segment_writer.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/index/sharding_partitioner_creator.h"
#include "indexlib/index/sharding_partitioner.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, MultiShardingSegmentWriter);

MultiShardingSegmentWriter::MultiShardingSegmentWriter(
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options,
        const vector<SegmentWriterPtr>& shardingWriters)
    : SegmentWriter(schema, options)
    , mShardingWriters(shardingWriters)
    , mNeedForceDump(false)
{}

MultiShardingSegmentWriter::~MultiShardingSegmentWriter() 
{}

MultiShardingSegmentWriter::MultiShardingSegmentWriter(
        const MultiShardingSegmentWriter& other,
        BuildingSegmentData& segmentData)
    : SegmentWriter(other)
    , mPartitioner(other.mPartitioner)
    , mCurrentSegmentInfo(other.mCurrentSegmentInfo)
    , mSegmentData(other.mSegmentData)
    , mNeedForceDump(other.mNeedForceDump)
{
    for (size_t i = 0; i < other.mShardingWriters.size(); ++i)
    {
        const auto& segWriter = other.mShardingWriters[i];
        BuildingSegmentData shardingSegmentData = segmentData.CreateShardingSegmentData(i);
        SegmentWriter* newSegWriter = segWriter->CloneWithNewSegmentData(shardingSegmentData);
        mShardingWriters.push_back(SegmentWriterPtr(newSegWriter));
    }
}

void MultiShardingSegmentWriter::Init(
        BuildingSegmentData& segmentData,
        const SegmentMetricsPtr& metrics,
        const QuotaControlPtr& buildMemoryQuotaControler,
        const BuildResourceMetricsPtr& buildResourceMetrics)
{
    for (size_t i = 0; i < mShardingWriters.size(); ++i)
    {
        BuildingSegmentData shardingSegmentData = segmentData.CreateShardingSegmentData(i);
        mShardingWriters[i]->Init(shardingSegmentData, metrics,
                buildMemoryQuotaControler, buildResourceMetrics);
    }
    assert(buildResourceMetrics);
    mNeedForceDump = false;
    mSegmentData = segmentData;
    mBuildResourceMetrics = buildResourceMetrics;
    mSegmentMetrics = metrics;
    mCurrentSegmentInfo.reset(new SegmentInfo(segmentData.GetSegmentInfo()));
    mCurrentSegmentInfo->shardingColumnCount = mShardingWriters.size();
    mPartitioner.reset(ShardingPartitionerCreator::Create(
                    mSchema, mShardingWriters.size()));
}

MultiShardingSegmentWriter* MultiShardingSegmentWriter::CloneWithNewSegmentData(
        BuildingSegmentData& segmentData) const
{
    return new MultiShardingSegmentWriter(*this, segmentData);
}

bool MultiShardingSegmentWriter::AddDocument(const DocumentPtr& doc)
{
    assert(mPartitioner);
    uint32_t shardingIdx = 0;
    if (!mPartitioner->GetShardingIdx(doc, shardingIdx))
    {
        return false;
    }
    if (!mShardingWriters[shardingIdx]->AddDocument(doc))
    {
        SetStatus(mShardingWriters[shardingIdx]->GetStatus());
        return false;
    }
    MEMORY_BARRIER();
    mCurrentSegmentInfo->docCount++;
    mNeedForceDump = mNeedForceDump || mShardingWriters[shardingIdx]->NeedForceDump();
    return true;
}

bool MultiShardingSegmentWriter::IsDirty() const
{
    return mCurrentSegmentInfo->docCount > 0;
}

bool MultiShardingSegmentWriter::NeedForceDump()
{
    return mNeedForceDump;
}

void MultiShardingSegmentWriter::CollectSegmentMetrics()
{
    for (auto & shardWriter : mShardingWriters)
    {
        shardWriter->CollectSegmentMetrics();
    }
}

void MultiShardingSegmentWriter::EndSegment()
{
    uint64_t docCount = 0;
    for (auto & shardWriter : mShardingWriters)
    {
        shardWriter->EndSegment();
        docCount += shardWriter->GetSegmentInfo()->docCount;
    }
    mCurrentSegmentInfo->docCount = docCount;
}

void MultiShardingSegmentWriter::CreateDumpItems(
        const DirectoryPtr& directory, vector<common::DumpItem*>& dumpItems)
{
    for (auto & shardWriter : mShardingWriters)
    {
        BuildingSegmentData shardSegData = mSegmentData.CreateShardingSegmentData(
                shardWriter->GetColumnIdx());
        shardWriter->CreateDumpItems(shardSegData.GetDirectory(), dumpItems);
    }
}

InMemorySegmentReaderPtr MultiShardingSegmentWriter::CreateInMemSegmentReader()
{
    return InMemorySegmentReaderPtr();
}
    
const SegmentInfoPtr& MultiShardingSegmentWriter::GetSegmentInfo() const
{
    return mCurrentSegmentInfo;
}

InMemorySegmentModifierPtr MultiShardingSegmentWriter::GetInMemorySegmentModifier()
{
    assert(false);
    return InMemorySegmentModifierPtr();
}

IE_NAMESPACE_END(partition);

