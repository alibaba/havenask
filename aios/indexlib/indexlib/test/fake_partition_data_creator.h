#ifndef __INDEXLIB_FAKE_PARTITION_DATA_CREATOR_H
#define __INDEXLIB_FAKE_PARTITION_DATA_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/test/slow_dump_segment_container.h"
#include "indexlib/test/partition_state_machine.h"

IE_NAMESPACE_BEGIN(partition);

class FakePartitionData : public index_base::PartitionData
{
public:
    FakePartitionData(const index_base::PartitionDataPtr& partData)
        : mPartData(partData)
    {}

    void Init(const std::vector<index_base::SegmentData>& builtSegments,
              const std::vector<index_base::InMemorySegmentPtr>& dumpingSegments,
              const index_base::InMemorySegmentPtr& buildingSegment)
    {
        mBuiltSegments = builtSegments;
        mDumpingSegments = dumpingSegments;
        mBuildingSegment = buildingSegment;
    }

public:
    index_base::Version GetVersion() const override
    { return mPartData->GetVersion(); }

    index_base::Version GetOnDiskVersion() const override
    { return mPartData->GetOnDiskVersion(); }

    index_base::PartitionMeta GetPartitionMeta() const override
    { return mPartData->GetPartitionMeta(); }

    const file_system::DirectoryPtr& GetRootDirectory() const override
    { return mPartData->GetRootDirectory(); }

    const index_base::IndexFormatVersion& GetIndexFormatVersion() const override
    { return mPartData->GetIndexFormatVersion(); }

    index_base::PartitionData::Iterator Begin() const override
    { return mPartData->Begin(); }
    index_base::PartitionData::Iterator End() const override
    { return mPartData->End(); }

    index_base::SegmentData GetSegmentData(segmentid_t segId) const override
    {
        for (size_t i = 0; i < mBuiltSegments.size(); i++)
        {
            if (segId == mBuiltSegments[i].GetSegmentId())
            {
                return mBuiltSegments[i];
            }
        }
        return index_base::SegmentData();
    }

    index::DeletionMapReaderPtr GetDeletionMapReader() const override
    { return mPartData->GetDeletionMapReader(); }

    index::PartitionInfoPtr GetPartitionInfo() const override
    { return mPartData->GetPartitionInfo(); }

    index_base::PartitionData* Clone() override { assert(false); return NULL; }
    index_base::PartitionDataPtr GetSubPartitionData() const override
    { assert(false); return index_base::PartitionDataPtr(); }

    const index_base::SegmentDirectoryPtr& GetSegmentDirectory() const override
    { return mPartData->GetSegmentDirectory(); }

    index_base::InMemorySegmentPtr GetInMemorySegment() const override
    { assert(false); return index_base::InMemorySegmentPtr(); }
    void ResetInMemorySegment() override { assert(false); }
    void CommitVersion() override { assert(false); }
    void RemoveSegments(const std::vector<segmentid_t>& segIds) override { assert(false); }
    void UpdatePartitionInfo() override { assert(false); }
    index_base::InMemorySegmentPtr CreateNewSegment() override
    { assert(false); return index_base::InMemorySegmentPtr(); }

    index_base::InMemorySegmentPtr CreateJoinSegment() override
    { assert(false); return index_base::InMemorySegmentPtr(); }

    uint32_t GetIndexShardingColumnNum(
            const config::IndexPartitionOptions& options) const override
    { assert(false); return 0; }

    segmentid_t GetLastValidRtSegmentInLinkDirectory() const override
    { assert(false); return INVALID_SEGMENTID; }

    bool SwitchToLinkDirectoryForRtSegments(segmentid_t lastLinkRtSegId) override
    { assert(false); return false; }

    const util::CounterMapPtr& GetCounterMap() const override
    {
        static util::CounterMapPtr counterMap;
        return counterMap;
    }

    std::string GetLastLocator() const override
    { assert(false); return std::string(""); }

    index_base::PartitionSegmentIteratorPtr CreateSegmentIterator() override
    {
        index_base::PartitionSegmentIteratorPtr segIter(
            new index_base::PartitionSegmentIterator(true));
        segIter->Init(mBuiltSegments, mDumpingSegments, mBuildingSegment, INVALID_SEGMENTID);
        return segIter;
    }

    const plugin::PluginManagerPtr& GetPluginManager() const override
    {
        assert(false);
        return mPartData->GetPluginManager();
    }

private:
    index_base::PartitionDataPtr mPartData;
    std::vector<index_base::SegmentData> mBuiltSegments;
    std::vector<index_base::InMemorySegmentPtr> mDumpingSegments;
    index_base::InMemorySegmentPtr mBuildingSegment;
};

DEFINE_SHARED_PTR(FakePartitionData);
IE_NAMESPACE_END(partition);

IE_NAMESPACE_BEGIN(test);

class FakePartitionDataCreator
{
public:
    FakePartitionDataCreator();
    ~FakePartitionDataCreator();

public:
    void Init(const config::IndexPartitionSchemaPtr& schema,
              config::IndexPartitionOptions options,
              std::string root,
              bool enableAsyncDumpSegment = true)
    {
        options.GetOnlineConfig().enableAsyncDumpSegment = enableAsyncDumpSegment;
        mPsm.Init(schema, options, root);
    }

    void AddFullDocs(const std::string& fullDocs);
    void AddBuiltSegment(const std::string& rtDocs);
    void AddInMemSegment(const std::string& rtDocs);
    void AddBuildingData(const std::string& rtDocs);

    void Reopen() {  mPsm.Transfer(PE_REOPEN, "", "", ""); }

    index_base::PartitionDataPtr CreatePartitionData(bool useFake = true);
    partition::IndexPartitionPtr GetIndexPartition();

    void DisableSlowDump() { mSlowDumpContainer->DisableSlowSleep(); }
    void EnableSlowSleep() { mSlowDumpContainer->EnableSlowSleep(); }
    void WaitDumpContainerEmpty() { mSlowDumpContainer->WaitEmpty(); }
    size_t GetDumpContainerSize() { return mSlowDumpContainer->GetDumpItemSize(); }

private:
    void FillBuiltSegments();
    void FillInMemSegments();

private:
    test::SlowDumpSegmentContainerPtr mSlowDumpContainer;
    PartitionStateMachine mPsm;
    std::vector<index_base::SegmentData> mBuiltSegments;
    std::vector<index_base::InMemorySegmentPtr> mInMemSegments;
    index_base::InMemorySegmentPtr mBuildingSegment;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakePartitionDataCreator);

IE_NAMESPACE_END(test);

#endif //__INDEXLIB_FAKE_PARTITION_DATA_CREATOR_H
