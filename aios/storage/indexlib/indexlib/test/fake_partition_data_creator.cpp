#include "indexlib/test/fake_partition_data_creator.h"

#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/partition/online_partition.h"

using namespace std;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::index;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, FakePartitionDataCreator);

FakePartitionDataCreator::FakePartitionDataCreator()
    : mSlowDumpContainer(new SlowDumpSegmentContainer(3600u * 24 * 30 * 1000, false, true))
    , mPsm(false, IndexPartitionResource(), mSlowDumpContainer)
{
}

FakePartitionDataCreator::~FakePartitionDataCreator() { mSlowDumpContainer->DisableSlowSleep(); }

void FakePartitionDataCreator::AddFullDocs(const std::string& fullDocs)
{
    mPsm.Transfer(BUILD_FULL, fullDocs, "", "");
    FillBuiltSegments();
}

void FakePartitionDataCreator::AddBuiltSegment(const std::string& rtDocs)
{
    mPsm.Transfer(BUILD_RT_SEGMENT, rtDocs, "", "");
    mSlowDumpContainer->DisableSlowSleep();
    mSlowDumpContainer->WaitEmpty();
    mSlowDumpContainer->EnableSlowSleep();
    mPsm.Transfer(PE_REOPEN, "", "", "");
    FillBuiltSegments();
}

void FakePartitionDataCreator::AddInMemSegment(const std::string& rtDocs)
{
    FillInMemSegments();
    mSlowDumpContainer->EnableSlowSleep();
    mPsm.Transfer(BUILD_RT_SEGMENT, rtDocs, "", "");
}

void FakePartitionDataCreator::AddBuildingData(const std::string& rtDocs)
{
    mPsm.Transfer(BUILD_RT, rtDocs, "", "");
    assert(!mBuildingSegment);
    OnlinePartitionPtr onlinePartition = DYNAMIC_POINTER_CAST(OnlinePartition, mPsm.GetIndexPartition());
    assert(onlinePartition);
    PartitionDataPtr partData = onlinePartition->GetPartitionData();
    mBuildingSegment = partData->GetInMemorySegment();
}

void FakePartitionDataCreator::FillBuiltSegments()
{
    assert(mInMemSegments.empty());
    assert(!mBuildingSegment);

    OnlinePartitionPtr onlinePartition = DYNAMIC_POINTER_CAST(OnlinePartition, mPsm.GetIndexPartition());
    assert(onlinePartition);
    PartitionDataPtr partData = onlinePartition->GetPartitionData();

    vector<SegmentData> tmpSegVec;
    PartitionData::Iterator iter = partData->Begin();
    while (iter != partData->End()) {
        tmpSegVec.push_back(*iter);
        iter++;
    }
    mBuiltSegments.swap(tmpSegVec);
}

void FakePartitionDataCreator::FillInMemSegments()
{
    assert(!mBuildingSegment);
    OnlinePartitionPtr onlinePartition = DYNAMIC_POINTER_CAST(OnlinePartition, mPsm.GetIndexPartition());
    assert(onlinePartition);
    PartitionDataPtr partData = onlinePartition->GetPartitionData();
    InMemorySegmentPtr inMemSegment = partData->GetInMemorySegment();
    mInMemSegments.push_back(inMemSegment);
}

PartitionDataPtr FakePartitionDataCreator::CreatePartitionData(bool useFake)
{
    OnlinePartitionPtr onlinePartition = DYNAMIC_POINTER_CAST(OnlinePartition, mPsm.GetIndexPartition());
    assert(onlinePartition);
    PartitionDataPtr partData = onlinePartition->GetPartitionData();

    FakePartitionDataPtr fakePartData(new FakePartitionData(partData));
    fakePartData->Init(mBuiltSegments, mInMemSegments, mBuildingSegment);

    PartitionSegmentIteratorPtr fakeSegIter = fakePartData->CreateSegmentIterator();
    PartitionSegmentIteratorPtr actualIter = partData->CreateSegmentIterator();

    while (actualIter->IsValid()) {
        if (actualIter->GetType() == SIT_BUILDING &&
            actualIter->GetInMemSegment()->GetStatus() == InMemorySegment::BUILDING) {
            break;
        }
        assert(fakeSegIter->IsValid());
        assert(fakeSegIter->GetSegmentId() == actualIter->GetSegmentId());
        actualIter->MoveToNext();
        fakeSegIter->MoveToNext();
    }
    if (useFake) {
        return fakePartData;
    }
    return partData;
}

partition::IndexPartitionPtr FakePartitionDataCreator::GetIndexPartition() { return mPsm.GetIndexPartition(); }
}} // namespace indexlib::test
