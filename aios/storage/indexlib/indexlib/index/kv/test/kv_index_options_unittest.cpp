#include "indexlib/index/kv/test/kv_index_options_unittest.h"

#include <random>

#include "indexlib/config/test/region_schema_maker.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/config/value_config.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/building_segment_iterator.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/segment_iterator.h"
#include "indexlib/index_define.h"
#include "indexlib/partition/in_memory_partition_data.h"
#include "indexlib/partition/segment/dump_segment_container.h"

using namespace std;
using namespace indexlib::common;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::partition;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KvIndexOptionsTest);

namespace {
class MockValueConfig : public ValueConfig
{
public:
    MOCK_METHOD(int32_t, GetFixedLength, (), (const));
};

class MockSegmentIterator : public BuildingSegmentIterator
{
public:
    MOCK_METHOD(bool, IsValid, (), (const, override));
    MOCK_METHOD(SegmentData, GetSegmentData, (), (const, override));
};

class MockPartitionSegmentIterator : public PartitionSegmentIterator
{
public:
    SegmentIteratorPtr CreateIterator(SegmentIteratorType type) { return mIterator; };
    void SetIterator(SegmentIteratorPtr& iterator) { mIterator = iterator; };
    // MOCK_METHOD(SegmentIteratorPtr, CreateIterator, (SegmentIteratorType type), (override));
    MOCK_METHOD(segmentid_t, GetNextBuildingSegmentId, (), (const));
    MOCK_METHOD(bool, IsValid, (), (const, override));
    MOCK_METHOD(segmentid_t, GetSegmentId, (), (const, override));
    MOCK_METHOD(void, MoveToNext, (), (override));

private:
    SegmentIteratorPtr mIterator;
};

class MockPartitionData : public InMemoryPartitionData
{
public:
    MockPartitionData() : InMemoryPartitionData(DumpSegmentContainerPtr(new DumpSegmentContainer())) {};
    MOCK_METHOD(index_base::Version, GetOnDiskVersion, (), (const, override));
    MOCK_METHOD(PartitionSegmentIteratorPtr, CreateSegmentIterator, (), (override));
};

}; // namespace

KvIndexOptionsTest::KvIndexOptionsTest() {}

KvIndexOptionsTest::~KvIndexOptionsTest() {}

void KvIndexOptionsTest::CaseSetUp() {}

void KvIndexOptionsTest::CaseTearDown() {}

void KvIndexOptionsTest::TestInit()
{
    regionid_t regionId = 888;
    KVIndexConfigPtr kvIndexConfig;
    kvIndexConfig.reset(new KVIndexConfig("test", it_kv));
    kvIndexConfig->SetRegionInfo(regionId, 100);
    MockValueConfig* mockValueConfig = new MockValueConfig();
    ValueConfigPtr valueConfig(mockValueConfig);
    kvIndexConfig->SetValueConfig(valueConfig);

    int64_t incTs = 1000000;
    Version version;
    version.SetTimestamp(incTs);
    MockPartitionData* mockPartitionData = new MockPartitionData();
    PartitionDataPtr partitionData(mockPartitionData);

    MockPartitionSegmentIterator* mockPartSegIter = new MockPartitionSegmentIterator();
    PartitionSegmentIteratorPtr partSegmentIterator(mockPartSegIter);

    MockSegmentIterator* mockSegIter = new MockSegmentIterator();
    SegmentIteratorPtr segIter(mockSegIter);
    // partSegmentIterator->SetIterator(segIter);

    segmentid_t buildingSegmentId = 18;
    SegmentData segData;
    segData.SetSegmentId(buildingSegmentId);

    EXPECT_CALL(*mockPartitionData, GetOnDiskVersion()).WillRepeatedly(Return(version));

    EXPECT_CALL(*mockPartitionData, CreateSegmentIterator()).WillRepeatedly(Return(partSegmentIterator));

    EXPECT_CALL(*mockSegIter, IsValid()).Times(1).WillOnce(Return(true));
    EXPECT_CALL(*mockSegIter, GetSegmentData()).WillRepeatedly(Return(segData));

    // EXPECT_CALL(*mockPartSegIter, CreateIterator(_))
    //    .WillRepeatedly(Return(segIter));

    SegmentIteratorPtr newIter = partSegmentIterator->CreateIterator(SIT_BUILDING);

    EXPECT_CALL(*mockPartSegIter, IsValid()).WillRepeatedly(Return(false));

    KVIndexOptions indexOptions;

    indexOptions.Init(kvIndexConfig, partitionData, 40);
    ASSERT_EQ(MicrosecondToSecond(incTs), indexOptions.incTsInSecond);
    ASSERT_EQ(buildingSegmentId, indexOptions.buildingSegmentId);
    ASSERT_EQ(buildingSegmentId, indexOptions.oldestRtSegmentId);
}

void KvIndexOptionsTest::TestGetRegionId()
{
    for (regionid_t regionId = -100; regionId < 100; regionId++) {
        KVIndexConfigPtr kvIndexConfig;
        kvIndexConfig.reset(new KVIndexConfig("test", it_kv));
        kvIndexConfig->SetRegionInfo(regionId, 100);
        KVIndexOptions indexOptions;
        indexOptions.hasMultiRegion = true;
        indexOptions.kvConfig = kvIndexConfig;
        ASSERT_EQ(regionId, indexOptions.GetRegionId());
    }
    {
        KVIndexOptions indexOptions;
        ASSERT_EQ(INVALID_REGIONID, indexOptions.GetRegionId());
    }
}

void KvIndexOptionsTest::TestGetLookupKeyHash()
{
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;

    {
        // no kvConfig
        KVIndexOptions indexOptions;
        uint64_t key = dis(gen);
        ASSERT_EQ(key, indexOptions.GetLookupKeyHash(key));
    }
    {
        // region count <= 1
        KVIndexConfigPtr kvConfig;
        kvConfig.reset(new KVIndexConfig("test", it_kv));
        kvConfig->SetRegionInfo(99, 0);
        KVIndexOptions indexOptions;
        indexOptions.kvConfig = kvConfig;
        uint64_t key = dis(gen);
        ASSERT_EQ(key, indexOptions.GetLookupKeyHash(key));

        key = dis(gen);
        kvConfig->SetRegionInfo(99, 1);
        ASSERT_EQ(key, indexOptions.GetLookupKeyHash(key));
    }
    for (regionid_t regionId = -100; regionId < 100; regionId++) {
        KVIndexConfigPtr kvConfig;
        kvConfig.reset(new KVIndexConfig("test", it_kv));
        kvConfig->SetRegionInfo(regionId, 100);
        KVIndexOptions indexOptions;
        indexOptions.hasMultiRegion = true;
        indexOptions.kvConfig = kvConfig;
        uint64_t key = dis(gen);
        uint64_t expectedKeyHash = MultiRegionRehasher::GetRehashKey(key, regionId);
        ASSERT_EQ(expectedKeyHash, indexOptions.GetLookupKeyHash(key));
    }
}
}} // namespace indexlib::index
