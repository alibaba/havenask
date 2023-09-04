#include "indexlib/partition/test/on_disk_partition_data_unittest.h"

#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/version_maker.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::test;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OnDiskPartitionDataTest);

OnDiskPartitionDataTest::OnDiskPartitionDataTest() {}

OnDiskPartitionDataTest::~OnDiskPartitionDataTest() {}

void OnDiskPartitionDataTest::CaseSetUp()
{
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.outputStorage = FSST_MEM;
    RESET_FILE_SYSTEM("ut", false, fsOptions);
}

void OnDiskPartitionDataTest::CaseTearDown() {}

void OnDiskPartitionDataTest::TestSimpleProcess()
{
    Version onDiskVersion = VersionMaker::Make(GET_PARTITION_DIRECTORY(), 0, "0,1", "", "", 0, true);

    SegmentDirectoryPtr segDir(new SegmentDirectory);
    segDir->Init(GET_PARTITION_DIRECTORY(), index_base::Version(INVALID_VERSION), true);
    plugin::PluginManagerPtr pluginManager;
    OnDiskPartitionData partitionData(pluginManager);
    partitionData.Open(segDir, OnDiskPartitionData::DMO_SHARED_NEED);

    PartitionData::Iterator it = partitionData.Begin();
    INDEXLIB_TEST_TRUE(it != partitionData.End());
    INDEXLIB_TEST_EQUAL((segmentid_t)0, it->GetSegmentId());
    ++it;
    INDEXLIB_TEST_TRUE(it != partitionData.End());
    INDEXLIB_TEST_EQUAL((segmentid_t)1, it->GetSegmentId());
    ++it;
    INDEXLIB_TEST_TRUE(it == partitionData.End());

    ASSERT_EQ(onDiskVersion, partitionData.GetVersion());
    ASSERT_TRUE(partitionData.GetDeletionMapReader());

    PartitionDataPtr subPartitionData = partitionData.GetSubPartitionData();
    it = partitionData.Begin();
    INDEXLIB_TEST_TRUE(it != partitionData.End());
    INDEXLIB_TEST_EQUAL((segmentid_t)0, it->GetSegmentId());
    ++it;
    INDEXLIB_TEST_TRUE(it != partitionData.End());
    INDEXLIB_TEST_EQUAL((segmentid_t)1, it->GetSegmentId());
    ++it;
    INDEXLIB_TEST_TRUE(it == partitionData.End());
}

void OnDiskPartitionDataTest::TestClone()
{
    Version onDiskVersion = VersionMaker::Make(GET_PARTITION_DIRECTORY(), 0, "0", "", "", 0, true);
    SegmentDirectoryPtr segDir(new SegmentDirectory);
    segDir->Init(GET_PARTITION_DIRECTORY(), index_base::Version(INVALID_VERSION), true);
    OnDiskPartitionDataPtr partData(new OnDiskPartitionData(plugin::PluginManagerPtr()));
    partData->Open(segDir);

    OnDiskPartitionDataPtr clonePartData(partData->Clone());
    ASSERT_TRUE(partData->mSegmentDirectory != clonePartData->mSegmentDirectory);
    ASSERT_TRUE(partData->mSubPartitionData != clonePartData->mSubPartitionData);
    ASSERT_EQ(partData->GetDeletionMapReader(), clonePartData->GetDeletionMapReader());
    ASSERT_EQ(partData->GetPartitionInfoHolder(), clonePartData->GetPartitionInfoHolder());
}

void OnDiskPartitionDataTest::TestGetPartitionMeta()
{
    Version onDiskVersion = VersionMaker::Make(GET_PARTITION_DIRECTORY(), 0, "0,1", "", "", 0, true);

    PartitionMeta meta;
    meta.AddSortDescription("field1", indexlibv2::config::sp_desc);
    meta.AddSortDescription("field2", indexlibv2::config::sp_asc);
    meta.Store(GET_PARTITION_DIRECTORY());

    SegmentDirectoryPtr segDir(new SegmentDirectory);
    segDir->Init(GET_PARTITION_DIRECTORY(), index_base::Version(INVALID_VERSION), true);
    OnDiskPartitionDataPtr partData(new OnDiskPartitionData(plugin::PluginManagerPtr()));
    partData->Open(segDir);

    ASSERT_NO_THROW(meta.AssertEqual(partData->GetPartitionMeta()));

    PartitionDataPtr subPartData = partData->GetSubPartitionData();
    ASSERT_ANY_THROW(meta.AssertEqual(subPartData->GetPartitionMeta()));
    ASSERT_NO_THROW(subPartData->GetPartitionMeta().AssertEqual(PartitionMeta()));
}
}} // namespace indexlib::partition
