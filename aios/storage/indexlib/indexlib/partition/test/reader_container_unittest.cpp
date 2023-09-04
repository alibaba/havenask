#include "indexlib/partition/test/reader_container_unittest.h"

#include "indexlib/framework/VersionDeployDescription.h"
#include "indexlib/partition/test/mock_index_partition_reader.h"

using namespace std;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, ReaderContainerTest);

ReaderContainerTest::ReaderContainerTest() {}

ReaderContainerTest::~ReaderContainerTest() {}

void ReaderContainerTest::CaseSetUp() {}

void ReaderContainerTest::CaseTearDown() {}

void ReaderContainerTest::TestSimpleProcess()
{
    ReaderContainer container;
    MockIndexPartitionReaderPtr reader(new MockIndexPartitionReader());

    EXPECT_CALL(*reader, GetVersion()).WillOnce(Return(Version(0)));

    container.AddReader(reader);
    ASSERT_EQ(reader, container.GetOldestReader());

    container.EvictOldestReader();
    ASSERT_EQ(IndexPartitionReaderPtr(), container.GetOldestReader());
}

void ReaderContainerTest::TestHasReader()
{
    ReaderContainer container;

    MockIndexPartitionReaderPtr reader0(new MockIndexPartitionReader());
    EXPECT_CALL(*reader0, GetVersion()).WillOnce(Return(Version(0)));
    container.AddReader(reader0);

    MockIndexPartitionReaderPtr reader1(new MockIndexPartitionReader());
    EXPECT_CALL(*reader1, GetVersion()).WillOnce(Return(Version(1)));
    container.AddReader(reader1);

    ASSERT_TRUE(container.HasReader(Version(0)));
    ASSERT_TRUE(container.HasReader(Version(1)));
    ASSERT_TRUE(!container.HasReader(Version(2)));
}

void ReaderContainerTest::TestGetSwitchRtSegments()
{
    ReaderContainer container;

    vector<segmentid_t> segIds;
    MockIndexPartitionReaderPtr reader0(new MockIndexPartitionReader());
    reader0->AddSwitchRtSegId(0);
    reader0->AddSwitchRtSegId(1);
    reader0->AddSwitchRtSegId(2);

    EXPECT_CALL(*reader0, GetVersion()).WillOnce(Return(Version(0)));
    container.AddReader(reader0);
    container.GetSwitchRtSegments(segIds);
    ASSERT_THAT(segIds, ElementsAre(0, 1, 2));

    MockIndexPartitionReaderPtr reader1(new MockIndexPartitionReader());
    reader1->AddSwitchRtSegId(1);
    reader1->AddSwitchRtSegId(2);
    reader1->AddSwitchRtSegId(4);

    EXPECT_CALL(*reader1, GetVersion()).WillOnce(Return(Version(1)));
    container.AddReader(reader1);
    container.GetSwitchRtSegments(segIds);
    ASSERT_THAT(segIds, ElementsAre(0, 1, 2, 4));

    container.EvictOldestReader();
    container.GetSwitchRtSegments(segIds);
    ASSERT_THAT(segIds, ElementsAre(1, 2, 4));
}

void ReaderContainerTest::TestGetNeedKeepDeployFiles()
{
    ReaderContainer container;
    MockIndexPartitionReaderPtr reader0(new MockIndexPartitionReader());
    EXPECT_CALL(*reader0, GetVersion()).WillOnce(Return(Version(0)));
    MockIndexPartitionReaderPtr reader1(new MockIndexPartitionReader());
    EXPECT_CALL(*reader1, GetVersion()).WillOnce(Return(Version(1)));

    container.AddReader(reader0);
    container.AddReader(reader1);

    auto versionDpDesc0 = std::make_shared<indexlibv2::framework::VersionDeployDescription>();
    auto versionDpDesc1 = std::make_shared<indexlibv2::framework::VersionDeployDescription>();

    std::string versionDpDescJsonStr0 = R"( {
        "local_deploy_index_metas": [
            {
                "deploy_file_metas" : [
                    {"path" : "segment_0_level_0/"},
                    {"path" : "segment_0_level_0/file"},
                    {"path" : "segment_1_level_0/"},
                    {"path" : "segment_1_level_0/file"}
                ]
            }
        ]
    } )";
    std::string versionDpDescJsonStr1 = R"( {
        "local_deploy_index_metas": [
            {
                "deploy_file_metas" : [
                    {"path" : "segment_0_level_0/file"},
                    {"path" : "segment_0_level_0/dir/file"},
                    {"path" : "segment_1_level_0/"},
                    {"path" : "segment_1_level_0/file"},
                    {"path" : "segment_2_level_0/"},
                    {"path" : "segment_2_level_0/file"}
                ]
            }
        ]
    } )";
    FromJsonString(versionDpDesc0, versionDpDescJsonStr0);
    FromJsonString(versionDpDesc1, versionDpDescJsonStr1);
    set<string> tmp;
    versionDpDesc0->GetLocalDeployFileList(&tmp);
    versionDpDesc1->GetLocalDeployFileList(&tmp);

    EXPECT_CALL(*reader0, GetVersionDeployDescription()).WillOnce(Return(versionDpDesc0));
    EXPECT_CALL(*reader1, GetVersionDeployDescription()).WillOnce(Return(versionDpDesc1));

    set<string> whiteList;
    ASSERT_TRUE(container.GetNeedKeepDeployFiles(&whiteList));
    EXPECT_THAT(whiteList,
                UnorderedElementsAre("segment_0_level_0/", "segment_0_level_0/file", "segment_0_level_0/dir/file",
                                     "segment_1_level_0/", "segment_1_level_0/file", "segment_2_level_0/",
                                     "segment_2_level_0/file"));

    EXPECT_CALL(*reader0, GetVersionDeployDescription()).WillOnce(Return(versionDpDesc0));
    EXPECT_CALL(*reader1, GetVersionDeployDescription()).WillOnce(Return(nullptr));
    whiteList.clear();
    ASSERT_FALSE(container.GetNeedKeepDeployFiles(&whiteList));

    ASSERT_TRUE(versionDpDesc0->DisableFeature(
        indexlibv2::framework::VersionDeployDescription::FeatureType::DEPLOY_META_MANIFEST));
    EXPECT_CALL(*reader0, GetVersionDeployDescription()).WillOnce(Return(versionDpDesc0));
    EXPECT_CALL(*reader1, GetVersionDeployDescription()).WillRepeatedly(Return(versionDpDesc1));
    whiteList.clear();
    ASSERT_FALSE(container.GetNeedKeepDeployFiles(&whiteList));
}

}} // namespace indexlib::partition
