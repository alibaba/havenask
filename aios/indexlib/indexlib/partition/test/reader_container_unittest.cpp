#include "indexlib/partition/test/reader_container_unittest.h"
#include "indexlib/partition/test/mock_index_partition_reader.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, ReaderContainerTest);

ReaderContainerTest::ReaderContainerTest()
{
}

ReaderContainerTest::~ReaderContainerTest()
{
}

void ReaderContainerTest::CaseSetUp()
{
}

void ReaderContainerTest::CaseTearDown()
{
}

void ReaderContainerTest::TestSimpleProcess()
{
    ReaderContainer container;
    MockIndexPartitionReaderPtr reader(new MockIndexPartitionReader());

    EXPECT_CALL(*reader, GetVersion())
        .WillOnce(Return(Version(0)));

    container.AddReader(reader);
    ASSERT_EQ(reader, container.GetOldestReader());

    container.EvictOldestReader();
    ASSERT_EQ(IndexPartitionReaderPtr(), container.GetOldestReader());
}

void ReaderContainerTest::TestHasReader()
{
    ReaderContainer container;

    MockIndexPartitionReaderPtr reader0(new MockIndexPartitionReader());
    EXPECT_CALL(*reader0, GetVersion())
        .WillOnce(Return(Version(0)));
    container.AddReader(reader0);

    MockIndexPartitionReaderPtr reader1(new MockIndexPartitionReader());
    EXPECT_CALL(*reader1, GetVersion())
        .WillOnce(Return(Version(1)));
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

    EXPECT_CALL(*reader0, GetVersion())
        .WillOnce(Return(Version(0)));
    container.AddReader(reader0);
    container.GetSwitchRtSegments(segIds);
    ASSERT_THAT(segIds, ElementsAre(0, 1, 2));

    MockIndexPartitionReaderPtr reader1(new MockIndexPartitionReader());
    reader1->AddSwitchRtSegId(1);
    reader1->AddSwitchRtSegId(2);
    reader1->AddSwitchRtSegId(4);

    EXPECT_CALL(*reader1, GetVersion())
        .WillOnce(Return(Version(1)));
    container.AddReader(reader1);
    container.GetSwitchRtSegments(segIds);
    ASSERT_THAT(segIds, ElementsAre(0, 1, 2, 4));

    container.EvictOldestReader();
    container.GetSwitchRtSegments(segIds);
    ASSERT_THAT(segIds, ElementsAre(1, 2, 4));
}

IE_NAMESPACE_END(partition);

