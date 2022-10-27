#include "indexlib/testlib/test/fake_index_partition_unittest.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(partition);

IE_NAMESPACE_BEGIN(testlib);
IE_LOG_SETUP(testlib, FakeIndexPartitionTest);

FakeIndexPartitionTest::FakeIndexPartitionTest()
{
}

FakeIndexPartitionTest::~FakeIndexPartitionTest()
{
}

void FakeIndexPartitionTest::CaseSetUp()
{
}

void FakeIndexPartitionTest::CaseTearDown()
{
}

void FakeIndexPartitionTest::TestSimpleProcess()
{
    FakeIndexPartition indexPartition;
    ASSERT_EQ(IndexPartition::OS_OK, indexPartition.Open("path/to/index", "",
                    IndexPartitionSchemaPtr(), IndexPartitionOptions()));
    ASSERT_EQ(IndexPartition::OS_OK, indexPartition.ReOpen(false));
}

IE_NAMESPACE_END(testlib);

