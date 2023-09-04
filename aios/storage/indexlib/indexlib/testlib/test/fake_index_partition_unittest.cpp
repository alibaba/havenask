#include "indexlib/testlib/test/fake_index_partition_unittest.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::partition;

namespace indexlib { namespace testlib {
IE_LOG_SETUP(testlib, FakeIndexPartitionTest);

FakeIndexPartitionTest::FakeIndexPartitionTest() {}

FakeIndexPartitionTest::~FakeIndexPartitionTest() {}

void FakeIndexPartitionTest::CaseSetUp() {}

void FakeIndexPartitionTest::CaseTearDown() {}

void FakeIndexPartitionTest::TestSimpleProcess()
{
    FakeIndexPartition indexPartition;
    ASSERT_EQ(IndexPartition::OS_OK,
              indexPartition.Open("path/to/index", "", IndexPartitionSchemaPtr(), IndexPartitionOptions()));
    ASSERT_EQ(IndexPartition::OS_OK, indexPartition.ReOpen(false));
}
}} // namespace indexlib::testlib
