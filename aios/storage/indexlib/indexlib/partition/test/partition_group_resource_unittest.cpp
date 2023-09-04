#include "indexlib/misc/log.h"
#include "indexlib/partition/partition_group_resource.h"
#include "indexlib/test/unittest.h"

using namespace std;

namespace indexlib { namespace partition {

class PartitionGroupResourceTest : public INDEXLIB_TESTBASE
{
public:
    PartitionGroupResourceTest();
    ~PartitionGroupResourceTest();

    DECLARE_CLASS_NAME(PartitionGroupResourceTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PartitionGroupResourceTest, TestSimpleProcess);
IE_LOG_SETUP(partition, PartitionGroupResourceTest);

PartitionGroupResourceTest::PartitionGroupResourceTest() {}

PartitionGroupResourceTest::~PartitionGroupResourceTest() {}

void PartitionGroupResourceTest::CaseSetUp() {}

void PartitionGroupResourceTest::CaseTearDown() {}

void PartitionGroupResourceTest::TestSimpleProcess()
{
    PartitionGroupResourcePtr resource =
        PartitionGroupResource::Create(1024, util::MetricProviderPtr(), nullptr, nullptr);
    ASSERT_TRUE(resource->GetFileBlockCacheContainer().get() != nullptr);
    ASSERT_TRUE(resource->GetTaskScheduler().get() != nullptr);
}

}} // namespace indexlib::partition
