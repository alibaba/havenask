#include "indexlib/partition/segment/test/in_memory_segment_container_unittest.h"

#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;
using namespace indexlib::util;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, InMemorySegmentContainerTest);

InMemorySegmentContainerTest::InMemorySegmentContainerTest() {}

InMemorySegmentContainerTest::~InMemorySegmentContainerTest() {}

void InMemorySegmentContainerTest::CaseSetUp() {}

void InMemorySegmentContainerTest::CaseTearDown() {}

namespace {
class MockInMemorySegment : public index_base::InMemorySegment
{
public:
    MockInMemorySegment()
        : InMemorySegment(config::BuildConfig(), util::MemoryQuotaControllerCreator::CreateBlockMemoryController(),
                          util::CounterMapPtr())
    {
    }
    ~MockInMemorySegment() {}

    MOCK_METHOD(size_t, GetTotalMemoryUse, (), (const, override));
};
DEFINE_SHARED_PTR(MockInMemorySegment);
} // namespace
void InMemorySegmentContainerTest::TestSimpleProcess()
{
    InMemorySegmentContainer container;

    ASSERT_FALSE(container.GetOldestInMemSegment());
    ASSERT_EQ((size_t)0, container.GetTotalMemoryUse());

    MockInMemorySegmentPtr firstInMemSeg(new MockInMemorySegment());
    container.PushBack(firstInMemSeg);
    EXPECT_CALL(*firstInMemSeg, GetTotalMemoryUse()).WillRepeatedly(Return(10));

    ASSERT_EQ(firstInMemSeg, container.GetOldestInMemSegment());
    ASSERT_EQ((size_t)10, container.GetTotalMemoryUse());

    MockInMemorySegmentPtr secondInMemSeg(new MockInMemorySegment());
    container.PushBack(secondInMemSeg);
    EXPECT_CALL(*secondInMemSeg, GetTotalMemoryUse()).WillRepeatedly(Return(20));

    ASSERT_EQ(firstInMemSeg, container.GetOldestInMemSegment());
    ASSERT_EQ((size_t)30, container.GetTotalMemoryUse());

    container.EvictOldestInMemSegment();
    ASSERT_EQ(secondInMemSeg, container.GetOldestInMemSegment());
    ASSERT_EQ((size_t)20, container.GetTotalMemoryUse());
}
}} // namespace indexlib::partition
