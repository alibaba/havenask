#include "indexlib/partition/segment/test/in_memory_segment_cleaner_unittest.h"

#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/partition/segment/in_memory_segment_container.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;

using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(segment, InMemorySegmentCleanerTest);

InMemorySegmentCleanerTest::InMemorySegmentCleanerTest() {}

InMemorySegmentCleanerTest::~InMemorySegmentCleanerTest() {}

void InMemorySegmentCleanerTest::CaseSetUp() {}

void InMemorySegmentCleanerTest::CaseTearDown() {}

namespace {
class MockInMemorySegment : public InMemorySegment
{
public:
    MockInMemorySegment()
        : InMemorySegment(config::BuildConfig(), util::MemoryQuotaControllerCreator::CreateBlockMemoryController(),
                          util::CounterMapPtr())
    {
    }
    ~MockInMemorySegment() {}
};
} // namespace

void InMemorySegmentCleanerTest::TestSimpleProcess()
{
    InMemorySegmentContainerPtr container(new InMemorySegmentContainer);
    container->PushBack(InMemorySegmentPtr(new MockInMemorySegment));
    InMemorySegmentPtr holdInMemSeg(new MockInMemorySegment);
    container->PushBack(holdInMemSeg);
    ASSERT_EQ((size_t)2, container->Size());

    InMemorySegmentCleaner cleaner(container);
    cleaner.Execute();
    ASSERT_EQ((size_t)1, container->Size());
    ASSERT_EQ(holdInMemSeg, container->GetOldestInMemSegment());

    holdInMemSeg.reset();
    cleaner.Execute();
    ASSERT_EQ((size_t)0, container->Size());
}
}} // namespace indexlib::partition
