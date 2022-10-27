#include "indexlib/partition/segment/test/in_memory_segment_cleaner_unittest.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/partition/segment/in_memory_segment_container.h"

using namespace std;

IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(segment, InMemorySegmentCleanerTest);

InMemorySegmentCleanerTest::InMemorySegmentCleanerTest()
{
}

InMemorySegmentCleanerTest::~InMemorySegmentCleanerTest()
{
}

void InMemorySegmentCleanerTest::CaseSetUp()
{
}

void InMemorySegmentCleanerTest::CaseTearDown()
{
}

namespace {
class MockInMemorySegment : public InMemorySegment
{
public:
    MockInMemorySegment()
        : InMemorySegment(config::BuildConfig(),            
                          util::MemoryQuotaControllerCreator::CreateBlockMemoryController(),
                          util::CounterMapPtr())
    {}
    ~MockInMemorySegment() {}
};
}

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

IE_NAMESPACE_END(partition);

