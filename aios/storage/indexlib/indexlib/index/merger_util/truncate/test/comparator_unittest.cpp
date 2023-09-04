#include "autil/mem_pool/ChunkAllocatorBase.h"
#include "autil/mem_pool/RecyclePool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/merger_util/truncate/comparator.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/MMapAllocator.h"

using namespace std;
using namespace autil::mem_pool;
using namespace indexlib::util;

namespace indexlib::index::legacy {

class ComparatorTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(ComparatorTest);
    void CaseSetUp() override
    {
        mAllocator = new MMapAllocator;
        mBufferPool = new RecyclePool(mAllocator, DEFAULT_CHUNK_SIZE);
    }

    void CaseTearDown() override
    {
        if (mBufferPool) {
            delete mBufferPool;
            mBufferPool = NULL;
        }
        if (mAllocator) {
            delete mAllocator;
            mAllocator = NULL;
        }
    }

    void TestCaseForSimple()
    {
        uint32_t docInfoSize = sizeof(bool) + sizeof(int32_t);
        {
            // not support null
            ReferenceTyped<int32_t> ref(0, ft_int32, false);
            DocInfo* docInfo = (DocInfo*)mBufferPool->allocate(docInfoSize);
            ref.Set(123, false, docInfo);
            DocInfo* docInfo2 = (DocInfo*)mBufferPool->allocate(docInfoSize);
            ref.Set(100, true, docInfo2);
            ComparatorTyped<int32_t> comparator(&ref, true);
            ASSERT_TRUE(comparator.LessThan(docInfo, docInfo2));
            ComparatorTyped<int32_t> comparator2(&ref, false);
            ASSERT_FALSE(comparator2.LessThan(docInfo, docInfo2));
        }

        {
            ReferenceTyped<int32_t> ref(0, ft_int32, true);
            // both not null
            DocInfo* docInfo = (DocInfo*)mBufferPool->allocate(docInfoSize);
            ref.Set(123, false, docInfo);
            DocInfo* docInfo2 = (DocInfo*)mBufferPool->allocate(docInfoSize);
            ref.Set(100, false, docInfo2);
            ComparatorTyped<int32_t> comparator(&ref, true);
            ASSERT_TRUE(comparator.LessThan(docInfo, docInfo2));
            ComparatorTyped<int32_t> comparator2(&ref, false);
            ASSERT_FALSE(comparator2.LessThan(docInfo, docInfo2));
        }
        {
            ReferenceTyped<int32_t> ref(0, ft_int32, true);
            // one doc is null
            DocInfo* docInfo = (DocInfo*)mBufferPool->allocate(docInfoSize);
            ref.Set(123, true, docInfo);
            DocInfo* docInfo2 = (DocInfo*)mBufferPool->allocate(docInfoSize);
            ref.Set(100, false, docInfo2);
            ComparatorTyped<int32_t> comparator(&ref, true);
            ASSERT_FALSE(comparator.LessThan(docInfo, docInfo2));
            ComparatorTyped<int32_t> comparator2(&ref, false);
            ASSERT_TRUE(comparator2.LessThan(docInfo, docInfo2));

            ref.Set(123, false, docInfo);
            ref.Set(100, true, docInfo2);
            ASSERT_TRUE(comparator.LessThan(docInfo, docInfo2));
            ASSERT_FALSE(comparator2.LessThan(docInfo, docInfo2));
        }
        {
            ReferenceTyped<int32_t> ref(0, ft_int32, true);
            // both null
            DocInfo* docInfo = (DocInfo*)mBufferPool->allocate(docInfoSize);
            ref.Set(123, true, docInfo);
            DocInfo* docInfo2 = (DocInfo*)mBufferPool->allocate(docInfoSize);
            ref.Set(100, true, docInfo2);
            ComparatorTyped<int32_t> comparator(&ref, true);
            ASSERT_FALSE(comparator.LessThan(docInfo, docInfo2));
            ComparatorTyped<int32_t> comparator2(&ref, false);
            ASSERT_FALSE(comparator2.LessThan(docInfo, docInfo2));
        }
    }

private:
    ChunkAllocatorBase* mAllocator;
    RecyclePool* mBufferPool;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, ComparatorTest);

INDEXLIB_UNIT_TEST_CASE(ComparatorTest, TestCaseForSimple);
} // namespace indexlib::index::legacy
