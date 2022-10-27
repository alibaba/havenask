#include <autil/mem_pool/RecyclePool.h>
#include <autil/mem_pool/ChunkAllocatorBase.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/truncate/reference_typed.h"
#include "indexlib/util/mmap_allocator.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(index);

class ReferenceTypedTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(ReferenceTypedTest);
    void CaseSetUp() override
    {
        mAllocator = new MMapAllocator();
        mBufferPool = new RecyclePool(mAllocator, DEFAULT_CHUNK_SIZE);
    }

    void CaseTearDown() override
    {
        if (mBufferPool)
        {
            delete mBufferPool;
            mBufferPool = NULL;
        }
        if (mAllocator)
        {
            delete mAllocator;
            mAllocator = NULL;
        }
    }

    void TestCaseForSimple()
    {
        uint32_t docInfoSize = sizeof(docid_t) + sizeof(int32_t);
        DocInfo* docInfo = (DocInfo*)mBufferPool->allocate(docInfoSize);
        ReferenceTyped<int32_t> referenceTyped(sizeof(docid_t), ft_int32);
        int32_t setValue  = 123;
        referenceTyped.Set(setValue, docInfo);
        int32_t getValue;
        referenceTyped.Get(docInfo, getValue);
        INDEXLIB_TEST_EQUAL((int32_t)123, getValue);
        std::string stringValue = referenceTyped.GetStringValue(docInfo);
        INDEXLIB_TEST_TRUE(stringValue.compare("123") == 0);
    }


private:
    ChunkAllocatorBase* mAllocator;
    RecyclePool* mBufferPool;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, ReferenceTypedTest);

INDEXLIB_UNIT_TEST_CASE(ReferenceTypedTest, TestCaseForSimple);

IE_NAMESPACE_END(index);
