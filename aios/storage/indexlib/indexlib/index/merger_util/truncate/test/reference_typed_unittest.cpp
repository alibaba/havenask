#include "autil/mem_pool/ChunkAllocatorBase.h"
#include "autil/mem_pool/RecyclePool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/merger_util/truncate/reference_typed.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/MMapAllocator.h"

using namespace std;
using namespace autil::mem_pool;

using namespace indexlib::util;
namespace indexlib::index::legacy {

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
        uint32_t docInfoSize = sizeof(docid_t) + sizeof(int32_t);
        DocInfo* docInfo = (DocInfo*)mBufferPool->allocate(docInfoSize);
        ReferenceTyped<int32_t> referenceTyped(sizeof(docid_t), ft_int32, false);
        int32_t setValue = 123;
        referenceTyped.Set(setValue, false, docInfo);
        int32_t getValue;
        bool isNull = false;
        referenceTyped.Get(docInfo, getValue, isNull);
        INDEXLIB_TEST_EQUAL((int32_t)123, getValue);
        ASSERT_FALSE(isNull);
        std::string stringValue = referenceTyped.GetStringValue(docInfo);
        INDEXLIB_TEST_TRUE(stringValue.compare("123") == 0);
    }

    void TestNullField()
    {
        uint32_t docInfoSize = sizeof(docid_t) + sizeof(int32_t) + sizeof(bool);
        DocInfo* docInfo = (DocInfo*)mBufferPool->allocate(docInfoSize);
        ReferenceTyped<int32_t> referenceTyped(sizeof(docid_t), ft_int32, true);
        int32_t setValue = 123;
        referenceTyped.Set(setValue, true, docInfo);
        bool isNull = false;
        int32_t getValue;
        referenceTyped.Get(docInfo, getValue, isNull);
        ASSERT_TRUE(isNull);
        std::string stringValue = referenceTyped.GetStringValue(docInfo);
        ASSERT_EQ("NULL", stringValue);
    }

private:
    ChunkAllocatorBase* mAllocator;
    RecyclePool* mBufferPool;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, ReferenceTypedTest);

INDEXLIB_UNIT_TEST_CASE(ReferenceTypedTest, TestCaseForSimple);
INDEXLIB_UNIT_TEST_CASE(ReferenceTypedTest, TestNullField);
} // namespace indexlib::index::legacy
