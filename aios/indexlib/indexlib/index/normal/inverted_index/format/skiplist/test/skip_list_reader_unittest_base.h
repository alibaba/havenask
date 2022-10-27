#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/byte_slice_writer.h"
#include <autil/mem_pool/RecyclePool.h>
#include <autil/mem_pool/Pool.h>
#include "indexlib/index/normal/inverted_index/format/skiplist/skip_list_reader.h"

IE_NAMESPACE_BEGIN(index);

class SkipListReaderTestBase : public INDEXLIB_TESTBASE {

public:
    static const size_t DEFAULT_CHUNK_SIZE = 8 * 1024;

public:
    SkipListReaderTestBase();

    virtual ~SkipListReaderTestBase();

public:
    void CaseSetup();
    void CaseTearDown() override;

    void TestCaseForSkipToNotEqual();
    void TestCaseForSkipToEqual();
    void TestCaseForGetSkippedItemCount();

    void TestCaseForSkipToEqualWithFlush();
    void TestCaseForSkipToNotEqualWithFlush();
    void TestCaseForGetSkippedItemCountWithFlush();

    void TestCaseForGetLastValueInBuffer();
    void TestCaseForGetLastValueInBufferForFlush();
    void TestCaseForGetLastValueInBufferForSeekFailed();

protected:
    void InternalTestCaseForSkipToEqual(uint32_t itemCount, 
            bool needFlush = false);
    void InternalTestCaseForSkipToNotEqual(uint32_t itemCount, 
            bool needFlush = false);
    void InternalTestCaseForGetSkippedItemCount(uint32_t itemCount, 
            bool needFlush = false);

    void CheckGetLastValue(uint32_t itemCount, uint32_t seekedKey,
                           uint32_t lastKey, uint32_t lastValue, 
                           bool isSkipSuccess, bool needFlush = false);

protected:
    virtual SkipListReaderPtr PrepareReader(
            uint32_t itemCount, bool needFlush) = 0;

protected:
    autil::mem_pool::Pool* mByteSlicePool;
    autil::mem_pool::RecyclePool* mBufferPool;
};

IE_NAMESPACE_END(index);
