#include "indexlib/index/normal/inverted_index/format/skiplist/test/skip_list_reader_unittest_base.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/buffered_skip_list_writer.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/tri_value_skip_list_reader.h"
#include <autil/mem_pool/SimpleAllocatePolicy.h>
#include "indexlib/index_define.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_skip_list_format.h"

using namespace autil::mem_pool;

IE_NAMESPACE_USE(common);


IE_NAMESPACE_BEGIN(index);

SkipListReaderTestBase::SkipListReaderTestBase()
{
    mByteSlicePool = new Pool(DEFAULT_CHUNK_SIZE);
    mBufferPool = new RecyclePool(DEFAULT_CHUNK_SIZE);
}

SkipListReaderTestBase::~SkipListReaderTestBase()
{
    delete mByteSlicePool;
    mByteSlicePool = NULL;
        
    delete mBufferPool;
    mBufferPool = NULL;
}

void SkipListReaderTestBase::CaseSetup()
{
}

void SkipListReaderTestBase::CaseTearDown()
{
}

void SkipListReaderTestBase::TestCaseForSkipToNotEqual()
{
    // InternalTestCaseForSkipToNotEqual(1);
    // InternalTestCaseForSkipToNotEqual(MAX_UNCOMPRESSED_SKIP_LIST_SIZE);
    InternalTestCaseForSkipToNotEqual(33);
}

void SkipListReaderTestBase::TestCaseForSkipToEqual()
{
    InternalTestCaseForSkipToEqual(1);
    InternalTestCaseForSkipToEqual(MAX_UNCOMPRESSED_SKIP_LIST_SIZE);
    InternalTestCaseForSkipToEqual(100);
}

void SkipListReaderTestBase::TestCaseForGetSkippedItemCount()
{
    InternalTestCaseForGetSkippedItemCount(1);
    InternalTestCaseForGetSkippedItemCount(MAX_UNCOMPRESSED_SKIP_LIST_SIZE);
    InternalTestCaseForGetSkippedItemCount(100);
}

void SkipListReaderTestBase::TestCaseForSkipToEqualWithFlush()
{
    SCOPED_TRACE("Failed");

    InternalTestCaseForSkipToEqual(1, true);
    InternalTestCaseForSkipToEqual(MAX_UNCOMPRESSED_SKIP_LIST_SIZE, true);
    InternalTestCaseForSkipToEqual(100, true);
}

void SkipListReaderTestBase::TestCaseForSkipToNotEqualWithFlush()
{
    SCOPED_TRACE("Failed");

    InternalTestCaseForSkipToNotEqual(1, true);
    InternalTestCaseForSkipToNotEqual(MAX_UNCOMPRESSED_SKIP_LIST_SIZE, true);
    InternalTestCaseForSkipToNotEqual(100, true);        
}

void SkipListReaderTestBase::TestCaseForGetSkippedItemCountWithFlush()
{
    SCOPED_TRACE("Failed");

    InternalTestCaseForGetSkippedItemCount(1, true);
    InternalTestCaseForGetSkippedItemCount(MAX_UNCOMPRESSED_SKIP_LIST_SIZE, true);
    InternalTestCaseForGetSkippedItemCount(100, true);
}

void SkipListReaderTestBase::InternalTestCaseForSkipToEqual(uint32_t itemCount,
        bool needFlush)
{
    SkipListReaderPtr reader = PrepareReader(itemCount, needFlush);
    TriValueSkipListReaderPtr triSkipReader = 
        DYNAMIC_POINTER_CAST(TriValueSkipListReader, reader);

    uint32_t key;
    uint32_t offset;
    uint32_t delta;
    uint32_t prevKey;

    INDEXLIB_TEST_TRUE(reader->SkipTo(1, key, prevKey, offset, delta));
    INDEXLIB_TEST_EQUAL(prevKey, (uint32_t)0);
    INDEXLIB_TEST_EQUAL(key, (uint32_t)1);
    INDEXLIB_TEST_EQUAL(offset, (uint32_t)0);
    if (!triSkipReader || itemCount > 1) 
    {
        // tri_value_skip_list_reader optimize logic, no last offset & delta
        INDEXLIB_TEST_EQUAL(delta, (uint32_t)0);
    }

    for (uint32_t i = 1; i < itemCount; ++i)
    {
        INDEXLIB_TEST_TRUE(reader->SkipTo(i * 2 + 1, key, prevKey, offset, delta));
        INDEXLIB_TEST_EQUAL(prevKey, (uint32_t)(i * 2 - 1));
        INDEXLIB_TEST_EQUAL(key, (uint32_t)(i * 2 + 1));

        if (!triSkipReader || i < itemCount - 1)
        {
            // tri_value_skip_list_reader optimize logic, no last offset & delta
            INDEXLIB_TEST_EQUAL(offset + delta, (uint32_t)(i * 3));
            INDEXLIB_TEST_EQUAL(delta, (uint32_t)3);
            if (triSkipReader)
            {
                uint32_t expectPrevTTF = (i == 1 ? 5 : ((i - 1) * 5 + 1));
                INDEXLIB_TEST_EQUAL(triSkipReader->GetPrevTTF(), expectPrevTTF);
            }
        }
    }
}

void SkipListReaderTestBase::InternalTestCaseForSkipToNotEqual(uint32_t itemCount,
        bool needFlush)
{
    uint32_t key;
    uint32_t prevKey;
    uint32_t offset;
    uint32_t delta;

    SkipListReaderPtr reader = PrepareReader(itemCount, needFlush);
    TriValueSkipListReaderPtr triSkipReader = 
        DYNAMIC_POINTER_CAST(TriValueSkipListReader, reader);

    INDEXLIB_TEST_TRUE(reader->SkipTo(0, key, prevKey, offset, delta));
    INDEXLIB_TEST_EQUAL(prevKey, (uint32_t)0);
    INDEXLIB_TEST_EQUAL(key, (uint32_t)1);
    INDEXLIB_TEST_EQUAL(offset, (uint32_t)0);
    if (!triSkipReader || itemCount > 1) 
    {
        INDEXLIB_TEST_EQUAL(delta, (uint32_t)0);
    }

    for (uint32_t i = 1; i < itemCount; ++i)
    {
        INDEXLIB_TEST_TRUE(reader->SkipTo(i * 2, key, prevKey, offset, delta));
        INDEXLIB_TEST_EQUAL(prevKey, (uint32_t)(i * 2 - 1));
        INDEXLIB_TEST_EQUAL(key, (uint32_t)(i * 2 + 1));
        if (!triSkipReader || i < itemCount - 1)
        {
            // tri_value_skip_list_reader optimize logic, no last offset & delta
            INDEXLIB_TEST_EQUAL(offset + delta, (uint32_t)(i * 3));
            INDEXLIB_TEST_EQUAL(delta, (uint32_t)3);
            if (triSkipReader)
            {
                uint32_t expectPrevTTF = (i == 1 ? 5 : ((i - 1) * 5 + 1));
                INDEXLIB_TEST_EQUAL(triSkipReader->GetPrevTTF(), expectPrevTTF);
            }
        }

    }

    reader = PrepareReader(itemCount, needFlush);
    INDEXLIB_TEST_TRUE(!reader->SkipTo(itemCount * 2, key, prevKey, offset, delta));
}
    
void SkipListReaderTestBase::InternalTestCaseForGetSkippedItemCount(uint32_t itemCount,
        bool needFlush)
{
    SkipListReaderPtr reader = PrepareReader(itemCount, needFlush);

    uint32_t key;
    uint32_t prevKey;
    uint32_t offset;
    uint32_t delta;

    for (uint32_t i = 0; i <= itemCount; i += 2)
    {
        reader->SkipTo(i * 2, key, prevKey, offset, delta);
        INDEXLIB_TEST_EQUAL(reader->GetSkippedItemCount(), (int32_t)i);
    }
}

void SkipListReaderTestBase::TestCaseForGetLastValueInBuffer()
{
    //compress
    CheckGetLastValue(33, 0, 63, 93, true);    
    CheckGetLastValue(33, 63, 63, 93, true);    
    CheckGetLastValue(33, 65, 65, 96, true);    

    //no compress
    CheckGetLastValue(MAX_UNCOMPRESSED_SKIP_LIST_SIZE, 0, 19, 27, true);    
    CheckGetLastValue(MAX_UNCOMPRESSED_SKIP_LIST_SIZE, 5, 19, 27, true);    
    CheckGetLastValue(MAX_UNCOMPRESSED_SKIP_LIST_SIZE, 
                      MAX_UNCOMPRESSED_SKIP_LIST_SIZE, 19, 27, true);    

    CheckGetLastValue(32, 0, 63, 93, true);    

    //compress, less than buffer size
    CheckGetLastValue(25, 0, 49, 72, true);    
}

void SkipListReaderTestBase::TestCaseForGetLastValueInBufferForFlush()
{
    SCOPED_TRACE("Failed");

    //compress
    CheckGetLastValue(33, 100, 65, 96, false, true);

    //one value
    CheckGetLastValue(1, 100, 1, 0, false, true);

    //short list
    CheckGetLastValue(MAX_UNCOMPRESSED_SKIP_LIST_SIZE, 100, 19, 27, false, true);
}

void SkipListReaderTestBase::TestCaseForGetLastValueInBufferForSeekFailed()
{
    //compress
    CheckGetLastValue(33, 100, 65, 96, false);

    //one value
    CheckGetLastValue(1, 100, 1, 0, false);

    //short list
    CheckGetLastValue(MAX_UNCOMPRESSED_SKIP_LIST_SIZE, 100, 19, 27, false);
}



void SkipListReaderTestBase::CheckGetLastValue(uint32_t itemCount, uint32_t seekedKey,
        uint32_t lastKey, uint32_t lastValue, bool isSkipSuccess, bool needFlush)
{
    uint32_t key;
    uint32_t prevKey;
    uint32_t offset;
    uint32_t delta;

    SkipListReaderPtr reader = PrepareReader(itemCount, needFlush);
    INDEXLIB_TEST_EQUAL(isSkipSuccess, 
                        reader->SkipTo(seekedKey, key, prevKey, offset, delta));
    INDEXLIB_TEST_EQUAL(lastKey, reader->GetLastKeyInBuffer());    
    INDEXLIB_TEST_EQUAL(lastValue, reader->GetLastValueInBuffer());    
}

IE_NAMESPACE_END(index);
