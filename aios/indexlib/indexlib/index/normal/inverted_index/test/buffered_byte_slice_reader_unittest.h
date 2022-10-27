#ifndef __INDEXLIB_BUFFEREDBYTESLICEREADERTEST_H
#define __INDEXLIB_BUFFEREDBYTESLICEREADERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/buffered_byte_slice_reader.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_format.h"

IE_NAMESPACE_BEGIN(index);

class BufferedByteSliceReaderTest : public INDEXLIB_TESTBASE
{
public:
    BufferedByteSliceReaderTest();
    ~BufferedByteSliceReaderTest();

    DECLARE_CLASS_NAME(BufferedByteSliceReaderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestDecodeWithZeroCount();
    void TestDecode();
    void TestSeek();
    
private:
    BufferedByteSliceReaderPtr CreateReader(
            docid_t docId[],
            docpayload_t docPayload[],
            uint32_t docCount,
            uint32_t flushCount,
            uint8_t compressMode);

    BufferedByteSliceReaderPtr CreateReader(
            uint32_t docCount,
            uint32_t flushCount,
            uint8_t compressMode);

    void TestCheck(const uint32_t docCount, uint32_t flushCount, 
                   uint8_t compressMode);

    void CheckDecode(uint32_t docCount, uint32_t flushCount, 
                     BufferedByteSliceReaderPtr &reader);

private:
    autil::mem_pool::Pool* mByteSlicePool;
    autil::mem_pool::RecyclePool* mBufferPool;
    BufferedByteSlicePtr mBufferedByteSlice;
    DocListFormatPtr mDocListFormat;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BufferedByteSliceReaderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(BufferedByteSliceReaderTest, TestDecodeWithZeroCount);
INDEXLIB_UNIT_TEST_CASE(BufferedByteSliceReaderTest, TestDecode);
INDEXLIB_UNIT_TEST_CASE(BufferedByteSliceReaderTest, TestSeek);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BUFFEREDBYTESLICEREADERTEST_H
