#ifndef __INDEXLIB_DOCLISTENCODERTEST_H
#define __INDEXLIB_DOCLISTENCODERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_encoder.h"
#include "indexlib/index/normal/inverted_index/accessor/position_bitmap_writer.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/buffered_skip_list_writer.h"
#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/RecyclePool.h>
#include <autil/mem_pool/SimpleAllocator.h>

IE_NAMESPACE_BEGIN(index);

class DocListEncoderTest : public INDEXLIB_TESTBASE {
public:
    class MockPositionBitmapWriter : public index::PositionBitmapWriter
    {
    public:
        MockPositionBitmapWriter()
            : index::PositionBitmapWriter(NULL) {}
    public:
        MOCK_METHOD1(Set, void(uint32_t index));
        MOCK_METHOD1(Resize, void(uint32_t size));
        MOCK_METHOD2(EndDocument, void(uint32_t df, uint32_t totalPosCount));
        MOCK_METHOD2(Dump, void(const file_system::FileWriterPtr& file,
                                uint32_t totalPosCount));
        MOCK_CONST_METHOD1(GetDumpLength, uint32_t(uint32_t bitCount));
    };
    
    class MockSkipListWriter : public index::BufferedSkipListWriter
    {
    public:
        MockSkipListWriter(autil::mem_pool::Pool* byteSlicePool,
                           autil::mem_pool::RecyclePool* bufferPool)
            : index::BufferedSkipListWriter(byteSlicePool, bufferPool)
        {}
    public:
        MOCK_METHOD1(Flush, size_t(uint8_t compressMode));
        MOCK_METHOD1(Dump, void(const file_system::FileWriterPtr& file));

    };
public:
    DocListEncoderTest();
    ~DocListEncoderTest();
public:
    void SetUp();
    void TearDown();
    void TestSimpleProcess();
    void TestAddPosition();
    void TestFlushDocListBuffer();
    void TestEndDocument();
    void TestFlush();
    void TestDump();
    void TestGetDumpLength();

private:
    void InnerTestDocListBufferManger(
            uint8_t bufferItemNum, bool hasTfList, 
            bool hasDocPayload, bool hasFieldMap);

    void InnerTestGetItemSize(
            bool hasTfList, bool hasDocPayload, 
            bool hasFieldMap, uint8_t expectItemSize);
    void InnerTestFlushDocListBuffer(uint8_t compressMode, bool hasSkipListWriter);
private:
    autil::mem_pool::Pool* mByteSlicePool;
    autil::mem_pool::RecyclePool* mBufferPool;
    util::SimplePool mSimplePool;
    autil::mem_pool::SimpleAllocator mAllocator;
    std::string mRootDir;
    std::string mDocListFile;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DocListEncoderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(DocListEncoderTest, TestAddPosition);
INDEXLIB_UNIT_TEST_CASE(DocListEncoderTest, TestFlushDocListBuffer);
INDEXLIB_UNIT_TEST_CASE(DocListEncoderTest, TestEndDocument);
INDEXLIB_UNIT_TEST_CASE(DocListEncoderTest, TestFlush);
INDEXLIB_UNIT_TEST_CASE(DocListEncoderTest, TestDump);
INDEXLIB_UNIT_TEST_CASE(DocListEncoderTest, TestGetDumpLength);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DOCLISTENCODERTEST_H
