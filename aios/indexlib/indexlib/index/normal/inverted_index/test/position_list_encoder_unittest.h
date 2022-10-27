#ifndef __INDEXLIB_POSITIONLISTENCODERTEST_H
#define __INDEXLIB_POSITIONLISTENCODERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_define.h"
#include "indexlib/index/normal/inverted_index/format/position_list_encoder.h"
#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/RecyclePool.h>
#include <autil/mem_pool/SimpleAllocator.h>

IE_NAMESPACE_BEGIN(index);

class PositionListEncoderTest : public INDEXLIB_TESTBASE {
public:
    class MockSkipListWriter : public index::BufferedSkipListWriter
    {
    public:
        MockSkipListWriter(autil::mem_pool::Pool* byteSlicePool,
                           autil::mem_pool::RecyclePool* bufferPool)
            :index::BufferedSkipListWriter(byteSlicePool, bufferPool)
        {}

    public:
        MOCK_METHOD1(Flush, size_t(uint8_t));
        MOCK_METHOD1(Dump, void(const file_system::FileWriterPtr& file));
    };
    DEFINE_SHARED_PTR(MockSkipListWriter);

public:
    PositionListEncoderTest();
    ~PositionListEncoderTest();

public:
    void SetUp();
    void TearDown();
    void TestSimpleProcess();
    void TestAddPosition();
    void TestAddPositionWithSkipList();
    void TestFlushPositionBuffer();
    void TestDump();

private:
    void InnerTestFlushPositionBuffer(
            uint8_t compressMode, bool hasTfBitmap,
            bool skipListWriterIsNull);
private:
    autil::mem_pool::Pool* mByteSlicePool;
    autil::mem_pool::RecyclePool* mBufferPool;
    autil::mem_pool::SimpleAllocator mAllocator;
    std::string mRootDir;
    std::string mPositionFile;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PositionListEncoderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(PositionListEncoderTest, TestAddPosition);
INDEXLIB_UNIT_TEST_CASE(PositionListEncoderTest, TestAddPositionWithSkipList);
INDEXLIB_UNIT_TEST_CASE(PositionListEncoderTest, TestFlushPositionBuffer);
INDEXLIB_UNIT_TEST_CASE(PositionListEncoderTest, TestDump);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSITIONLISTENCODERTEST_H
