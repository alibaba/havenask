#ifndef __INDEXLIB_POSITIONLISTENCODERPERFTEST_H
#define __INDEXLIB_POSITIONLISTENCODERPERFTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/position_list_encoder.h"

IE_NAMESPACE_BEGIN(index);

class PositionListEncoderPerfTest : public INDEXLIB_TESTBASE {
public:
    PositionListEncoderPerfTest();
    ~PositionListEncoderPerfTest();
public:
    void SetUp();
    void TearDown();
    void TestAddPositionWithoutPayload();
    void TestAddAllPerf();
    void TestFlushPerf();

private:
    autil::mem_pool::Pool mByteSlicePool;
    autil::mem_pool::RecyclePool mBufferPool;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PositionListEncoderPerfTest, TestAddPositionWithoutPayload);
INDEXLIB_UNIT_TEST_CASE(PositionListEncoderPerfTest, TestAddAllPerf);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSITIONLISTENCODERPERFTEST_H
