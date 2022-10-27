#ifndef __INDEXLIB_SEGMENTPOSTINGTEST_H
#define __INDEXLIB_SEGMENTPOSTINGTEST_H

#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/RecyclePool.h>
#include <autil/mem_pool/SimpleAllocator.h>
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/simple_pool.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_posting.h"

IE_NAMESPACE_BEGIN(index);

class SegmentPostingTest : public INDEXLIB_TESTBASE 
{
public:
    SegmentPostingTest();
    ~SegmentPostingTest();

public:
    void SetUp();
    void TearDown();
    void TestEqual();
    void TestInitWithRealtime();

private:
    void DumpTermMeta(index::TermMeta& termMeta, std::string fileName);

private:
    std::string mRootDir;
    autil::mem_pool::Pool* mByteSlicePool;
    autil::mem_pool::RecyclePool* mBufferPool;
    util::SimplePool mSimplePool;
    autil::mem_pool::SimpleAllocator mAllocator;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SegmentPostingTest, TestEqual);
INDEXLIB_UNIT_TEST_CASE(SegmentPostingTest, TestInitWithRealtime);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SEGMENTPOSTINGTEST_H
