#ifndef __INDEXLIB_DOCLISTENCODERPERFTEST_H
#define __INDEXLIB_DOCLISTENCODERPERFTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_encoder.h"

IE_NAMESPACE_BEGIN(index);

class DocListEncoderPerfTest : public INDEXLIB_TESTBASE {
public:
    DocListEncoderPerfTest();
    ~DocListEncoderPerfTest();
public:
    void SetUp();
    void TearDown();
    void TestDocListAddDocIdPerf();
    void TestDocListAddAllPerf();
    void TestDocListFlushPerf();

private:
    autil::mem_pool::Pool mByteSlicePool;
    autil::mem_pool::RecyclePool mBufferPool;
    util::SimplePool mSimplePool;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DocListEncoderPerfTest, TestDocListAddDocIdPerf);
INDEXLIB_UNIT_TEST_CASE(DocListEncoderPerfTest, TestDocListAddAllPerf);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DOCLISTENCODERPERFTEST_H
