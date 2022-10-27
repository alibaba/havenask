#ifndef __INDEXLIB_BUFFEREDSKIPLISTWRITERTEST_H
#define __INDEXLIB_BUFFEREDSKIPLISTWRITERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/buffered_skip_list_writer.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_skip_list_format.h"

IE_NAMESPACE_BEGIN(index);

class BufferedSkipListWriterTest : public INDEXLIB_TESTBASE {
public:
    BufferedSkipListWriterTest();
    ~BufferedSkipListWriterTest();

    DECLARE_CLASS_NAME(BufferedSkipListWriterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestNoCompressedFlush();
    void TestNoCompressedDump();
    void TestCompressedFlush();

private:
    index::DocListSkipListFormatPtr mDocListSkipListFormat;
    autil::mem_pool::Pool mByteSlicePool;
    autil::mem_pool::RecyclePool mBufferPool;
    std::string mDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BufferedSkipListWriterTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(BufferedSkipListWriterTest, TestNoCompressedFlush);
INDEXLIB_UNIT_TEST_CASE(BufferedSkipListWriterTest, TestCompressedFlush);
INDEXLIB_UNIT_TEST_CASE(BufferedSkipListWriterTest, TestNoCompressedDump);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BUFFEREDSKIPLISTWRITERTEST_H
