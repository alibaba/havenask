#ifndef __INDEXLIB_BUFFEREDINDEXDECODERTEST_H
#define __INDEXLIB_BUFFEREDINDEXDECODERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/buffered_index_decoder.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_posting.h"

IE_NAMESPACE_BEGIN(index);

class BufferedIndexDecoderTest : public INDEXLIB_TESTBASE {
public:
    BufferedIndexDecoderTest();
    ~BufferedIndexDecoderTest();

    DECLARE_CLASS_NAME(BufferedIndexDecoderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestGetSegmentBaseDocId();
    void TestLocateSegment();

private:
    void PrepareSegmentPostings();

private:
    SegmentPostingVectorPtr mSegPostings;
    index::PostingFormatOption mFormatOption;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BufferedIndexDecoderTest, TestGetSegmentBaseDocId);
INDEXLIB_UNIT_TEST_CASE(BufferedIndexDecoderTest, TestLocateSegment);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BUFFEREDINDEXDECODERTEST_H
