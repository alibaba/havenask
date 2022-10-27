#ifndef __INDEXLIB_NORMALINDEXREADERTEST_H
#define __INDEXLIB_NORMALINDEXREADERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_reader.h"
#include "indexlib/index/normal/inverted_index/test/mock_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"

IE_NAMESPACE_BEGIN(index);

class NormalIndexReaderTest : public INDEXLIB_TESTBASE 
{
public:
    NormalIndexReaderTest();
    ~NormalIndexReaderTest();

    DECLARE_CLASS_NAME(NormalIndexReaderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestFillTruncSegmentPostingWithTruncAndMain();
    void TestFillTruncSegmentPostingWithTruncAndNoMain();
    void TestFillTruncSegmentPostingWithNoTruncAndMain();
    void TestFillTruncSegmentPostingWithNoTruncAndNoMain();
    void TestLoadSegments();
    void TestCompressedPostingHeader();
    void TestCompressedPostingHeaderForCompatible();
    void TestIndexWithReferenceCompress();
    void TestIndexWithReferenceCompressForBuildingSegment();
    void TestIndexWithReferenceCompressWithMerge();
    void TestIndexWithReferenceCompressForSkiplist();
    void TestMmapLoadConfig();
    void TestCacheLoadConfig();
    void TestLookupWithMultiInMemSegments();
    void TestPartialLookup();

private:
    void PrepareSegmentPosting(MockNormalIndexReader &indexReader,
                               const index::TermMeta &termMeta);
    index::SegmentPosting CreateSegmentPosting(
            const index::TermMeta &termMeta);
    void CheckLoadSegment(config::IndexConfigPtr indexConfig,
                          std::string& segmentPath,
                          bool hasDictReader,
                          bool hasPostingReader, bool hasException);
    void innerTestIndexWithReferenceCompress(docid_t docCount, docid_t realtimeDocCount = 0);

    // termInfoStr: "index1:A"; docIdListStr: "0,1,..."
    void CheckPostingIterator(const index::IndexReaderPtr& indexReader, PostingType postingType,
        const std::string& termInfoStr, const std::string& docIdListStr,
        const DocIdRangeVector& ranges = {});

private:
    std::string mRootDir;
    MockNormalIndexReader mIndexReader;
    MockMultiFieldIndexReader mMultiFieldIndexReader;    
    MockNormalIndexReaderPtr mTruncateIndexReader;
    index::IndexReaderPtr mIndexReaderPtr;
    MockBitmapIndexReader *mBitmapIndexReader;
    //all test used mPostingFormatOption should equal
    PostingFormatOption mPostingFormatOption;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(NormalIndexReaderTest, 
                        TestFillTruncSegmentPostingWithTruncAndMain);
INDEXLIB_UNIT_TEST_CASE(NormalIndexReaderTest, 
                        TestFillTruncSegmentPostingWithTruncAndNoMain);
INDEXLIB_UNIT_TEST_CASE(NormalIndexReaderTest, 
                        TestFillTruncSegmentPostingWithNoTruncAndMain);
INDEXLIB_UNIT_TEST_CASE(NormalIndexReaderTest, 
                        TestFillTruncSegmentPostingWithNoTruncAndNoMain);
INDEXLIB_UNIT_TEST_CASE(NormalIndexReaderTest, TestLoadSegments);
INDEXLIB_UNIT_TEST_CASE(NormalIndexReaderTest, TestCompressedPostingHeader);
INDEXLIB_UNIT_TEST_CASE(NormalIndexReaderTest, TestCompressedPostingHeaderForCompatible);
INDEXLIB_UNIT_TEST_CASE(NormalIndexReaderTest, TestIndexWithReferenceCompress);
INDEXLIB_UNIT_TEST_CASE(NormalIndexReaderTest, TestIndexWithReferenceCompressForBuildingSegment);
INDEXLIB_UNIT_TEST_CASE(NormalIndexReaderTest, TestIndexWithReferenceCompressWithMerge);
INDEXLIB_UNIT_TEST_CASE(NormalIndexReaderTest, TestIndexWithReferenceCompressForSkiplist);
INDEXLIB_UNIT_TEST_CASE(NormalIndexReaderTest, TestMmapLoadConfig);
INDEXLIB_UNIT_TEST_CASE(NormalIndexReaderTest, TestCacheLoadConfig);
INDEXLIB_UNIT_TEST_CASE(NormalIndexReaderTest, TestLookupWithMultiInMemSegments);
INDEXLIB_UNIT_TEST_CASE(NormalIndexReaderTest, TestPartialLookup);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_NORMALINDEXREADERTEST_H
