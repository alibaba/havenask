#include <fslib/fslib.h>
#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/test/bitmap_posting_maker.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/in_mem_bitmap_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_iterator.h"
#include "indexlib/config/single_field_index_config.h"

IE_NAMESPACE_BEGIN(index);

class MockBitmapIndexReader : public BitmapIndexReader
{
public:
    MockBitmapIndexReader();
    MOCK_CONST_METHOD1(CreateBitmapPostingIterator, BitmapPostingIterator*(
                           autil::mem_pool::Pool *sessionPool));
};

class MockInMemBitmapIndexSegmentReader : public InMemBitmapIndexSegmentReader
{
public:
MockInMemBitmapIndexSegmentReader()
    : InMemBitmapIndexSegmentReader(NULL, false)
    {}

    MOCK_CONST_METHOD4(GetSegmentPosting, bool(dictkey_t key, 
                                               docid_t baseDocId, SegmentPosting &segPosting,
                                               autil::mem_pool::Pool* sessionPool));
};

class MockBitmapPostingIterator : public BitmapPostingIterator
{
public:
    bool Init(const SegmentPostingVectorPtr& segPostings,
              const uint32_t statePoolSize)
    { mSegPostings = segPostings; return true; }

    SegmentPostingVectorPtr mSegPostings;
};

class BitmapIndexReaderTest : public INDEXLIB_TESTBASE {
public:
    typedef BitmapPostingMaker::Key2DocIdListMap Key2DocIdListMap;

public:
    BitmapIndexReaderTest() {}
    ~BitmapIndexReaderTest() {}

    DECLARE_CLASS_NAME(InMemBitmapIndexSegmentReaderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCaseForOneSegment();
    void TestCaseForMultiSegments();
    void TestCaseForBadDictionary();
    void TestCaseForBadPosting();
    void TestCaseForLookupWithRealtimeSegment();
    void TestBitMapReaderWithLoadConfig();
    
private:
    void CheckData(const BitmapIndexReaderPtr& reader, Key2DocIdListMap& answer);

    void CheckOnePosting(const BitmapIndexReaderPtr& reader,
                         const std::string& key,
                         const std::vector<docid_t>& docIdList);

    BitmapIndexReaderPtr OpenBitmapIndex(const std::string& dir);
    
    void WriteBadDataToFile(const std::string& fileName);

private:
    static const uint32_t MAX_DOC_COUNT_PER_SEG = 10000;
    std::string mTestDir;
};

INDEXLIB_UNIT_TEST_CASE(BitmapIndexReaderTest, TestCaseForOneSegment);
INDEXLIB_UNIT_TEST_CASE(BitmapIndexReaderTest, TestCaseForMultiSegments);
INDEXLIB_UNIT_TEST_CASE(BitmapIndexReaderTest, TestCaseForBadDictionary);
INDEXLIB_UNIT_TEST_CASE(BitmapIndexReaderTest, TestCaseForBadPosting);
INDEXLIB_UNIT_TEST_CASE(BitmapIndexReaderTest, TestCaseForLookupWithRealtimeSegment);

IE_NAMESPACE_END(index);

