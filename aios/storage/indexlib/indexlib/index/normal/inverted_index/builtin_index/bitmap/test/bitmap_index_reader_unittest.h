#pragma once
#include "fslib/fslib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/InMemBitmapIndexSegmentReader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_index_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/test/bitmap_posting_maker.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index { namespace legacy {

class MockBitmapIndexReader : public BitmapIndexReader
{
public:
    MockBitmapIndexReader();
    MOCK_METHOD(index::BitmapPostingIterator*, CreateBitmapPostingIterator, (autil::mem_pool::Pool * sessionPool),
                (const, override));
};

class MockInMemBitmapIndexSegmentReader : public index::InMemBitmapIndexSegmentReader
{
public:
    MockInMemBitmapIndexSegmentReader() : index::InMemBitmapIndexSegmentReader(NULL, false) {}

    MOCK_METHOD(bool, GetSegmentPosting,
                (const index::DictKeyInfo& key, docid64_t baseDocId, SegmentPosting& segPosting,
                 autil::mem_pool::Pool* sessionPool, file_system::ReadOption, InvertedIndexSearchTracer*),
                (const, override));
};

class MockBitmapPostingIterator : public index::BitmapPostingIterator
{
public:
    bool Init(const std::shared_ptr<SegmentPostingVector>& segPostings, const uint32_t statePoolSize)
    {
        mSegPostings = segPostings;
        return true;
    }

    std::shared_ptr<SegmentPostingVector> mSegPostings;
};

class BitmapIndexReaderTest : public INDEXLIB_TESTBASE
{
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

    void CheckOnePosting(const BitmapIndexReaderPtr& reader, const std::string& key,
                         const std::vector<docid_t>& docIdList);

    BitmapIndexReaderPtr OpenBitmapIndex(const std::string& dir, const std::string& fieldName);

    void WriteBadDataToFile(const indexlib::file_system::DirectoryPtr& rootDir, const std::string& fileName);

private:
    static const uint32_t MAX_DOC_COUNT_PER_SEG = 10000;
    std::string mTestDir;
};

INDEXLIB_UNIT_TEST_CASE(BitmapIndexReaderTest, TestCaseForOneSegment);
INDEXLIB_UNIT_TEST_CASE(BitmapIndexReaderTest, TestCaseForMultiSegments);
INDEXLIB_UNIT_TEST_CASE(BitmapIndexReaderTest, TestCaseForBadDictionary);
INDEXLIB_UNIT_TEST_CASE(BitmapIndexReaderTest, TestCaseForBadPosting);
INDEXLIB_UNIT_TEST_CASE(BitmapIndexReaderTest, TestCaseForLookupWithRealtimeSegment);
}}} // namespace indexlib::index::legacy
