#include <tr1/memory>
#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_iterator.h"

IE_NAMESPACE_BEGIN(index);

enum CheckType
{
    CT_BITMAP_UT_TEST,
    CT_BITMAP_UT_SEEK
};

class BitmapPostingIteratorTest : public INDEXLIB_TESTBASE
{
public:
    BitmapPostingIteratorTest() {}
    ~BitmapPostingIteratorTest() {}

    DECLARE_CLASS_NAME(BitmapPostingIteratorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCaseForSeekDocWithOneCell();
    void TestCaseForSeekDocWithTwoCell();
    void TestCaseForSeekDocWithManyCell();

    void TestCaseForInitSingleIterator();

private:
    void CheckIterator(const std::vector<std::string>& strs,
                       CheckType checkType,
                       optionflag_t optionFlag = OPTION_FLAG_ALL);

private:
    static const uint64_t KEY = 0x123456789ULL;
    static const termpayload_t TERM_PAYLOAD_DEF = 9;
    static const uint32_t POOL_SIZE = 1000;
    static const uint32_t MAX_DOC_COUNT = 10000;

    std::string mKey;
    std::string mSegmentInfosPath;
    index_base::SegmentInfos mSegmentInfos;
    index::PostingFormatOption mPostingFormatOption;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BitmapPostingIteratorTest, TestCaseForSeekDocWithOneCell);
INDEXLIB_UNIT_TEST_CASE(BitmapPostingIteratorTest, TestCaseForSeekDocWithTwoCell);
INDEXLIB_UNIT_TEST_CASE(BitmapPostingIteratorTest, TestCaseForSeekDocWithManyCell);
INDEXLIB_UNIT_TEST_CASE(BitmapPostingIteratorTest, TestCaseForInitSingleIterator);

IE_LOG_SETUP(index, BitmapPostingIteratorTest);

IE_NAMESPACE_END(index);
