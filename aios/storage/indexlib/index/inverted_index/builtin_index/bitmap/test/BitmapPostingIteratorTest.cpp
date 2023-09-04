#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingIterator.h"

#include <memory>

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/file/InterimFileWriter.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingWriter.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;

using namespace indexlib::file_system;
using namespace indexlib::util;

using namespace indexlib::index;

namespace indexlibv2::index {
enum CheckType { CT_BITMAP_UT_TEST, CT_BITMAP_UT_SEEK };

class BitmapPostingIteratorTest : public TESTBASE
{
public:
    BitmapPostingIteratorTest() {}
    ~BitmapPostingIteratorTest() {}

public:
    void setUp() override;
    void tearDown() override;
    void TestCaseForSeekDocWithOneCell();
    void TestCaseForSeekDocWithTwoCell();
    void TestCaseForSeekDocWithManyCell();

    void TestCaseForInitSingleIterator();

private:
    void CheckIterator(const std::vector<std::string>& strs, CheckType checkType,
                       optionflag_t optionFlag = OPTION_FLAG_ALL);

private:
    static const uint64_t KEY = 0x123456789ULL;
    static const termpayload_t TERM_PAYLOAD_DEF = 9;
    static const uint32_t POOL_SIZE = 1000;
    static const uint32_t MAX_DOC_COUNT = 10000;

    std::string _key;
    std::string _segmentInfosPath;
    PostingFormatOption _postingFormatOption;
};

void BitmapPostingIteratorTest::setUp()
{
    PostingFormatOption tmpFormatOption;
    _postingFormatOption = tmpFormatOption.GetBitmapPostingFormatOption();
}

void BitmapPostingIteratorTest::tearDown() {}

void BitmapPostingIteratorTest::TestCaseForSeekDocWithOneCell()
{
    string str1 = "1,2,7";
    vector<string> strs;
    strs.push_back(str1);
    CheckIterator(strs, CT_BITMAP_UT_SEEK);
    CheckIterator(strs, CT_BITMAP_UT_TEST);
}

void BitmapPostingIteratorTest::TestCaseForSeekDocWithTwoCell()
{
    string str1 = "1,2";
    string str2 = "111,112,117";
    vector<string> strs;
    strs.push_back(str1);
    strs.push_back(str2);
    CheckIterator(strs, CT_BITMAP_UT_SEEK);
    CheckIterator(strs, CT_BITMAP_UT_TEST);
}

void BitmapPostingIteratorTest::TestCaseForSeekDocWithManyCell()
{
    const int32_t cellCount[] = {5, 17, 79};
    for (size_t k = 0; k < sizeof(cellCount) / sizeof(int32_t); ++k) {
        vector<string> strs;
        int oldDocId = 0;
        for (int x = 0; x < cellCount[k]; ++x) {
            ostringstream oss;
            int docCount = 97 + rand() % 1234;
            for (int i = 0; i < docCount; ++i) {
                oss << (oldDocId += rand() % 19 + 1) << ",";
            }
            strs.push_back(oss.str());
        }
        CheckIterator(strs, CT_BITMAP_UT_SEEK);
        CheckIterator(strs, CT_BITMAP_UT_TEST);
    }
}

void BitmapPostingIteratorTest::TestCaseForInitSingleIterator()
{
    BitmapPostingWriter postingWriter;
    for (size_t i = 0; i < 10; ++i) {
        postingWriter.AddPosition(0, 0, 0);
    }
    postingWriter.EndDocument(99, 0);

    PostingFormatOption option;
    SegmentPosting segPosting(option);

    segPosting.Init(10000, 200, &postingWriter);

    shared_ptr<SegmentPostingVector> segPostings(new SegmentPostingVector);
    segPostings->push_back(segPosting);

    BitmapPostingIterator bitmapIter;
    bitmapIter.Init(segPostings, 1000);

    ASSERT_EQ((docid_t)10000, bitmapIter._curBaseDocId);
    ASSERT_EQ((docid_t)10100, bitmapIter._segmentLastDocId);
    ASSERT_EQ((docid_t)10100, bitmapIter._curLastDocId);
}

void BitmapPostingIteratorTest::CheckIterator(const vector<string>& strs, CheckType checkType, optionflag_t optionFlag)
{
    vector<docid_t> docIds;
    shared_ptr<SegmentPostingVector> segPostings(new SegmentPostingVector);
    docid_t baseDocId = 0;
    for (size_t i = 0; i < strs.size(); ++i) {
        BitmapPostingWriter writer;
        vector<docid_t> segDocIds;
        autil::StringTokenizer st(strs[i], ",",
                                  autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
        bool setBaseDocId = false;
        for (StringTokenizer::Iterator it = st.begin(); it != st.end(); ++it) {
            docid_t docId;
            StringUtil::strToInt32(it->c_str(), docId);
            if (!setBaseDocId) {
                baseDocId = docId;
                setBaseDocId = true;
            }
            writer.EndDocument(docId - baseDocId, docId % 10);
            docIds.push_back(docId);
        }
        writer.EndSegment();
        indexlib::file_system::InterimFileWriterPtr fileWriter(new indexlib::file_system::InterimFileWriter());
        fileWriter->Init(1024 * 16);
        uint32_t posLen = 0;
        fileWriter->WriteVUInt32(posLen).GetOrThrow();
        writer.SetTermPayload(TERM_PAYLOAD_DEF);
        writer.Dump(fileWriter);

        uint32_t docCount = (*docIds.rbegin()) - baseDocId + 1;
        SegmentPosting segPosting(_postingFormatOption);
        segPosting.Init(0, fileWriter->CopyToByteSliceList(true), baseDocId, docCount);
        segPostings->push_back(segPosting);
    }

    BitmapPostingIterator iter;
    iter.Init(segPostings, 1000);
    // iter.Test(0);
    docid_t oldDocId = 0;

    for (size_t i = 0; i < docIds.size(); ++i) {
        docid_t docId = docIds[i];
        if (checkType == CT_BITMAP_UT_TEST) {
            for (docid_t j = oldDocId + 1; j < docId; j += 2) {
                ASSERT_TRUE(!iter.Test(j));
            }
        }
        int dif = docId - oldDocId;
        oldDocId = docId;
        if (dif > 1) {
            docId -= rand() % (dif - 1) + 1;
        }
        if (checkType == CT_BITMAP_UT_SEEK) {
            ASSERT_EQ(docIds[i], iter.SeekDoc(docId));
            ASSERT_EQ(docIds[i], iter.GetCurrentGlobalDocId());
            TermMatchData tm;
            iter.Unpack(tm);
            ASSERT_EQ((size_t)1, (size_t)tm.GetTermFreq());
            tm.FreeInDocPositionState();
        } else {
            ASSERT_TRUE(iter.Test(docIds[i]));
            ASSERT_EQ(docIds[i], iter.GetCurrentGlobalDocId());
        }
    }
    if (checkType == CT_BITMAP_UT_SEEK) {
        ASSERT_EQ(INVALID_DOCID, iter.SeekDoc(oldDocId + 7));
        ASSERT_TRUE(oldDocId + 7 != iter.GetCurrentGlobalDocId());
    } else {
        ASSERT_TRUE(!iter.Test(oldDocId + 20));
        ASSERT_TRUE(oldDocId + 20 != iter.GetCurrentGlobalDocId());
    }
}
TEST_F(BitmapPostingIteratorTest, TestCaseForSeekDocWithOneCell) { TestCaseForSeekDocWithOneCell(); }
TEST_F(BitmapPostingIteratorTest, TestCaseForSeekDocWithTwoCell) { TestCaseForSeekDocWithTwoCell(); }
TEST_F(BitmapPostingIteratorTest, TestCaseForSeekDocWithManyCell) { TestCaseForSeekDocWithManyCell(); }
TEST_F(BitmapPostingIteratorTest, TestCaseForInitSingleIterator) { TestCaseForInitSingleIterator(); }
} // namespace indexlibv2::index
