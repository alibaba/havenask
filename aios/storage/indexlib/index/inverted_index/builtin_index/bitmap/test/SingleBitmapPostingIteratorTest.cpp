#include "indexlib/index/inverted_index/builtin_index/bitmap/SingleBitmapPostingIterator.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/MemFileNode.h"
#include "indexlib/file_system/file/MemFileNodeCreator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingWriter.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
using namespace indexlib::file_system;
using namespace indexlib::index;

namespace indexlibv2::index {
class SingleBitmapPostingIteratorTest : public TESTBASE
{
public:
    SingleBitmapPostingIteratorTest();
    ~SingleBitmapPostingIteratorTest() {}

public:
    void setUp() override;
    void tearDown() override;

    void TestCaseForSeekDocWithOnlyOneDoc();
    void TestCaseForSeekDoc();
    void TestCaseForSeekDocWithExpand();
    void TestCaseForUnpack();
    void TestCaseForUnpackWithManyDoc();
    void TestCaseForRealTimeInit();

private:
    void MakePostingData(const std::string& docIdStr, SingleBitmapPostingIterator* postIt,
                         std::vector<docid_t>* answer);

    void CheckPostingData(SingleBitmapPostingIterator& it, const std::vector<docid_t>& answer);

private:
    static const termpayload_t TERM_PAYLOAD_DEF = 9;
    static const uint32_t POOL_SIZE = 1000;
    static const uint32_t MAX_DOC_COUNT = 10000;

    indexlib::util::ObjectPool<SingleBitmapPostingIterator::InDocPositionStateType> _statePool;
    std::string _testDir;
    indexlib::file_system::MemFileNodePtr _fileReader;
};
SingleBitmapPostingIteratorTest::SingleBitmapPostingIteratorTest() {}

void SingleBitmapPostingIteratorTest::setUp() { _testDir = GET_TEMP_DATA_PATH(); }

void SingleBitmapPostingIteratorTest::tearDown() {}

void SingleBitmapPostingIteratorTest::TestCaseForSeekDocWithOnlyOneDoc()
{
    SingleBitmapPostingIterator it;
    string docIdStr = "1";
    vector<docid_t> answer;

    MakePostingData(docIdStr, &it, &answer);
    CheckPostingData(it, answer);
}

void SingleBitmapPostingIteratorTest::TestCaseForSeekDoc()
{
    SingleBitmapPostingIterator it;
    string docIdStr = "1, 4, 5, 7, 11, 55, 68";

    // for baseDocId == 0
    vector<docid_t> answer;
    MakePostingData(docIdStr, &it, &answer);
    CheckPostingData(it, answer);

    // for baseDocId != 0
    docid_t baseDocId = 100;
    for (size_t i = 0; i < answer.size(); ++i) {
        answer[i] += baseDocId;
    }
    it.SetBaseDocId(baseDocId);
    it.Reset();
    CheckPostingData(it, answer);
}

void SingleBitmapPostingIteratorTest::TestCaseForSeekDocWithExpand()
{
    SingleBitmapPostingIterator it;
    string docIdStr = "1, 4, 5, 7, 11, 55, 68";

    // for baseDocId == 0
    vector<docid_t> answer;
    MakePostingData(docIdStr, &it, &answer);
    CheckPostingData(it, answer);

    // for baseDocId != 0
    docid_t baseDocId = 100;
    for (size_t i = 0; i < answer.size(); ++i) {
        answer[i] += baseDocId;
    }
    it.SetBaseDocId(baseDocId);
    it.Reset();
    CheckPostingData(it, answer);
}

void SingleBitmapPostingIteratorTest::TestCaseForUnpack()
{
    SingleBitmapPostingIterator it;
    string docIdStr = "1, 4, 5, 7, 11, 55, 68";
    vector<docid_t> answer;
    MakePostingData(docIdStr, &it, &answer);

    docid_t docId = it.SeekDoc(0);
    TermMatchData tmd;
    it.Unpack(tmd);
    ASSERT_EQ((docpayload_t)0, tmd.GetDocPayload());

    docId = it.SeekDoc(1);
    it.Unpack(tmd);
    ASSERT_EQ((docpayload_t)0, tmd.GetDocPayload());

    docId = it.SeekDoc(56);
    (void)docId;
    it.Unpack(tmd);
    ASSERT_EQ((docpayload_t)0, tmd.GetDocPayload());
    ASSERT_TRUE(tmd.GetInDocPositionState() != NULL);
    std::shared_ptr<InDocPositionIterator> iterator(tmd.GetInDocPositionState()->CreateInDocIterator());
    ASSERT_EQ(INVALID_POSITION, iterator->SeekPosition(0));
}

void SingleBitmapPostingIteratorTest::TestCaseForUnpackWithManyDoc()
{
    stringstream docIdStr;
    for (int i = 0; i < 1000; ++i) {
        docIdStr << i * 2 + 1 << ",";
    }
    SingleBitmapPostingIterator it;
    vector<docid_t> answer;
    MakePostingData(docIdStr.str(), &it, &answer);

    docid_t docId = it.SeekDoc(0);
    ;
    TermMatchData tmd;
    it.Unpack(tmd);

    ASSERT_EQ((docpayload_t)0, tmd.GetDocPayload());
    std::shared_ptr<InDocPositionIterator> iterator(tmd.GetInDocPositionState()->CreateInDocIterator());
    ASSERT_EQ(INVALID_POSITION, iterator->SeekPosition(0));

    docId = it.SeekDoc(111);
    it.Unpack(tmd);
    ASSERT_EQ((docpayload_t)0, tmd.GetDocPayload());
    iterator.reset(tmd.GetInDocPositionState()->CreateInDocIterator());
    ASSERT_EQ(INVALID_POSITION, iterator->SeekPosition(0));

    docId = it.SeekDoc(1117);
    (void)docId;
    it.Unpack(tmd);
    ASSERT_EQ((docpayload_t)0, tmd.GetDocPayload());
    iterator.reset(tmd.GetInDocPositionState()->CreateInDocIterator());
    ASSERT_EQ(INVALID_POSITION, iterator->SeekPosition(0));
}

void SingleBitmapPostingIteratorTest::TestCaseForRealTimeInit()
{
    SCOPED_TRACE("Failed");

    BitmapPostingWriter postingWriter;
    for (size_t i = 0; i < 10; ++i) {
        postingWriter.AddPosition(0, 0, 0);
    }
    postingWriter.EndDocument(500, 0);

    SingleBitmapPostingIterator bitmapIter;
    bitmapIter.SetBaseDocId(1000);
    bitmapIter.Init(&postingWriter, nullptr);

    TermMeta* termMeta = bitmapIter.GetTermMeta();
    ASSERT_TRUE(termMeta);
    ASSERT_EQ((df_t)1, termMeta->GetDocFreq());
    ASSERT_EQ((termpayload_t)0, termMeta->GetPayload());
    ASSERT_EQ((tf_t)10, termMeta->GetTotalTermFreq());

    ASSERT_EQ((docid_t)999, bitmapIter.GetCurrentGlobalDocId());
    ASSERT_EQ((docid_t)1501, bitmapIter.GetLastDocId());

    ASSERT_EQ(postingWriter.GetBitmapData()->GetData(), bitmapIter._bitmap.GetData());

    ASSERT_TRUE(bitmapIter.Test(1500));
    ASSERT_FALSE(bitmapIter.Test(1499));
}

void SingleBitmapPostingIteratorTest::MakePostingData(const string& docIdStr, SingleBitmapPostingIterator* postIt,
                                                      std::vector<docid_t>* answer)
{
    _statePool.Init(POOL_SIZE);

    BitmapPostingWriter writer;
    StringTokenizer st(docIdStr, ",", StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);

    df_t df = 0;
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); ++it) {
        docid_t docId;
        StringUtil::strToInt32(it->c_str(), docId);
        writer.EndDocument(docId, docId % 10);
        answer->push_back(docId);
        df++;
    }

    writer.EndSegment();

    WriterOption writerOption;
    writerOption.atomicDump = false;
    writerOption.bufferSize = 1024u * 16;
    FileWriterPtr fileWriter(new BufferedFileWriter(writerOption, [](int64_t) {}));
    string fileName = _testDir + "test_posting";
    ASSERT_EQ(FSEC_OK, fileWriter->Open(fileName, fileName));

    // TermMeta termMeta(df, df, TERM_PAYLOAD_DEF);
    // termMeta.Dump(&fileWriter);
    writer.SetTermPayload(TERM_PAYLOAD_DEF);
    writer.Dump(fileWriter);

    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    _fileReader.reset(indexlib::file_system::MemFileNodeCreator::TEST_Create(fileName));
    int64_t fileLength = _fileReader->GetLength();
    ByteSliceListPtr sliceListPtr(_fileReader->ReadToByteSliceList(fileLength, 0, ReadOption()));
    assert(sliceListPtr);
    postIt->Init(sliceListPtr, nullptr, &_statePool);
    sliceListPtr->Clear(NULL);
}

void SingleBitmapPostingIteratorTest::CheckPostingData(SingleBitmapPostingIterator& it, const vector<docid_t>& answer)
{
    for (size_t i = 0; i < answer.size(); ++i) {
        docid_t seekedDocId = it.SeekDoc(answer[i]);
        assert(answer[i] == seekedDocId);
        ASSERT_EQ(answer[i], seekedDocId);
        ASSERT_EQ((docpayload_t)0, it.GetDocPayload());
    }

    termpayload_t expectPayload = TERM_PAYLOAD_DEF;
    ASSERT_EQ(expectPayload, it.GetTermMeta()->GetPayload());

    it.Reset();
    for (size_t i = 0; i < answer.size(); ++i) {
        docid_t seekedDocId = it.SeekDoc(answer[i] - 1);
        assert(answer[i] == seekedDocId);
        ASSERT_EQ(answer[i], seekedDocId);
        ASSERT_EQ((docpayload_t)0, it.GetDocPayload());
    }
}
TEST_F(SingleBitmapPostingIteratorTest, TestCaseForSeekDocWithOnlyOneDoc) { TestCaseForSeekDocWithOnlyOneDoc(); }
TEST_F(SingleBitmapPostingIteratorTest, TestCaseForSeekDoc) { TestCaseForSeekDoc(); }
TEST_F(SingleBitmapPostingIteratorTest, TestCaseForSeekDocWithExpand) { TestCaseForSeekDocWithExpand(); }
TEST_F(SingleBitmapPostingIteratorTest, TestCaseForUnpack) { TestCaseForUnpack(); }
TEST_F(SingleBitmapPostingIteratorTest, TestCaseForUnpackWithManyDoc) { TestCaseForUnpackWithManyDoc(); }
TEST_F(SingleBitmapPostingIteratorTest, TestCaseForRealTimeInit) { TestCaseForRealTimeInit(); }
} // namespace indexlibv2::index
