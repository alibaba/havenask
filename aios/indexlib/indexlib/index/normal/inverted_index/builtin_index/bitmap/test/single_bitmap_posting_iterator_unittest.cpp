#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/test/single_bitmap_posting_iterator_unittest.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_writer.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/file_system/in_mem_file_node_creator.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);

SingleBitmapPostingIteratorTest::SingleBitmapPostingIteratorTest()
{
}

void SingleBitmapPostingIteratorTest::CaseSetUp()
{
    mTestDir = GET_TEST_DATA_PATH();
}

void SingleBitmapPostingIteratorTest::CaseTearDown()
{
}

void SingleBitmapPostingIteratorTest::TestCaseForSeekDocWithOnlyOneDoc()
{
    SingleBitmapPostingIterator it;
    string docIdStr = "1";
    vector<docid_t> answer;

    MakePostingData(it, docIdStr, answer);
    CheckPostingData(it, answer);
}

void SingleBitmapPostingIteratorTest::TestCaseForSeekDoc()
{
    SingleBitmapPostingIterator it;
    string docIdStr = "1, 4, 5, 7, 11, 55, 68";

    // for baseDocId == 0
    vector<docid_t> answer;
    MakePostingData(it, docIdStr, answer);
    CheckPostingData(it, answer);

    // for baseDocId != 0
    docid_t baseDocId = 100;
    for (size_t i = 0; i < answer.size(); ++i)
    {
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
    MakePostingData(it, docIdStr, answer);
        
    docid_t docId = it.SeekDoc(0);
    TermMatchData tmd;
    it.Unpack(tmd);
    INDEXLIB_TEST_EQUAL((docpayload_t)0, tmd.GetDocPayload());

    docId = it.SeekDoc(1);
    it.Unpack(tmd);
    INDEXLIB_TEST_EQUAL((docpayload_t)0, tmd.GetDocPayload());

    docId = it.SeekDoc(56);
    (void)docId;
    it.Unpack(tmd);
    INDEXLIB_TEST_EQUAL((docpayload_t)0, tmd.GetDocPayload());
    INDEXLIB_TEST_TRUE(tmd.GetInDocPositionState() != NULL);
    InDocPositionIteratorPtr iterator(tmd.GetInDocPositionState()->CreateInDocIterator());
    INDEXLIB_TEST_EQUAL(INVALID_POSITION, iterator->SeekPosition(0));
}

void SingleBitmapPostingIteratorTest::TestCaseForUnpackWithManyDoc()
{
    stringstream docIdStr;
    for (int i = 0; i < 1000; ++i)
    {
        docIdStr << i * 2 + 1 << ","; 
    }
    SingleBitmapPostingIterator it;
    vector<docid_t> answer;
    MakePostingData(it, docIdStr.str(), answer);

    docid_t docId = it.SeekDoc(0);;
    TermMatchData tmd;
    it.Unpack(tmd);

    INDEXLIB_TEST_EQUAL((docpayload_t)0, tmd.GetDocPayload());
    InDocPositionIteratorPtr iterator(tmd.GetInDocPositionState()->CreateInDocIterator());
    INDEXLIB_TEST_EQUAL(INVALID_POSITION, iterator->SeekPosition(0));

    docId = it.SeekDoc(111);
    it.Unpack(tmd);
    INDEXLIB_TEST_EQUAL((docpayload_t)0, tmd.GetDocPayload());
    iterator.reset(tmd.GetInDocPositionState()->CreateInDocIterator());
    INDEXLIB_TEST_EQUAL(INVALID_POSITION, iterator->SeekPosition(0));

    docId = it.SeekDoc(1117);
    (void)docId;
    it.Unpack(tmd);
    INDEXLIB_TEST_EQUAL((docpayload_t)0, tmd.GetDocPayload());
    iterator.reset(tmd.GetInDocPositionState()->CreateInDocIterator());
    INDEXLIB_TEST_EQUAL(INVALID_POSITION, iterator->SeekPosition(0));
}

void SingleBitmapPostingIteratorTest::TestCaseForRealTimeInit()
{
    SCOPED_TRACE("Failed");

    BitmapPostingWriter postingWriter;
    for (size_t i = 0; i < 10; ++i)
    {
        postingWriter.AddPosition(0, 0, 0);
    }
    postingWriter.EndDocument(500, 0);

    SingleBitmapPostingIterator bitmapIter;
    bitmapIter.SetBaseDocId(1000);
    bitmapIter.Init(&postingWriter, NULL);

    TermMeta* termMeta = bitmapIter.GetTermMeta();
    ASSERT_TRUE(termMeta);
    ASSERT_EQ((df_t)1, termMeta->GetDocFreq());
    ASSERT_EQ((termpayload_t)0, termMeta->GetPayload());
    ASSERT_EQ((tf_t)10, termMeta->GetTotalTermFreq());

    ASSERT_EQ((docid_t)999, bitmapIter.GetCurrentGlobalDocId());
    ASSERT_EQ((docid_t)1501, bitmapIter.GetLastDocId());

    ASSERT_EQ(postingWriter.GetBitmapData()->GetData(), bitmapIter.mData);

    ASSERT_TRUE(bitmapIter.Test(1500));
    ASSERT_FALSE(bitmapIter.Test(1499));
}

void SingleBitmapPostingIteratorTest::MakePostingData(
        SingleBitmapPostingIterator& postIt,
        const string& docIdStr, vector<docid_t>& answer)
{
    mStatePool.Init(POOL_SIZE);

    BitmapPostingWriter writer;
    StringTokenizer st(docIdStr, ",", StringTokenizer::TOKEN_TRIM 
                       | StringTokenizer::TOKEN_IGNORE_EMPTY);

    df_t df = 0;
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); ++it)
    {
        docid_t docId; 
        StringUtil::strToInt32(it->c_str(), docId);
        writer.EndDocument(docId, docId % 10);
        answer.push_back(docId);
        df++;
    }

    writer.EndSegment();

    file_system::FileWriterPtr fileWriter(
            new file_system::BufferedFileWriter(file_system::FSWriterParam(1024u * 16, false)));
    string fileName = mTestDir + "test_posting";
    fileWriter->Open(fileName);
        
    // TermMeta termMeta(df, df, TERM_PAYLOAD_DEF);
    // termMeta.Dump(&fileWriter);
    writer.SetTermPayload(TERM_PAYLOAD_DEF);
    writer.Dump(fileWriter);

    fileWriter->Close();

    mFileReader.reset(file_system::InMemFileNodeCreator::Create(fileName));
    int64_t fileLength = mFileReader->GetLength();
    ByteSliceListPtr sliceListPtr(mFileReader->Read(fileLength, 0));
    postIt.Init(sliceListPtr, &mStatePool);
    sliceListPtr->Clear(NULL);
}

void SingleBitmapPostingIteratorTest::CheckPostingData(
        SingleBitmapPostingIterator& it, const vector<docid_t>& answer)
{
    for (size_t i = 0; i < answer.size(); ++i)
    {
        docid_t seekedDocId = it.SeekDoc(answer[i]);
        assert(answer[i] == seekedDocId);
        INDEXLIB_TEST_EQUAL(answer[i], seekedDocId);
        INDEXLIB_TEST_EQUAL((docpayload_t)0, it.GetDocPayload());
    }

    termpayload_t expectPayload = TERM_PAYLOAD_DEF;
    INDEXLIB_TEST_EQUAL(expectPayload, it.GetTermMeta()->GetPayload());

    it.Reset();
    for (size_t i = 0; i < answer.size(); ++i)
    {
        docid_t seekedDocId = it.SeekDoc(answer[i] - 1);
        assert(answer[i] == seekedDocId);
        INDEXLIB_TEST_EQUAL(answer[i], seekedDocId);
        INDEXLIB_TEST_EQUAL((docpayload_t)0, it.GetDocPayload());
    }
}

IE_NAMESPACE_END(index);
