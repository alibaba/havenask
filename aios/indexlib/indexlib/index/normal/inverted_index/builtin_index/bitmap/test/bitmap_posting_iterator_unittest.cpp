#include <autil/StringUtil.h>
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_writer.h"
#include "indexlib/common/in_mem_file_writer.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/test/bitmap_posting_iterator_unittest.h"

using namespace std;
using namespace std::tr1;
using namespace autil;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);

void BitmapPostingIteratorTest::CaseSetUp()
{
    PostingFormatOption tmpFormatOption;
    mPostingFormatOption = tmpFormatOption.GetBitmapPostingFormatOption();
}

void BitmapPostingIteratorTest::CaseTearDown()
{
}

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
    for(size_t k = 0; k < sizeof(cellCount) / sizeof(int32_t); ++k)
    {
        vector<string> strs;
        int oldDocId = 0;
        for (int x = 0; x < cellCount[k]; ++x)
        {
            ostringstream oss;
            int docCount = 97 + rand() % 1234;
            for (int i = 0; i < docCount; ++i)
            {
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
    for (size_t i = 0; i < 10; ++i)
    {
        postingWriter.AddPosition(0, 0, 0);
    }
    postingWriter.EndDocument(99, 0);

    PostingFormatOption option;
    SegmentPosting segPosting(option);

    segPosting.Init(10000, 200, &postingWriter);

    SegmentPostingVectorPtr segPostings(new SegmentPostingVector);
    segPostings->push_back(segPosting);

    BitmapPostingIterator bitmapIter;
    bitmapIter.Init(segPostings, 1000);

    ASSERT_EQ((docid_t)10000, bitmapIter.mCurBaseDocId);
    ASSERT_EQ((docid_t)10100, bitmapIter.mSegmentLastDocId);
    ASSERT_EQ((docid_t)10100, bitmapIter.mCurLastDocId);

}

void BitmapPostingIteratorTest::CheckIterator(const vector<string>& strs,
        CheckType checkType, optionflag_t optionFlag)
{
    vector<docid_t> docIds;
    SegmentPostingVectorPtr segPostings(new SegmentPostingVector);
    docid_t baseDocId = 0;
    for (size_t i = 0; i < strs.size(); ++i)
    {
        BitmapPostingWriter writer;
        vector<docid_t> segDocIds;
        autil::StringTokenizer st(strs[i], ",", 
                autil::StringTokenizer::TOKEN_IGNORE_EMPTY
                | autil::StringTokenizer::TOKEN_TRIM);
        bool setBaseDocId = false;
        for (StringTokenizer::Iterator it = st.begin(); it != st.end(); ++it)
        {
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
        common::InMemFileWriterPtr fileWriter(new common::InMemFileWriter());
        fileWriter->Init(1024 * 16);
        uint32_t posLen = 0;
        fileWriter->WriteVUInt32(posLen);
        writer.SetTermPayload(TERM_PAYLOAD_DEF);
        writer.Dump(fileWriter);

        uint32_t docCount = (*docIds.rbegin()) - baseDocId + 1;
        SegmentPosting segPosting(mPostingFormatOption);
        SegmentInfo segmentInfo;
        segmentInfo.docCount = docCount;
        segPosting.Init(0, fileWriter->CopyToByteSliceList(true), 
                        baseDocId, segmentInfo);
        segPostings->push_back(segPosting);
    }

    BitmapPostingIterator iter;
    iter.Init(segPostings, 1000);
    // iter.Test(0);
    docid_t oldDocId = 0;

    for (size_t i = 0; i < docIds.size(); ++i)
    {
        docid_t docId = docIds[i];
        if (checkType == CT_BITMAP_UT_TEST)
        {
            for (docid_t j = oldDocId + 1; j < docId; j += 2)
            {
                INDEXLIB_TEST_TRUE(!iter.Test(j));
            }
        }
        int dif = docId - oldDocId;
        oldDocId = docId;
        if (dif > 1)
        {
            docId -= rand() % (dif - 1) + 1;
        }
        if (checkType == CT_BITMAP_UT_SEEK)
        {
            INDEXLIB_TEST_EQUAL(docIds[i], iter.SeekDoc(docId));
            INDEXLIB_TEST_EQUAL(docIds[i], 
                    iter.GetCurrentGlobalDocId());
            TermMatchData tm;
            iter.Unpack(tm);
            INDEXLIB_TEST_EQUAL((size_t)1, (size_t)tm.GetTermFreq());
            tm.FreeInDocPositionState();
        }
        else
        {
            INDEXLIB_TEST_TRUE(iter.Test(docIds[i]));
            INDEXLIB_TEST_EQUAL(docIds[i], 
                    iter.GetCurrentGlobalDocId());
        }
    }
    if (checkType == CT_BITMAP_UT_SEEK)
    {
        INDEXLIB_TEST_EQUAL(INVALID_DOCID, iter.SeekDoc(oldDocId + 7)); 
        INDEXLIB_TEST_TRUE(oldDocId + 7 != 
                           iter.GetCurrentGlobalDocId());
    }
    else
    {
        INDEXLIB_TEST_TRUE(!iter.Test(oldDocId + 20));
        INDEXLIB_TEST_TRUE(oldDocId + 20 != 
                           iter.GetCurrentGlobalDocId());
    }
}

IE_NAMESPACE_END(index);
