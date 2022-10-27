#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/bitmap.h"
#include "indexlib/util/numeric_util.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_decoder.h"

using namespace std;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);

class BitmapPostingDecoderTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(BitmapPostingDecoderTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForDecodeWithOneDoc()
    {
        TestDecode(1);
    }

    void TestCaseForDecodeWithSomeDocs()
    {
        TestDecode(200);
    }

    void TestCaseForDecodeWithManyDocs()
    {
        TestDecode(80002);
    }


private:
    void TestDecode(uint32_t docNum)
    {
        TermMetaPtr termMeta;
        ByteSliceListPtr postingList;
        vector<docid_t> answer;
        CreateData(docNum, termMeta, postingList, answer);
        ByteSlice* byteSlice = postingList->GetHead();
        
        uint32_t stepLens[] = {100, 999, 100000};

        for (size_t i = 0; i < sizeof(stepLens) / sizeof(stepLens[0]); ++i)
        {
            BitmapPostingDecoderPtr decoder(new BitmapPostingDecoder);
            decoder->Init(termMeta.get(), byteSlice->data, byteSlice->size);
            CheckDecoder(decoder, stepLens[i], termMeta, answer);
        }
        postingList->Clear(NULL);
    }

    void CreateData(uint32_t docNum,
                    TermMetaPtr& termMeta, 
                    ByteSliceListPtr& postingList,
                    vector<docid_t>& answer)
    {
        srand(8888);

        termMeta.reset(new TermMeta());
        postingList.reset(new ByteSliceList);
        uint32_t dataSize = docNum / (Bitmap::BYTE_SLOT_NUM) + 1;
        dataSize = NumericUtil::UpperPack(dataSize, 4);
        ByteSlice* byteSlice = ByteSlice::CreateObject(dataSize);
        postingList->Add(byteSlice);
        
        Bitmap bitmap;
        memset(byteSlice->data, 0, byteSlice->size);
        uint32_t* uint32Data = (uint32_t*)byteSlice->data;
        bitmap.Mount(docNum, uint32Data);

        df_t df = 0;
        tf_t ttf = 0;
        for (uint32_t i = 0; i < docNum; ++i)
        {
            if (rand() % 8 <= 6)
            {
                bitmap.Set(i);
                answer.push_back(i);
                df++;
                ttf++;
            }
        }
        
        termMeta->SetDocFreq(df);
        termMeta->SetTotalTermFreq(ttf);
        termMeta->SetPayload(rand() % 800);
    }

    void CheckDecoder(BitmapPostingDecoderPtr& decoder,
                      uint32_t stepSize,
                      const TermMetaPtr& termMeta,
                      const vector<docid_t>& answer)
    {
        docid_t *docBuffer = new docid_t[stepSize];
        int32_t leftSize = answer.size();
        uint32_t cursor = 0;
        while (leftSize > 0)
        {
            uint32_t decodedCount = decoder->DecodeDocList(docBuffer, stepSize);
            INDEXLIB_TEST_TRUE(decodedCount <= stepSize);
            leftSize -= decodedCount;
            if (decodedCount < stepSize)
            {
                INDEXLIB_TEST_EQUAL(0, leftSize);
            }
            
            for (uint32_t i = 0; i < decodedCount; ++i, ++cursor)
            {
                INDEXLIB_TEST_EQUAL(answer[cursor], docBuffer[i]);
            }
        }
        delete []docBuffer;
    }
};

INDEXLIB_UNIT_TEST_CASE(BitmapPostingDecoderTest, TestCaseForDecodeWithOneDoc);
INDEXLIB_UNIT_TEST_CASE(BitmapPostingDecoderTest, TestCaseForDecodeWithSomeDocs);
INDEXLIB_UNIT_TEST_CASE(BitmapPostingDecoderTest, TestCaseForDecodeWithManyDocs);

IE_NAMESPACE_END(index);
