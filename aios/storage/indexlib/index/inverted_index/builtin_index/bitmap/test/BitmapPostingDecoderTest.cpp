#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingDecoder.h"

#include "indexlib/util/Bitmap.h"
#include "indexlib/util/NumericUtil.h"
#include "unittest/unittest.h"

namespace indexlib::index {
namespace {
using util::Bitmap;
using util::ByteSlice;
using util::ByteSliceListPtr;
} // namespace

class BitmapPostingDecoderTest : public TESTBASE
{
public:
    void TestCaseForDecodeWithOneDoc() { TestDecode(1); }

    void TestCaseForDecodeWithSomeDocs() { TestDecode(200); }

    void TestCaseForDecodeWithManyDocs() { TestDecode(80002); }

private:
    void TestDecode(uint32_t docNum)
    {
        std::shared_ptr<TermMeta> termMeta;
        ByteSliceListPtr postingList;
        std::vector<docid_t> answer;
        CreateData(docNum, termMeta, postingList, answer);
        ByteSlice* byteSlice = postingList->GetHead();

        uint32_t stepLens[] = {100, 999, 100000};

        for (size_t i = 0; i < sizeof(stepLens) / sizeof(stepLens[0]); ++i) {
            std::shared_ptr<BitmapPostingDecoder> decoder(new BitmapPostingDecoder);
            decoder->Init(termMeta.get(), byteSlice->data, byteSlice->size);
            CheckDecoder(decoder, stepLens[i], termMeta, answer);
        }
        postingList->Clear(NULL);
    }

    void CreateData(uint32_t docNum, std::shared_ptr<TermMeta>& termMeta, ByteSliceListPtr& postingList,
                    std::vector<docid_t>& answer)
    {
        srand(8888);

        termMeta.reset(new TermMeta());
        postingList.reset(new util::ByteSliceList);
        uint32_t dataSize = docNum / (Bitmap::BYTE_SLOT_NUM) + 1;
        dataSize = util::NumericUtil::UpperPack(dataSize, 4);
        ByteSlice* byteSlice = ByteSlice::CreateObject(dataSize);
        postingList->Add(byteSlice);

        Bitmap bitmap;
        memset(byteSlice->data, 0, byteSlice->size);
        uint32_t* uint32Data = (uint32_t*)byteSlice->data;
        bitmap.Mount(docNum, uint32Data);

        df_t df = 0;
        tf_t ttf = 0;
        for (uint32_t i = 0; i < docNum; ++i) {
            if (rand() % 8 <= 6) {
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

    void CheckDecoder(std::shared_ptr<BitmapPostingDecoder>& decoder, uint32_t stepSize,
                      const std::shared_ptr<TermMeta>& termMeta, const std::vector<docid_t>& answer)
    {
        docid_t* docBuffer = new docid_t[stepSize];
        int32_t leftSize = answer.size();
        uint32_t cursor = 0;
        while (leftSize > 0) {
            uint32_t decodedCount = decoder->DecodeDocList(docBuffer, stepSize);
            ASSERT_TRUE(decodedCount <= stepSize);
            leftSize -= decodedCount;
            if (decodedCount < stepSize) {
                ASSERT_EQ(0, leftSize);
            }

            for (uint32_t i = 0; i < decodedCount; ++i, ++cursor) {
                ASSERT_EQ(answer[cursor], docBuffer[i]);
            }
        }
        delete[] docBuffer;
    }
};

TEST_F(BitmapPostingDecoderTest, TestCaseForDecodeWithOneDoc) { TestCaseForDecodeWithOneDoc(); }
TEST_F(BitmapPostingDecoderTest, TestCaseForDecodeWithSomeDocs) { TestCaseForDecodeWithSomeDocs(); }
TEST_F(BitmapPostingDecoderTest, TestCaseForDecodeWithManyDocs) { TestCaseForDecodeWithManyDocs(); }
} // namespace indexlib::index
