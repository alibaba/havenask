#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingWriter.h"

#include "autil/memory.h"
#include "indexlib/file_system/ByteSliceReader.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/MemFileNode.h"
#include "indexlib/file_system/file/MemFileNodeCreator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"
#include "indexlib/util/NumericUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace indexlib::util;
using namespace indexlib::file_system;
using namespace indexlib::index;

namespace indexlibv2::index {

class BitmapPostingWriterTest : public TESTBASE
{
public:
    static const uint32_t MAX_DOC_COUNT = 100;

    BitmapPostingWriterTest() {}

    void setUp() override { _testDir = GET_TEMP_DATA_PATH(); }

    void TestCaseForEndDocumentWithoutDumpBuffer()
    {
        BitmapPostingWriter writer;
        vector<docid_t> answerDocIdSet;
        MakeWriterData(writer, MAX_DOC_COUNT, answerDocIdSet);

        const Bitmap& bitmap = writer.GetBitmap();
        for (size_t i = 0; i < answerDocIdSet.size(); ++i) {
            ASSERT_TRUE(bitmap.Test(answerDocIdSet[i]));
        }
    }

    void TestCaseForEndDocumentWithDumpBuffer()
    {
        BitmapPostingWriter writer;
        vector<docid_t> answerDocIdSet;
        MakeWriterData(writer, MAX_DOC_COUNT, answerDocIdSet);
        writer.EndSegment();

        string fileName = _testDir + "test_posting";
        WriterOption writerOption;
        writerOption.atomicDump = false;
        writerOption.bufferSize = 1024u * 16;
        FileWriterPtr fileWriter(new BufferedFileWriter(writerOption, [](int64_t) {}));
        EXPECT_EQ(FSEC_OK, fileWriter->Open(fileName, fileName));
        writer.Dump(fileWriter);
        ASSERT_EQ(FSEC_OK, fileWriter->Close());

        CheckPosting(answerDocIdSet);
    }

    void TestSortDump()
    {
        BitmapPostingWriter writer;
        vector<docid_t> answerDocIdSetOld;
        vector<docid_t> answerDocIdSetNew;
        MakeWriterData(writer, MAX_DOC_COUNT, answerDocIdSetOld);
        writer.SetTermPayload(114514);
        writer.EndSegment();
        ASSERT_EQ(answerDocIdSetOld.size(), writer.GetTotalTF());

        autil::mem_pool::Pool pool;
        vector<docid_t> old2new(MAX_DOC_COUNT, INVALID_DOCID);
        for (docid_t i = 0; i < MAX_DOC_COUNT; ++i) {
            old2new[i] = i == 0 ? MAX_DOC_COUNT - 1 : i - 1;
        }
        for (auto docid : answerDocIdSetOld) {
            answerDocIdSetNew.push_back(old2new[docid]);
        }
        BitmapPostingWriter dumpWriter;
        ASSERT_TRUE(writer.CreateReorderPostingWriter(&pool, &old2new, &dumpWriter));
        ASSERT_EQ(answerDocIdSetNew.size(), dumpWriter.GetTotalTF());
        ASSERT_EQ(answerDocIdSetNew.size(), dumpWriter.GetDF());
        ASSERT_EQ(114514, dumpWriter.GetTermPayload());

        string fileName = _testDir + "test_posting";
        WriterOption writerOption;
        writerOption.atomicDump = false;
        writerOption.bufferSize = 1024u * 16;
        FileWriterPtr fileWriter(new BufferedFileWriter(writerOption, [](int64_t) {}));
        EXPECT_EQ(FSEC_OK, fileWriter->Open(fileName, fileName));
        dumpWriter.Dump(fileWriter);
        ASSERT_EQ(FSEC_OK, fileWriter->Close());
        CheckPosting(answerDocIdSetNew);
    }

private:
    void MakeWriterData(BitmapPostingWriter& writer, uint32_t count, vector<docid_t>& answerDocIdSet)
    {
        for (uint32_t i = 0; i < count; ++i) {
            if (i % 5 != 0) {
                writer.AddPosition(0, 0, 0); // three 0s are all fake
                writer.EndDocument(i, 0);
                answerDocIdSet.push_back(i);
            }
        }
    }

    void CheckPosting(const vector<docid_t>& answerDocIdSet)
    {
        ExpandableBitmap bitmap;

        string fileName = _testDir + "test_posting";
        FileNodePtr fileReader(MemFileNodeCreator::TEST_Create(fileName));

        int64_t fileLength = FslibWrapper::GetFileLength(fileName).GetOrThrow();
        ByteSliceListPtr sliceListPtr(fileReader->ReadToByteSliceList(fileLength, 0, ReadOption()));
        assert(sliceListPtr);
        ByteSlice* slice = sliceListPtr->GetHead();
        uint8_t* dataCursor = slice->data;
        TermMeta termMeta(answerDocIdSet.size(), 0);
        TermMetaDumper tmDumper;
        dataCursor += tmDumper.CalculateStoreSize(termMeta);

        uint32_t bmSize = *(uint32_t*)dataCursor;
        dataCursor += sizeof(bmSize);

        docid_t maxDocId = answerDocIdSet.back();
        uint32_t expectedBmSize = NumericUtil::UpperPack(maxDocId + 1, Bitmap::SLOT_SIZE) / Bitmap::BYTE_SLOT_NUM;
        ASSERT_EQ(expectedBmSize, bmSize);

        uint32_t* bmData = new uint32_t[bmSize / sizeof(uint32_t)];
        memcpy((void*)bmData, (void*)dataCursor, bmSize);
        bitmap.Mount(bmSize * Bitmap::BYTE_SLOT_NUM, bmData, true);

        for (size_t i = 0; i < answerDocIdSet.size(); ++i) {
            ASSERT_TRUE(bitmap.Test(answerDocIdSet[i])) << answerDocIdSet[i];
        }

        delete[] bmData;
        sliceListPtr->Clear(NULL);
        ASSERT_EQ(FSEC_OK, fileReader->Close());
    }

    string _testDir;
};

TEST_F(BitmapPostingWriterTest, TestCaseForEndDocumentWithoutDumpBuffer) { TestCaseForEndDocumentWithoutDumpBuffer(); }
TEST_F(BitmapPostingWriterTest, TestCaseForEndDocumentWithDumpBuffer) { TestCaseForEndDocumentWithDumpBuffer(); }
TEST_F(BitmapPostingWriterTest, TestSortDump) { TestSortDump(); }
} // namespace indexlibv2::index
