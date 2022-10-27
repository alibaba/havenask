#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/byte_slice_reader.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/numeric_util.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_writer.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/file_system/in_mem_file_node.h"
#include "indexlib/file_system/in_mem_file_node_creator.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/index/normal/inverted_index/format/term_meta_dumper.h"

using namespace std;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(file_system);


IE_NAMESPACE_BEGIN(index);

class BitmapPostingWriterTest : public INDEXLIB_TESTBASE
{
public:
    static const uint32_t MAX_DOC_COUNT = 100;

    BitmapPostingWriterTest()
    {
    }

    DECLARE_CLASS_NAME(BitmapPostingWriterTest);
    void CaseSetUp() override
    {
        mTestDir = GET_TEST_DATA_PATH();
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForEndDocumentWithoutDumpBuffer()
    {
        BitmapPostingWriter writer;
        vector<docid_t> answerDocIdSet;
        MakeWriterData(writer, MAX_DOC_COUNT, answerDocIdSet);
        
        const Bitmap& bitmap = writer.GetBitmap();
        for (size_t i = 0; i < answerDocIdSet.size(); ++i)
        {
            INDEXLIB_TEST_TRUE(bitmap.Test(answerDocIdSet[i]));
        }        
    }

    void TestCaseForEndDocumentWithDumpBuffer()
    {
        BitmapPostingWriter writer;
        vector<docid_t> answerDocIdSet;
        MakeWriterData(writer, MAX_DOC_COUNT, answerDocIdSet);
        writer.EndSegment();

        string fileName = mTestDir + "test_posting";
        FileWriterPtr fileWriter(new BufferedFileWriter(FSWriterParam(1024u * 16, false)));
        fileWriter->Open(fileName);
        writer.Dump(fileWriter);
        fileWriter->Close();

        CheckPosting(answerDocIdSet);
    }


private:
    void MakeWriterData(BitmapPostingWriter& writer, uint32_t count, 
                        vector<docid_t>& answerDocIdSet)
    {
        for (uint32_t i = 0; i < count; ++i)
        {
            if (i % 5 != 0)
            {
                writer.EndDocument(i, 0);
                answerDocIdSet.push_back(i);
            }
        }
    }

    void CheckPosting(const vector<docid_t>& answerDocIdSet)
    {
        ExpandableBitmap bitmap;
        
        string fileName = mTestDir + "test_posting";
        file_system::FileNodePtr fileReader(file_system::InMemFileNodeCreator::Create(fileName));

        int64_t fileLength = storage::FileSystemWrapper::GetFileLength(fileName);
        ByteSliceListPtr sliceListPtr(fileReader->Read(fileLength, 0));
        ByteSlice* slice = sliceListPtr->GetHead();
        uint8_t* dataCursor = slice->data;
        TermMeta termMeta(answerDocIdSet.size(), 0);
        TermMetaDumper tmDumper;
        dataCursor += tmDumper.CalculateStoreSize(termMeta);

        uint32_t bmSize = *(uint32_t*)dataCursor;
        dataCursor += sizeof(bmSize);
        
        docid_t maxDocId = answerDocIdSet.back();
        uint32_t expectedBmSize = NumericUtil::UpperPack(maxDocId + 1, Bitmap::SLOT_SIZE)
                                  / Bitmap::BYTE_SLOT_NUM;
        INDEXLIB_TEST_EQUAL(expectedBmSize, bmSize);

        uint32_t* bmData = new uint32_t[bmSize / sizeof(uint32_t)];
        memcpy((void*)bmData, (void*)dataCursor, bmSize);
        bitmap.Mount(bmSize * Bitmap::BYTE_SLOT_NUM, bmData, true);

        for (size_t i = 0; i < answerDocIdSet.size(); ++i)
        {
            INDEXLIB_TEST_TRUE(bitmap.Test(answerDocIdSet[i]));
        }        

        delete []bmData;
        sliceListPtr->Clear(NULL);
        fileReader->Close();
    }

    string mTestDir;
};

INDEXLIB_UNIT_TEST_CASE(BitmapPostingWriterTest, TestCaseForEndDocumentWithoutDumpBuffer);
INDEXLIB_UNIT_TEST_CASE(BitmapPostingWriterTest, TestCaseForEndDocumentWithDumpBuffer);

IE_NAMESPACE_END(index);
