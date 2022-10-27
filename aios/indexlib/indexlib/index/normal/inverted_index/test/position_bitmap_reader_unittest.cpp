#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/numeric_compress/vbyte_compressor.h"
#include "indexlib/util/numeric_util.h"
#include "indexlib/util/expandable_bitmap.h"
#include "indexlib/common/byte_slice_writer.h"
#include "indexlib/index_define.h"
#include "indexlib/index/normal/inverted_index/accessor/position_bitmap_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/position_bitmap_writer.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/file_system/in_mem_file_node.h"
#include "indexlib/file_system/in_mem_file_node_creator.h"

using namespace std;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(index);

class PositionBitmapReaderTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(PositionBitmapReaderTest);
    void CaseSetUp() override
    {
        mDir = GET_TEST_DATA_PATH();
        mFileReader.reset(file_system::InMemFileNodeCreator::Create());
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForGetPosCountInfo()
    {
        DoTest(10, false);        
        DoTest(10, true);

        DoTest(MAX_DOC_PER_BITMAP_BLOCK - 1, false);
        DoTest(MAX_DOC_PER_BITMAP_BLOCK - 1, true);

        DoTest(MAX_DOC_PER_BITMAP_BLOCK, false);
        DoTest(MAX_DOC_PER_BITMAP_BLOCK, true);        

        DoTest(MAX_DOC_PER_BITMAP_BLOCK + 1, false);
        DoTest(MAX_DOC_PER_BITMAP_BLOCK + 1, true);        

        DoTest(MAX_DOC_PER_BITMAP_BLOCK * 3 + 10, false);
        DoTest(MAX_DOC_PER_BITMAP_BLOCK * 3 + 10, true);        
    }

    void TestCaseForGetPosCountInfo2()
    {
        util::SimplePool pool;
        PositionBitmapWriter writer(&pool);

        /*
         * first block:
         * 100111111....11111111111
         *    |<------255-------->|
         */
        writer.Set(0);
        writer.EndDocument(1, 3);
        for (uint32_t i = 0; i < 255; ++i)
        {
            writer.Set(i + 3);
            writer.EndDocument(i + 2, i + 4);
        }
        /*
         * second block:
         * 111111111111111111111111
         * |<--------100--------->|
         */
        for (uint32_t i = 0; i < 100; ++i)
        {
            writer.Set(i + 258);
            writer.EndDocument(i + 257, i + 259);
        }
        uint32_t posCount = 358;
        writer.Resize(posCount);

        string fileName = mDir + "bitmap";
        BufferedFileWriterPtr file(new BufferedFileWriter);
        file->Open(fileName);
        writer.Dump(file, posCount);
        file->Close();

        uint32_t fileLength = writer.GetDumpLength(posCount);
        mFileReader->Open(fileName, file_system::FSOT_IN_MEM);
        mFileReader->Populate();
        ByteSliceListPtr sliceListPtr(mFileReader->Read(fileLength, 0));

        PosCountInfo info;
        PositionBitmapReader reader;
        reader.Init(sliceListPtr.get(), 0);
        
        info = reader.GetPosCountInfo(300);
        INDEXLIB_TEST_EQUAL((uint32_t)301, info.preDocAggPosCount);
        INDEXLIB_TEST_EQUAL((uint32_t)1, info.currentDocPosCount);

        info = reader.GetPosCountInfo(356);
        INDEXLIB_TEST_EQUAL((uint32_t)357, info.preDocAggPosCount);
        INDEXLIB_TEST_EQUAL((uint32_t)1, info.currentDocPosCount);

        mFileReader->Close();
    }
    
    void DoTest(uint32_t docCount, bool random)
    {
        vector< pair<uint32_t, uint32_t> > answer;

        util::SimplePool pool;
        PositionBitmapWriter writer(&pool);
        pos_t posCount = 0;
        for (uint32_t i = 0; i < docCount; ++i)
        {
            pair<uint32_t, uint32_t> oneDocAnswer;
            oneDocAnswer.first = posCount;
            writer.Set(posCount);
            posCount++;
            if (random)
            {
                int r = rand() % 10;
                posCount += r;
                oneDocAnswer.second = r + 1;
            }
            else
            {
                posCount += i;
                oneDocAnswer.second = i + 1;
            }
            writer.EndDocument(i + 1, posCount);
            answer.push_back(oneDocAnswer);
        }
        writer.Resize(posCount);

        string fileName = mDir + "bitmap";
        BufferedFileWriterPtr file(new BufferedFileWriter);
        file->Open(fileName);
        writer.Dump(file, posCount);
        file->Close();

        uint32_t fileLength = writer.GetDumpLength(posCount);
        mFileReader->Open(fileName, file_system::FSOT_IN_MEM);
        mFileReader->Populate();
        ByteSliceListPtr sliceListPtr(mFileReader->Read(fileLength, 0));

        PosCountInfo info;
        PositionBitmapReader reader;
        reader.Init(sliceListPtr.get(), 0);
        for (uint32_t i = 0; i < docCount; i++)
        {
            info = reader.GetPosCountInfo(i + 1);
            INDEXLIB_TEST_EQUAL(info.preDocAggPosCount, answer[i].first);
            INDEXLIB_TEST_EQUAL(info.currentDocPosCount, answer[i].second);
        }
        mFileReader->Close();
    }

    
private:
    string mDir;
    file_system::InMemFileNodePtr mFileReader;
private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, PositionBitmapReaderTest);

INDEXLIB_UNIT_TEST_CASE(PositionBitmapReaderTest, TestCaseForGetPosCountInfo);
INDEXLIB_UNIT_TEST_CASE(PositionBitmapReaderTest, TestCaseForGetPosCountInfo2);

IE_NAMESPACE_END(index);
