#include "indexlib/index/inverted_index/format/PositionBitmapReader.h"

#include "indexlib/file_system/ByteSliceWriter.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/MemFileNode.h"
#include "indexlib/file_system/file/MemFileNodeCreator.h"
#include "indexlib/index/common/numeric_compress/VbyteCompressor.h"
#include "indexlib/index/inverted_index/format/PositionBitmapWriter.h"
#include "indexlib/util/ExpandableBitmap.h"
#include "indexlib/util/NumericUtil.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class PositionBitmapReaderTest : public TESTBASE
{
public:
    void setUp() override
    {
        _dir = GET_TEMP_DATA_PATH();
        _fileReader.reset(file_system::MemFileNodeCreator::TEST_Create());
    }
    void tearDown() override {}

    void DoTest(uint32_t docCount, bool random)
    {
        std::vector<std::pair<uint32_t, uint32_t>> answer;

        util::SimplePool pool;
        PositionBitmapWriter writer(&pool);
        pos_t posCount = 0;
        for (uint32_t i = 0; i < docCount; ++i) {
            std::pair<uint32_t, uint32_t> oneDocAnswer;
            oneDocAnswer.first = posCount;
            writer.Set(posCount);
            posCount++;
            if (random) {
                int r = rand() % 10;
                posCount += r;
                oneDocAnswer.second = r + 1;
            } else {
                posCount += i;
                oneDocAnswer.second = i + 1;
            }
            writer.EndDocument(i + 1, posCount);
            answer.push_back(oneDocAnswer);
        }
        writer.Resize(posCount);

        std::string fileName = _dir + "bitmap";
        std::shared_ptr<file_system::BufferedFileWriter> file(new file_system::BufferedFileWriter);
        ASSERT_EQ(file_system::FSEC_OK, file->Open(fileName, fileName));
        writer.Dump(file, posCount);
        ASSERT_EQ(file_system::FSEC_OK, file->Close());

        uint32_t fileLength = writer.GetDumpLength(posCount);
        ASSERT_EQ(file_system::FSEC_OK, _fileReader->Close());
        ASSERT_EQ(file_system::FSEC_OK, _fileReader->Open("", fileName, file_system::FSOT_MEM, -1));
        ASSERT_EQ(file_system::FSEC_OK, _fileReader->Populate());
        util::ByteSliceListPtr sliceListPtr(_fileReader->ReadToByteSliceList(fileLength, 0, file_system::ReadOption()));
        assert(sliceListPtr);

        PosCountInfo info;
        PositionBitmapReader reader;
        reader.Init(sliceListPtr.get(), 0);
        for (uint32_t i = 0; i < docCount; i++) {
            info = reader.GetPosCountInfo(i + 1);
            ASSERT_EQ(info.preDocAggPosCount, answer[i].first);
            ASSERT_EQ(info.currentDocPosCount, answer[i].second);
        }
        ASSERT_EQ(file_system::FSEC_OK, _fileReader->Close());
    }

private:
    std::string _dir;
    std::shared_ptr<file_system::MemFileNode> _fileReader;
};

TEST_F(PositionBitmapReaderTest, testCaseForGetPosCountInfo)
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

TEST_F(PositionBitmapReaderTest, testCaseForGetPosCountInfo2)
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
    for (uint32_t i = 0; i < 255; ++i) {
        writer.Set(i + 3);
        writer.EndDocument(i + 2, i + 4);
    }
    /*
     * second block:
     * 111111111111111111111111
     * |<--------100--------->|
     */
    for (uint32_t i = 0; i < 100; ++i) {
        writer.Set(i + 258);
        writer.EndDocument(i + 257, i + 259);
    }
    uint32_t posCount = 358;
    writer.Resize(posCount);

    std::string fileName = _dir + "bitmap";
    std::shared_ptr<file_system::BufferedFileWriter> file(new file_system::BufferedFileWriter);
    ASSERT_EQ(file_system::FSEC_OK, file->Open(fileName, fileName));
    writer.Dump(file, posCount);
    ASSERT_EQ(file_system::FSEC_OK, file->Close());

    uint32_t fileLength = writer.GetDumpLength(posCount);
    ASSERT_EQ(file_system::FSEC_OK, _fileReader->Open("", fileName, file_system::FSOT_MEM, -1));
    ASSERT_EQ(file_system::FSEC_OK, _fileReader->Populate());
    std::shared_ptr<util::ByteSliceList> sliceListPtr(
        _fileReader->ReadToByteSliceList(fileLength, 0, file_system::ReadOption()));
    assert(sliceListPtr);

    PosCountInfo info;
    PositionBitmapReader reader;
    reader.Init(sliceListPtr.get(), 0);

    info = reader.GetPosCountInfo(300);
    ASSERT_EQ((uint32_t)301, info.preDocAggPosCount);
    ASSERT_EQ((uint32_t)1, info.currentDocPosCount);

    info = reader.GetPosCountInfo(356);
    ASSERT_EQ((uint32_t)357, info.preDocAggPosCount);
    ASSERT_EQ((uint32_t)1, info.currentDocPosCount);

    ASSERT_EQ(file_system::FSEC_OK, _fileReader->Close());
}

} // namespace indexlib::index
