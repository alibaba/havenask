#include "indexlib/index/inverted_index/format/PositionListEncoder.h"

#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/RecyclePool.h"
#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/inverted_index/format/BufferedByteSliceReader.h"
#include "indexlib/index/inverted_index/format/skiplist/BufferedSkipListWriter.h"
#include "indexlib/util/PathUtil.h"
#include "unittest/unittest.h"

namespace indexlib::index {
namespace {
using testing::_;
using testing::Return;
} // namespace

class PositionListEncoderTest : public TESTBASE
{
public:
    PositionListEncoderTest()
    {
        _byteSlicePool = new autil::mem_pool::Pool(&_allocator, 10240);
        _bufferPool = new autil::mem_pool::RecyclePool(&_allocator, 10240);
    }

    ~PositionListEncoderTest()
    {
        delete _byteSlicePool;
        delete _bufferPool;
    }

    class MockSkipListWriter : public BufferedSkipListWriter
    {
    public:
        MockSkipListWriter(autil::mem_pool::Pool* byteSlicePool, autil::mem_pool::RecyclePool* bufferPool)
            : BufferedSkipListWriter(byteSlicePool, bufferPool)
        {
        }

    public:
        MOCK_METHOD(size_t, Flush, (uint8_t), (override));
        MOCK_METHOD(void, Dump, (const std::shared_ptr<file_system::FileWriter>& file), (override));
    };

    void setUp() override
    {
        _rootDir = util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "position_list_encoder_test");
        file_system::FslibWrapper::MkDirE(_rootDir);
        _positionFile = util::PathUtil::JoinPath(_rootDir, "positionFile");
    }
    void tearDown() override {}

private:
    void InnerTestFlushPositionBuffer(uint8_t compressMode, bool hasTfBitmap, bool skipListWriterIsNull)
    {
        optionflag_t optionFlag = of_position_list | of_position_payload;
        if (hasTfBitmap) {
            optionFlag |= (of_term_frequency | of_tf_bitmap);
        }
        PositionListFormatOption posListFormatOption(optionFlag);
        PositionListEncoder positionEncoder(posListFormatOption, _byteSlicePool, _bufferPool);

        positionEncoder.AddPosition(1, 1);
        positionEncoder.FlushPositionBuffer(compressMode);

        ASSERT_EQ((nullptr == positionEncoder._posSkipListWriter), skipListWriterIsNull);
        if (!skipListWriterIsNull) {
            BufferedSkipListWriter* skipListWriter = positionEncoder._posSkipListWriter;
            ASSERT_TRUE(skipListWriter != nullptr);
        }
    }

    autil::mem_pool::Pool* _byteSlicePool;
    autil::mem_pool::RecyclePool* _bufferPool;
    autil::mem_pool::SimpleAllocator _allocator;
    std::string _rootDir;
    std::string _positionFile;
};

TEST_F(PositionListEncoderTest, testSimpleProcess)
{
    PositionListFormatOption posListFormatOption;
    posListFormatOption._hasPositionList = true;
    posListFormatOption._hasPositionPayload = true;
    PositionListEncoder positionEncoder(posListFormatOption, _byteSlicePool, _bufferPool);

    for (uint32_t i = 0; i <= MAX_DOC_PER_RECORD + 1; i++) {
        positionEncoder.AddPosition(i, i);
    }
    positionEncoder.EndDocument();
    std::shared_ptr<file_system::FileWriter> positionFile(new file_system::BufferedFileWriter());
    ASSERT_EQ(file_system::FSEC_OK, positionFile->Open(_positionFile, _positionFile));
    positionEncoder.Dump(positionFile);
    ASSERT_EQ(file_system::FSEC_OK, positionFile->Close());
    ASSERT_TRUE(file_system::FslibWrapper::IsExist(_positionFile).GetOrThrow());
    ASSERT_TRUE(file_system::FslibWrapper::GetFileLength(_positionFile).GetOrThrow() > 0);
}

TEST_F(PositionListEncoderTest, testAddPosition)
{
    PositionListFormatOption posListFormatOption(of_position_list | of_position_payload);

    PositionListEncoder positionEncoder(posListFormatOption, _byteSlicePool, _bufferPool);
    for (uint32_t i = 1; i <= MAX_POS_PER_RECORD + 1; i++) {
        positionEncoder.AddPosition(i, i);
    }

    BufferedByteSliceReader reader;
    reader.Open(&positionEncoder._posListBuffer);

    size_t decodeCount = 0;
    pos_t posBuffer[MAX_POS_PER_RECORD];
    pospayload_t posPayloadBuffer[MAX_POS_PER_RECORD];
    ASSERT_TRUE(reader.Decode(posBuffer, MAX_POS_PER_RECORD, decodeCount));
    ASSERT_EQ((size_t)MAX_POS_PER_RECORD, decodeCount);
    for (uint32_t i = 0; i < MAX_POS_PER_RECORD; i++) {
        ASSERT_EQ((pos_t)1, posBuffer[i]);
    }
    ASSERT_TRUE(reader.Decode(posPayloadBuffer, MAX_POS_PER_RECORD, decodeCount));
    ASSERT_EQ((size_t)MAX_POS_PER_RECORD, decodeCount);
    for (uint32_t i = 0; i < MAX_POS_PER_RECORD; i++) {
        ASSERT_EQ((pos_t)(i + 1), posPayloadBuffer[i]);
    }
    ASSERT_TRUE(reader.Decode(posBuffer, MAX_POS_PER_RECORD, decodeCount));
    ASSERT_EQ((size_t)1, decodeCount);
    ASSERT_TRUE(reader.Decode(posPayloadBuffer, MAX_POS_PER_RECORD, decodeCount));
    ASSERT_EQ((size_t)1, decodeCount);

    ASSERT_TRUE(!reader.Decode(posBuffer, MAX_POS_PER_RECORD, decodeCount));

    ASSERT_TRUE(positionEncoder._posSkipListWriter != nullptr);
    ASSERT_EQ((size_t)1, positionEncoder._posListBuffer.GetBufferSize());
    ASSERT_EQ(MAX_POS_PER_RECORD + 1, positionEncoder._lastPosInCurDoc);
    ASSERT_EQ(MAX_POS_PER_RECORD + 1, positionEncoder._totalPosCount);
}

TEST_F(PositionListEncoderTest, testAddPositionWithSkipList)
{
    PositionListFormatOption posListFormatOption(of_position_list | of_position_payload);

    PositionListEncoder positionEncoder(posListFormatOption, _byteSlicePool, _bufferPool);
    for (uint32_t i = 0; i < MAX_POS_PER_RECORD; i++) {
        positionEncoder.AddPosition(i, i);
    }
    for (uint32_t i = MAX_POS_PER_RECORD; i < 2 * MAX_POS_PER_RECORD; i++) {
        positionEncoder.AddPosition(2 * i, i);
    }
    BufferedSkipListWriter* skipListWriter = positionEncoder._posSkipListWriter;

    BufferedByteSliceReader reader;
    reader.Open(skipListWriter);

    size_t decodeCount = 0;
    uint32_t totalPosBuffer[SKIP_LIST_BUFFER_SIZE];
    uint32_t offsetBuffer[SKIP_LIST_BUFFER_SIZE];
    ASSERT_TRUE(reader.Decode(totalPosBuffer, SKIP_LIST_BUFFER_SIZE, decodeCount));
    ASSERT_EQ((size_t)2, decodeCount);
    ASSERT_EQ((uint32_t)128, totalPosBuffer[0]);
    ASSERT_EQ((uint32_t)128, totalPosBuffer[1]);

    ASSERT_TRUE(reader.Decode(offsetBuffer, SKIP_LIST_BUFFER_SIZE, decodeCount));
    ASSERT_EQ((size_t)2, decodeCount);
    BufferedByteSlice& positionList = positionEncoder._posListBuffer;

    BufferedByteSliceReader posReader;
    posReader.Open(&positionList);

    posReader.Seek(offsetBuffer[0]);
    pos_t posBuffer[MAX_POS_PER_RECORD];
    pospayload_t posPayloadBuffer[MAX_POS_PER_RECORD];

    ASSERT_TRUE(posReader.Decode(posBuffer, MAX_POS_PER_RECORD, decodeCount));
    ASSERT_EQ((size_t)MAX_POS_PER_RECORD, decodeCount);
    ASSERT_EQ((pos_t)129, posBuffer[0]);
    ASSERT_EQ((pos_t)2, posBuffer[1]);

    ASSERT_TRUE(posReader.Decode(posPayloadBuffer, MAX_POS_PER_RECORD, decodeCount));
    ASSERT_EQ((size_t)MAX_POS_PER_RECORD, decodeCount);
    ASSERT_EQ((pospayload_t)128, posPayloadBuffer[0]);
    ASSERT_EQ((pospayload_t)129, posPayloadBuffer[1]);

    ASSERT_EQ(positionList.EstimateDumpSize(), offsetBuffer[0] + offsetBuffer[1]);
}

TEST_F(PositionListEncoderTest, testFlushPositionBuffer)
{
    InnerTestFlushPositionBuffer(
        /*compressmode=*/SHORT_LIST_COMPRESS_MODE,
        /*hasTfBitmap=*/true,
        /*skipListWriterIsNull=*/true);

    InnerTestFlushPositionBuffer(
        /*compressmode=*/PFOR_DELTA_COMPRESS_MODE,
        /*hasTfBitmap=*/true,
        /*skipListWriterIsNull=*/false);

    InnerTestFlushPositionBuffer(
        /*compressmode=*/PFOR_DELTA_COMPRESS_MODE,
        /*hasTfBitmap=*/false,
        /*skipListWriterIsNull=*/false);
}

TEST_F(PositionListEncoderTest, testDump)
{
    std::shared_ptr<file_system::FileWriter> positionFile(new file_system::BufferedFileWriter());
    ASSERT_EQ(file_system::FSEC_OK, positionFile->Open(_positionFile, _positionFile));

    optionflag_t optionFlag = of_position_list | of_position_payload | of_term_frequency | of_tf_bitmap;
    PositionListFormatOption posListFormatOption(optionFlag);
    PositionListEncoder positionEncoder(posListFormatOption, _byteSlicePool, _bufferPool);

    positionEncoder._totalPosCount = 1000;
    void* buffer = _byteSlicePool->allocate(sizeof(MockSkipListWriter));
    MockSkipListWriter* mockSkipListWriter = new (buffer) MockSkipListWriter(_byteSlicePool, _bufferPool);
    EXPECT_CALL(*mockSkipListWriter, Flush(_)).WillOnce(Return(0));
    EXPECT_CALL(*mockSkipListWriter, Dump(_)).WillOnce(Return());
    positionEncoder.SetDocSkipListWriter(mockSkipListWriter);

    positionEncoder.Dump(positionFile);
    ASSERT_EQ(file_system::FSEC_OK, positionFile->Close());
}

} // namespace indexlib::index
