#include "indexlib/index/normal/inverted_index/test/position_list_encoder_unittest.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/index/normal/inverted_index/format/buffered_byte_slice_reader.h"

using testing::_;
using testing::Return;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PositionListEncoderTest);

using namespace std;
using namespace autil::mem_pool;

PositionListEncoderTest::PositionListEncoderTest()
{
    mByteSlicePool = new Pool(&mAllocator, 10240);
    mBufferPool = new RecyclePool(&mAllocator, 10240);
}

PositionListEncoderTest::~PositionListEncoderTest()
{
    delete mByteSlicePool;
    delete mBufferPool;
}

void PositionListEncoderTest::SetUp()
{
    mRootDir = FileSystemWrapper::JoinPath(TEST_DATA_PATH, "position_list_encoder_test");
    if (FileSystemWrapper::IsExist(mRootDir))
    {
        FileSystemWrapper::DeleteDir(mRootDir);
    }
    FileSystemWrapper::MkDir(mRootDir);
    mPositionFile = FileSystemWrapper::JoinPath(mRootDir, "positionFile");
}

void PositionListEncoderTest::TearDown()
{
    FileSystemWrapper::DeleteDir(mRootDir);
}

void PositionListEncoderTest::TestSimpleProcess()
{
    PositionListFormatOption posListFormatOption;
    posListFormatOption.mHasPositionList = true;
    posListFormatOption.mHasPositionPayload = true;
    PositionListEncoder positionEncoder(
            posListFormatOption, mByteSlicePool, mBufferPool);

    for (uint32_t i = 0; i <= MAX_DOC_PER_RECORD + 1; i++)
    {
        positionEncoder.AddPosition(i, i);
    }
    positionEncoder.EndDocument();
    file_system::FileWriterPtr positionFile(
            new file_system::BufferedFileWriter());
    positionFile->Open(mPositionFile);
    positionEncoder.Dump(positionFile);
    positionFile->Close();
    INDEXLIB_TEST_TRUE(FileSystemWrapper::IsExist(mPositionFile));
    INDEXLIB_TEST_TRUE(FileSystemWrapper::GetFileLength(mPositionFile) > 0);
}

void PositionListEncoderTest::TestAddPosition()
{
    PositionListFormatOption posListFormatOption(
            of_position_list|of_position_payload);

    PositionListEncoder positionEncoder(
            posListFormatOption, mByteSlicePool, mBufferPool);
    for (uint32_t i = 1; i <= MAX_POS_PER_RECORD + 1; i++)
    {
        positionEncoder.AddPosition(i, i);
    }

    BufferedByteSliceReader reader;
    reader.Open(&positionEncoder.mPosListBuffer);

    size_t decodeCount = 0;
    pos_t posBuffer[MAX_POS_PER_RECORD];
    pospayload_t posPayloadBuffer[MAX_POS_PER_RECORD];
    INDEXLIB_TEST_TRUE(reader.Decode(
                    posBuffer, MAX_POS_PER_RECORD, decodeCount));
    INDEXLIB_TEST_EQUAL((size_t)MAX_POS_PER_RECORD, decodeCount);
    for (uint32_t i = 0; i < MAX_POS_PER_RECORD; i++)
    {
        INDEXLIB_TEST_EQUAL((pos_t)1, posBuffer[i]);
    }
    INDEXLIB_TEST_TRUE(reader.Decode(
                    posPayloadBuffer, MAX_POS_PER_RECORD, decodeCount));
    INDEXLIB_TEST_EQUAL((size_t)MAX_POS_PER_RECORD, decodeCount);
    for (uint32_t i = 0; i < MAX_POS_PER_RECORD; i++)
    {
        INDEXLIB_TEST_EQUAL((pos_t)(i + 1), posPayloadBuffer[i]);
    }
    INDEXLIB_TEST_TRUE(reader.Decode(
                    posBuffer, MAX_POS_PER_RECORD, decodeCount));
    INDEXLIB_TEST_EQUAL((size_t)1, decodeCount);
    INDEXLIB_TEST_TRUE(reader.Decode(
                    posPayloadBuffer, MAX_POS_PER_RECORD, decodeCount));
    INDEXLIB_TEST_EQUAL((size_t)1, decodeCount);

    INDEXLIB_TEST_TRUE(!reader.Decode(
                    posBuffer, MAX_POS_PER_RECORD, decodeCount));

    INDEXLIB_TEST_TRUE(positionEncoder.mPosSkipListWriter != NULL);
    INDEXLIB_TEST_EQUAL((size_t)1, positionEncoder.mPosListBuffer.GetBufferSize());
    INDEXLIB_TEST_EQUAL(MAX_POS_PER_RECORD + 1, positionEncoder.mLastPosInCurDoc);
    INDEXLIB_TEST_EQUAL(MAX_POS_PER_RECORD + 1, positionEncoder.mTotalPosCount);
}

void PositionListEncoderTest::TestAddPositionWithSkipList()
{
    PositionListFormatOption posListFormatOption(
            of_position_list|of_position_payload);

    PositionListEncoder positionEncoder(
            posListFormatOption, mByteSlicePool, mBufferPool);
    for (uint32_t i = 0; i < MAX_POS_PER_RECORD; i++)
    {
        positionEncoder.AddPosition(i, i);
    }
    for (uint32_t i = MAX_POS_PER_RECORD; i < 2 * MAX_POS_PER_RECORD; i++)
    {
        positionEncoder.AddPosition(2 * i, i);
    }
    BufferedSkipListWriter* skipListWriter = positionEncoder.mPosSkipListWriter;

    BufferedByteSliceReader reader;
    reader.Open(skipListWriter);

    size_t decodeCount = 0;
    uint32_t totalPosBuffer[SKIP_LIST_BUFFER_SIZE];
    uint32_t offsetBuffer[SKIP_LIST_BUFFER_SIZE];
    INDEXLIB_TEST_TRUE(reader.Decode(
                    totalPosBuffer, SKIP_LIST_BUFFER_SIZE, decodeCount));
    INDEXLIB_TEST_EQUAL((size_t)2, decodeCount);
    INDEXLIB_TEST_EQUAL((uint32_t)128, totalPosBuffer[0]);
    INDEXLIB_TEST_EQUAL((uint32_t)128, totalPosBuffer[1]);

    INDEXLIB_TEST_TRUE(reader.Decode(
                    offsetBuffer, SKIP_LIST_BUFFER_SIZE, decodeCount));
    INDEXLIB_TEST_EQUAL((size_t)2, decodeCount);
    BufferedByteSlice& positionList = positionEncoder.mPosListBuffer;

    BufferedByteSliceReader posReader;
    posReader.Open(&positionList);

    posReader.Seek(offsetBuffer[0]);
    pos_t posBuffer[MAX_POS_PER_RECORD];
    pospayload_t posPayloadBuffer[MAX_POS_PER_RECORD];

    INDEXLIB_TEST_TRUE(posReader.Decode(
                    posBuffer, MAX_POS_PER_RECORD, decodeCount));
    INDEXLIB_TEST_EQUAL((size_t)MAX_POS_PER_RECORD, decodeCount);
    INDEXLIB_TEST_EQUAL((pos_t)129, posBuffer[0]);
    INDEXLIB_TEST_EQUAL((pos_t)2, posBuffer[1]);

    INDEXLIB_TEST_TRUE(posReader.Decode(
                    posPayloadBuffer, MAX_POS_PER_RECORD, decodeCount));
    INDEXLIB_TEST_EQUAL((size_t)MAX_POS_PER_RECORD, decodeCount);
    INDEXLIB_TEST_EQUAL((pospayload_t)128, posPayloadBuffer[0]);
    INDEXLIB_TEST_EQUAL((pospayload_t)129, posPayloadBuffer[1]);

    INDEXLIB_TEST_EQUAL(positionList.EstimateDumpSize(),
                        offsetBuffer[0] + offsetBuffer[1]);
}

void PositionListEncoderTest::InnerTestFlushPositionBuffer(
        uint8_t compressMode, bool hasTfBitmap,
        bool skipListWriterIsNull)
{
    optionflag_t optionFlag = of_position_list | of_position_payload;
    if (hasTfBitmap)
    {
        optionFlag |= (of_term_frequency|of_tf_bitmap);
    }
    PositionListFormatOption posListFormatOption(optionFlag);
    PositionListEncoder positionEncoder(posListFormatOption, 
            mByteSlicePool, mBufferPool);

    positionEncoder.AddPosition(1, 1);
    positionEncoder.FlushPositionBuffer(compressMode);

    INDEXLIB_TEST_EQUAL((NULL == positionEncoder.mPosSkipListWriter), skipListWriterIsNull);
    if (!skipListWriterIsNull)
    {
        BufferedSkipListWriter* skipListWriter = positionEncoder.mPosSkipListWriter;
        INDEXLIB_TEST_TRUE(skipListWriter != NULL);
    }
}

void PositionListEncoderTest::TestFlushPositionBuffer()
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

void PositionListEncoderTest::TestDump()
{
    file_system::FileWriterPtr positionFile(
            new file_system::BufferedFileWriter());
    positionFile->Open(mPositionFile);

    optionflag_t optionFlag = of_position_list | of_position_payload
                              | of_term_frequency | of_tf_bitmap;
    PositionListFormatOption posListFormatOption(optionFlag);
    PositionListEncoder positionEncoder(posListFormatOption, 
            mByteSlicePool, mBufferPool);

    positionEncoder.mTotalPosCount = 1000;
    void *buffer = mByteSlicePool->allocate(sizeof(MockSkipListWriter));
    MockSkipListWriter* mockSkipListWriter = 
        new(buffer) MockSkipListWriter(mByteSlicePool, mBufferPool);
    EXPECT_CALL(*mockSkipListWriter, Flush(_)).WillOnce(Return (0));
    EXPECT_CALL(*mockSkipListWriter, Dump(_)).WillOnce(Return ());
    positionEncoder.SetDocSkipListWriter(mockSkipListWriter);
    
    positionEncoder.Dump(positionFile);
    positionFile->Close();
}

IE_NAMESPACE_END(index);

