#include "indexlib/index/normal/inverted_index/test/in_mem_position_list_decoder_unittest.h"
#include "indexlib/index/normal/inverted_index/format/position_list_encoder.h"
#include "indexlib/util/byte_slice_list/byte_slice_list.h"

using namespace std;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemPositionListDecoderTest);

InMemPositionListDecoderTest::InMemPositionListDecoderTest()
    : mBufferPool(10240)
    , mByteSlicePool(10240)
{
}

InMemPositionListDecoderTest::~InMemPositionListDecoderTest()
{
}

void InMemPositionListDecoderTest::CaseSetUp()
{
}

void InMemPositionListDecoderTest::CaseTearDown()
{
}

void InMemPositionListDecoderTest::TestSimpleProcess()
{
    PositionListFormatOption option(NO_PAYLOAD);

    PositionListEncoder posListEncoder(option, &mByteSlicePool, &mBufferPool);
    posListEncoder.AddPosition(3, 0);
    posListEncoder.EndDocument();

    InMemPositionListDecoder* posListDecoder = 
        posListEncoder.GetInMemPositionListDecoder(&mByteSlicePool);
    ASSERT_TRUE(posListDecoder);

    NormalInDocState state(SHORT_LIST_COMPRESS_MODE, NO_PAYLOAD);
    ASSERT_TRUE(posListDecoder->SkipTo(0, &state));

    uint32_t tf = 0;
    ASSERT_TRUE(posListDecoder->LocateRecord(&state, tf));
    ASSERT_EQ((int32_t)0, state.GetRecordOffset());
    ASSERT_EQ((int32_t)0, state.GetOffsetInRecord());

    pos_t posBuffer[MAX_POS_PER_RECORD];
    pospayload_t posPayloadBuffer[MAX_POS_PER_RECORD];
    ASSERT_EQ((uint32_t)1, posListDecoder->DecodeRecord(posBuffer,
                    MAX_POS_PER_RECORD, posPayloadBuffer, MAX_POS_PER_RECORD));
    ASSERT_EQ((pos_t)3, posBuffer[0]);
}

void InMemPositionListDecoderTest::TestShortListDecodeWithoutPayload()
{
    const optionflag_t flag = of_position_list;
    tf_t tf = 5;
    pos_t posList[] = {1, 5, 6, 19, 20};

    TestDecodeWithOptionFlag(flag, tf, posList, NULL, false);
    TestDecodeWithOptionFlag(flag, tf, posList, NULL, true);
}

void InMemPositionListDecoderTest::TestShortListDecodeWithPayload()
{
    const optionflag_t flag = of_position_list | of_position_payload;
    tf_t tf = 5;
    pos_t posList[] = {1, 5, 6, 19, 20};
    pospayload_t payloadList[] = {4, 1, 18, 0, 2};

    TestDecodeWithOptionFlag(flag, tf, posList, payloadList, false);
    TestDecodeWithOptionFlag(flag, tf, posList, payloadList, true);
}


void InMemPositionListDecoderTest::TestDecodeWithoutPayload()
{
    const optionflag_t flag = of_position_list;    
    InnerTestDecode(flag, 1);
    InnerTestDecode(flag, MAX_UNCOMPRESSED_POS_LIST_SIZE + 1);
    InnerTestDecode(flag, MAX_POS_PER_RECORD);
    InnerTestDecode(flag, MAX_POS_PER_RECORD + 10);
    InnerTestDecode(flag, MAX_POS_PER_RECORD * 5 + 1);
    InnerTestDecode(flag, MAX_POS_PER_RECORD * MAX_UNCOMPRESSED_SKIP_LIST_SIZE + 1);
    InnerTestDecode(flag, MAX_POS_PER_RECORD * SKIP_LIST_BUFFER_SIZE + 1);
    InnerTestDecode(flag, MAX_POS_PER_RECORD * SKIP_LIST_BUFFER_SIZE * 2 + 1);
}

void InMemPositionListDecoderTest::TestDecodeWithPayload()
{
    const optionflag_t flag = of_position_list | of_position_payload;
    InnerTestDecode(flag, 1);
    InnerTestDecode(flag, MAX_UNCOMPRESSED_POS_LIST_SIZE + 1);
    InnerTestDecode(flag, MAX_POS_PER_RECORD);
    InnerTestDecode(flag, MAX_POS_PER_RECORD + 10);
    InnerTestDecode(flag, MAX_POS_PER_RECORD * 5 + 1);
    InnerTestDecode(flag, MAX_POS_PER_RECORD * MAX_UNCOMPRESSED_SKIP_LIST_SIZE + 1);
    InnerTestDecode(flag, MAX_POS_PER_RECORD * SKIP_LIST_BUFFER_SIZE + 1);
    InnerTestDecode(flag, MAX_POS_PER_RECORD * SKIP_LIST_BUFFER_SIZE * 2 + 1);

}

void InMemPositionListDecoderTest::TestDecodeWithOptionFlag(
        const optionflag_t flag, tf_t tf,
        pos_t* posList, pospayload_t* payloadList,
        bool needFlush)
{
    bool hasPayload = flag & of_position_payload;
    PositionListFormatOption option(flag);

    PositionListEncoder posListEncoder(option, &mByteSlicePool, &mBufferPool);
    for (size_t i = 0; i < (size_t)tf; ++i)
    {
        pospayload_t payload = hasPayload ? payloadList[i] : 0;
        posListEncoder.AddPosition(posList[i], payload);
    }
    posListEncoder.EndDocument();
    if (needFlush)
    {
        posListEncoder.Flush();
    }

    InMemPositionListDecoder* posListDecoder = 
        posListEncoder.GetInMemPositionListDecoder(&mByteSlicePool);
    ASSERT_TRUE(posListDecoder);

    // compress mode is useless in decoder
    NormalInDocState state(SHORT_LIST_COMPRESS_MODE, flag);
    ASSERT_TRUE(posListDecoder->SkipTo(0, &state));

    uint32_t tempTF = 0;
    ASSERT_TRUE(posListDecoder->LocateRecord(&state, tempTF));
    ASSERT_EQ((int32_t)0, state.GetRecordOffset());
    ASSERT_EQ((int32_t)0, state.GetOffsetInRecord());
    
    pos_t posBuffer[MAX_POS_PER_RECORD];
    pospayload_t posPayloadBuffer[MAX_POS_PER_RECORD];

    size_t posIdx = 0;
    pos_t lastPos = 0;
    for (size_t i = 0; i < tf / MAX_POS_PER_RECORD; ++i)
    {
        uint32_t decodeCount = posListDecoder->DecodeRecord(posBuffer,
                MAX_POS_PER_RECORD, posPayloadBuffer, MAX_POS_PER_RECORD);
        ASSERT_EQ((uint32_t)MAX_POS_PER_RECORD, decodeCount);
        for (size_t j = 0; j < MAX_POS_PER_RECORD; ++j)
        {
            ASSERT_EQ(posList[posIdx], lastPos + posBuffer[j]);
            if (hasPayload)
            {
                ASSERT_EQ(payloadList[posIdx], posPayloadBuffer[j]);
            }
            lastPos = lastPos + posBuffer[j];
            posIdx++;
        }
    }
    if (tf % MAX_POS_PER_RECORD != 0)
    {
        uint32_t decodeCount = posListDecoder->DecodeRecord(posBuffer,
                MAX_POS_PER_RECORD, posPayloadBuffer, MAX_POS_PER_RECORD);
        ASSERT_EQ((uint32_t)(tf % MAX_POS_PER_RECORD), decodeCount);
        for (size_t j = 0; j < decodeCount; ++j)
        {
            ASSERT_EQ(posList[posIdx], lastPos + posBuffer[j]);
            if (hasPayload)
            {
                ASSERT_EQ(payloadList[posIdx], posPayloadBuffer[j]);
            }
            lastPos = lastPos + posBuffer[j];
            posIdx++;
        }
    }
}

void InMemPositionListDecoderTest::InnerTestDecode(
        const optionflag_t flag, tf_t tf)
{
    pos_t *posList = new pos_t[tf];
    pospayload_t *payloadList = new pospayload_t[tf];

    for (tf_t i = 0; i < tf; ++i)
    {
        posList[i] = i * 2 + 1;
        payloadList[i] = (pospayload_t)(i * 2);
    }

    TestDecodeWithOptionFlag(flag, tf, posList, payloadList, false);
    TestDecodeWithOptionFlag(flag, tf, posList, payloadList, true);

    delete[] posList;
    delete[] payloadList;
}

void InMemPositionListDecoderTest::TestSkipTo()
{
    PositionListFormatOption option(of_position_list);
    NormalInDocState state(SHORT_LIST_COMPRESS_MODE, of_position_list);

    {
        BufferedByteSlice *posListBuffer =     
            IE_POOL_COMPATIBLE_NEW_CLASS((&mByteSlicePool), 
                    BufferedByteSlice, &mByteSlicePool, &mBufferPool);

        InMemPositionListDecoder decoder(option, &mByteSlicePool);
        decoder.Init(100, NULL, posListBuffer);

        // current ttf over totalTF
        ASSERT_FALSE(decoder.SkipTo(101, &state));
        ASSERT_TRUE(decoder.SkipTo(50, &state));

        ASSERT_EQ((int32_t)0, state.GetRecordOffset());
        ASSERT_EQ((int32_t)50, state.GetOffsetInRecord()); 
    }

    {
        BufferedByteSlice *posListBuffer =     
            IE_POOL_COMPATIBLE_NEW_CLASS((&mByteSlicePool),
                    BufferedByteSlice, &mByteSlicePool, &mBufferPool);


        InMemPositionListDecoder decoder(option, &mByteSlicePool);
        // skipListReader call skipTo : return true
        MockSkipListReader* skipListReader = IE_POOL_COMPATIBLE_NEW_CLASS((&mByteSlicePool),
                MockSkipListReader);
        skipListReader->SetPrevKey(10);

        EXPECT_CALL(*skipListReader, SkipTo(_,_,_,_,_))
            .WillOnce(DoAll(SetArgReferee<1>(30), SetArgReferee<3>(123), Return(true)));

        decoder.Init(100, skipListReader, posListBuffer);
        ASSERT_TRUE(decoder.SkipTo(20, &state));

        ASSERT_EQ((int32_t)123, state.GetRecordOffset());
        ASSERT_EQ((int32_t)10, state.GetOffsetInRecord()); 

        // currentTTF in last record
        ASSERT_TRUE(decoder.SkipTo(25, &state));
        ASSERT_EQ((int32_t)123, state.GetRecordOffset());
        ASSERT_EQ((int32_t)15, state.GetOffsetInRecord()); 

        // skipListReader call skipTo : return false
        skipListReader->SetCurrentKey(50);

        EXPECT_CALL(*skipListReader, SkipTo(_,_,_,_,_))
            .WillOnce(Return(false));

        EXPECT_CALL(*skipListReader, GetLastValueInBuffer())
            .WillOnce(Return(150));

        ASSERT_TRUE(decoder.SkipTo(70, &state));
        ASSERT_EQ((int32_t)150, state.GetRecordOffset());
        ASSERT_EQ((int32_t)20, state.GetOffsetInRecord()); 

        // currentTTF in last record
        ASSERT_TRUE(decoder.SkipTo(90, &state));
        ASSERT_EQ((int32_t)150, state.GetRecordOffset());
        ASSERT_EQ((int32_t)40, state.GetOffsetInRecord()); 
    }
}

void InMemPositionListDecoderTest::TestSkipAndLocateAndDecodeWithoutPayload()
{
    const optionflag_t flag = of_position_list;    

    InnerTestSkipAndLocateAndDecode(flag, 1, true);
    InnerTestSkipAndLocateAndDecode(flag, MAX_UNCOMPRESSED_POS_LIST_SIZE + 1, true);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD, true);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD + 10, true);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD * 5 + 1, true);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD * MAX_UNCOMPRESSED_SKIP_LIST_SIZE + 1, true);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD * SKIP_LIST_BUFFER_SIZE + 1, true);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD * SKIP_LIST_BUFFER_SIZE * 2 + 1, true);

    InnerTestSkipAndLocateAndDecode(flag, 1, false);
    InnerTestSkipAndLocateAndDecode(flag, MAX_UNCOMPRESSED_POS_LIST_SIZE + 1, false);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD, false);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD + 10, false);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD * 5 + 1, false);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD * MAX_UNCOMPRESSED_SKIP_LIST_SIZE + 1, false);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD * SKIP_LIST_BUFFER_SIZE + 1, false);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD * SKIP_LIST_BUFFER_SIZE * 2 + 1, false);
}

void InMemPositionListDecoderTest::TestSkipAndLocateAndDecodeWithPayload()
{
    const optionflag_t flag = of_position_list | of_position_payload;

    InnerTestSkipAndLocateAndDecode(flag, 1, true);
    InnerTestSkipAndLocateAndDecode(flag, MAX_UNCOMPRESSED_POS_LIST_SIZE + 1, true);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD, true);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD + 10, true);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD * 5 + 1, true);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD * MAX_UNCOMPRESSED_SKIP_LIST_SIZE + 1, true);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD * SKIP_LIST_BUFFER_SIZE + 1, true);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD * SKIP_LIST_BUFFER_SIZE * 2 + 1, true);

    InnerTestSkipAndLocateAndDecode(flag, 1, false);
    InnerTestSkipAndLocateAndDecode(flag, MAX_UNCOMPRESSED_POS_LIST_SIZE + 1, false);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD, false);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD + 10, false);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD * 5 + 1, false);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD * MAX_UNCOMPRESSED_SKIP_LIST_SIZE + 1, false);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD * SKIP_LIST_BUFFER_SIZE + 1, false);
    InnerTestSkipAndLocateAndDecode(flag, MAX_POS_PER_RECORD * SKIP_LIST_BUFFER_SIZE * 2 + 1, false);
}

void InMemPositionListDecoderTest::InnerTestSkipAndLocateAndDecode(
        optionflag_t flag, ttf_t ttf, bool needFlush)
{
    bool hasPayload = flag & of_position_payload;
    PositionListFormatOption option(flag);

    PositionListEncoder posListEncoder(option, &mByteSlicePool, &mBufferPool);
    pos_t sum = 0;
    for (size_t i = 0; i < (size_t)ttf; ++i)
    {
        sum += i;
        posListEncoder.AddPosition(sum, i % 131);
    }
    posListEncoder.EndDocument();
    if (needFlush)
    {
        posListEncoder.Flush();
    }

    InMemPositionListDecoder* posListDecoder = 
        posListEncoder.GetInMemPositionListDecoder(&mByteSlicePool);
    ASSERT_TRUE(posListDecoder);

    // compress mode is useless in decoder
    NormalInDocState state(SHORT_LIST_COMPRESS_MODE, flag);

    int32_t recordOffset = 0;
    uint32_t tempTF = 0;
    pos_t posBuffer[MAX_POS_PER_RECORD];
    pospayload_t payloadBuffer[MAX_POS_PER_RECORD];
    for (size_t i = 0; i < (size_t)ttf; ++i)
    {
        ASSERT_TRUE(posListDecoder->SkipTo(i, &state));
        if (i % MAX_POS_PER_RECORD == 0)
        {
            ASSERT_TRUE(posListDecoder->LocateRecord(&state, tempTF));
            recordOffset = state.GetRecordOffset();
            // check decode buffer
            uint32_t decodeCount = posListDecoder->DecodeRecord(posBuffer, 
                    MAX_POS_PER_RECORD, payloadBuffer, MAX_POS_PER_RECORD);
            uint32_t expectDecodeCount = std::min(uint32_t(ttf - i), MAX_POS_PER_RECORD);
            ASSERT_EQ(expectDecodeCount, decodeCount);
            for (size_t j = 0; j < (size_t)decodeCount; ++j)
            {
                ASSERT_EQ(i + j, posBuffer[j]);
                if (hasPayload)
                {
                    ASSERT_EQ((i + j) % 131, payloadBuffer[j]);
                }
            }
        }
        else
        {
            ASSERT_FALSE(posListDecoder->LocateRecord(&state, tempTF));
            ASSERT_EQ(recordOffset, state.GetRecordOffset());
        }
        ASSERT_EQ((int32_t)(i % MAX_POS_PER_RECORD), state.GetOffsetInRecord());
    }
}

void InMemPositionListDecoderTest::TestDestruction()
{
    PositionListFormatOption option(NO_PAYLOAD);
    InMemPositionListDecoder* decoder = IE_POOL_COMPATIBLE_NEW_CLASS(
        (&mByteSlicePool), InMemPositionListDecoder, option, nullptr);

    auto *skipListReader = new PairValueSkipListReader();
    auto *posListBuffer = new BufferedByteSlice(&mByteSlicePool, &mBufferPool);
    decoder->Init(100, skipListReader, posListBuffer);

    IE_POOL_COMPATIBLE_DELETE_CLASS(&mByteSlicePool, decoder);
}

IE_NAMESPACE_END(index);

