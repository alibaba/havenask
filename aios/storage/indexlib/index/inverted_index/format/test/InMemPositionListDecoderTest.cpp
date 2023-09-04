#include "indexlib/index/inverted_index/format/InMemPositionListDecoder.h"

#include "indexlib/index/inverted_index/format/PositionListEncoder.h"
#include "indexlib/util/PoolUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "unittest/unittest.h"

namespace indexlib::index {
namespace {
} // namespace

class InMemPositionListDecoderTest : public TESTBASE
{
public:
    InMemPositionListDecoderTest() : _bufferPool(10240), _byteSlicePool(10240) {}

    class MockSkipListReader : public PairValueSkipListReader
    {
    public:
        MOCK_METHOD((std::pair<Status, bool>), SkipTo,
                    (uint32_t queryKey, uint32_t& key, uint32_t& prevKey, uint32_t& value, uint32_t& delta),
                    (override));
        MOCK_METHOD(uint32_t, GetLastValueInBuffer, (), (const, override));
    };

    void setUp() override {}
    void tearDown() override {}

private:
    void TestDecodeWithOptionFlag(const optionflag_t flag, tf_t tf, pos_t* posList, pospayload_t* payloadList,
                                  bool needFlush)
    {
        bool hasPayload = flag & of_position_payload;
        PositionListFormatOption option(flag);

        PositionListEncoder posListEncoder(option, &_byteSlicePool, &_bufferPool);
        for (size_t i = 0; i < (size_t)tf; ++i) {
            pospayload_t payload = hasPayload ? payloadList[i] : 0;
            posListEncoder.AddPosition(posList[i], payload);
        }
        posListEncoder.EndDocument();
        if (needFlush) {
            posListEncoder.Flush();
        }

        InMemPositionListDecoder* posListDecoder = posListEncoder.GetInMemPositionListDecoder(&_byteSlicePool);
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
        for (size_t i = 0; i < tf / MAX_POS_PER_RECORD; ++i) {
            uint32_t decodeCount =
                posListDecoder->DecodeRecord(posBuffer, MAX_POS_PER_RECORD, posPayloadBuffer, MAX_POS_PER_RECORD);
            ASSERT_EQ((uint32_t)MAX_POS_PER_RECORD, decodeCount);
            for (size_t j = 0; j < MAX_POS_PER_RECORD; ++j) {
                ASSERT_EQ(posList[posIdx], lastPos + posBuffer[j]);
                if (hasPayload) {
                    ASSERT_EQ(payloadList[posIdx], posPayloadBuffer[j]);
                }
                lastPos = lastPos + posBuffer[j];
                posIdx++;
            }
        }
        if (tf % MAX_POS_PER_RECORD != 0) {
            uint32_t decodeCount =
                posListDecoder->DecodeRecord(posBuffer, MAX_POS_PER_RECORD, posPayloadBuffer, MAX_POS_PER_RECORD);
            ASSERT_EQ((uint32_t)(tf % MAX_POS_PER_RECORD), decodeCount);
            for (size_t j = 0; j < decodeCount; ++j) {
                ASSERT_EQ(posList[posIdx], lastPos + posBuffer[j]);
                if (hasPayload) {
                    ASSERT_EQ(payloadList[posIdx], posPayloadBuffer[j]);
                }
                lastPos = lastPos + posBuffer[j];
                posIdx++;
            }
        }
    }

    void InnerTestDecode(const optionflag_t flag, tf_t tf)
    {
        pos_t* posList = new pos_t[tf];
        pospayload_t* payloadList = new pospayload_t[tf];

        for (tf_t i = 0; i < tf; ++i) {
            posList[i] = i * 2 + 1;
            payloadList[i] = (pospayload_t)(i * 2);
        }

        TestDecodeWithOptionFlag(flag, tf, posList, payloadList, false);
        TestDecodeWithOptionFlag(flag, tf, posList, payloadList, true);

        delete[] posList;
        delete[] payloadList;
    }

    void InnerTestSkipAndLocateAndDecode(optionflag_t flag, ttf_t ttf, bool needFlush)
    {
        bool hasPayload = flag & of_position_payload;
        PositionListFormatOption option(flag);

        PositionListEncoder posListEncoder(option, &_byteSlicePool, &_bufferPool);
        pos_t sum = 0;
        for (size_t i = 0; i < (size_t)ttf; ++i) {
            sum += i;
            posListEncoder.AddPosition(sum, i % 131);
        }
        posListEncoder.EndDocument();
        if (needFlush) {
            posListEncoder.Flush();
        }

        InMemPositionListDecoder* posListDecoder = posListEncoder.GetInMemPositionListDecoder(&_byteSlicePool);
        ASSERT_TRUE(posListDecoder);

        // compress mode is useless in decoder
        NormalInDocState state(SHORT_LIST_COMPRESS_MODE, flag);

        int32_t recordOffset = 0;
        uint32_t tempTF = 0;
        pos_t posBuffer[MAX_POS_PER_RECORD];
        pospayload_t payloadBuffer[MAX_POS_PER_RECORD];
        for (size_t i = 0; i < (size_t)ttf; ++i) {
            ASSERT_TRUE(posListDecoder->SkipTo(i, &state));
            if (i % MAX_POS_PER_RECORD == 0) {
                ASSERT_TRUE(posListDecoder->LocateRecord(&state, tempTF));
                recordOffset = state.GetRecordOffset();
                // check decode buffer
                uint32_t decodeCount =
                    posListDecoder->DecodeRecord(posBuffer, MAX_POS_PER_RECORD, payloadBuffer, MAX_POS_PER_RECORD);
                uint32_t expectDecodeCount = std::min(uint32_t(ttf - i), MAX_POS_PER_RECORD);
                ASSERT_EQ(expectDecodeCount, decodeCount);
                for (size_t j = 0; j < (size_t)decodeCount; ++j) {
                    ASSERT_EQ(i + j, posBuffer[j]);
                    if (hasPayload) {
                        ASSERT_EQ((i + j) % 131, payloadBuffer[j]);
                    }
                }
            } else {
                ASSERT_FALSE(posListDecoder->LocateRecord(&state, tempTF));
                ASSERT_EQ(recordOffset, state.GetRecordOffset());
            }
            ASSERT_EQ((int32_t)(i % MAX_POS_PER_RECORD), state.GetOffsetInRecord());
        }
    }

    autil::mem_pool::RecyclePool _bufferPool;
    autil::mem_pool::Pool _byteSlicePool;
};

TEST_F(InMemPositionListDecoderTest, testSimpleProcess)
{
    PositionListFormatOption option(NO_PAYLOAD);

    PositionListEncoder posListEncoder(option, &_byteSlicePool, &_bufferPool);
    posListEncoder.AddPosition(3, 0);
    posListEncoder.EndDocument();

    InMemPositionListDecoder* posListDecoder = posListEncoder.GetInMemPositionListDecoder(&_byteSlicePool);
    ASSERT_TRUE(posListDecoder);

    NormalInDocState state(SHORT_LIST_COMPRESS_MODE, NO_PAYLOAD);
    ASSERT_TRUE(posListDecoder->SkipTo(0, &state));

    uint32_t tf = 0;
    ASSERT_TRUE(posListDecoder->LocateRecord(&state, tf));
    ASSERT_EQ((int32_t)0, state.GetRecordOffset());
    ASSERT_EQ((int32_t)0, state.GetOffsetInRecord());

    pos_t posBuffer[MAX_POS_PER_RECORD];
    pospayload_t posPayloadBuffer[MAX_POS_PER_RECORD];
    ASSERT_EQ((uint32_t)1,
              posListDecoder->DecodeRecord(posBuffer, MAX_POS_PER_RECORD, posPayloadBuffer, MAX_POS_PER_RECORD));
    ASSERT_EQ((pos_t)3, posBuffer[0]);
}

TEST_F(InMemPositionListDecoderTest, testShortListDecodeWithoutPayload)
{
    const optionflag_t flag = of_position_list;
    tf_t tf = 5;
    pos_t posList[] = {1, 5, 6, 19, 20};

    TestDecodeWithOptionFlag(flag, tf, posList, NULL, false);
    TestDecodeWithOptionFlag(flag, tf, posList, NULL, true);
}

TEST_F(InMemPositionListDecoderTest, testShortListDecodeWithPayload)
{
    const optionflag_t flag = of_position_list | of_position_payload;
    tf_t tf = 5;
    pos_t posList[] = {1, 5, 6, 19, 20};
    pospayload_t payloadList[] = {4, 1, 18, 0, 2};

    TestDecodeWithOptionFlag(flag, tf, posList, payloadList, false);
    TestDecodeWithOptionFlag(flag, tf, posList, payloadList, true);
}

TEST_F(InMemPositionListDecoderTest, testDecodeWithoutPayload)
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

TEST_F(InMemPositionListDecoderTest, testDecodeWithPayload)
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

TEST_F(InMemPositionListDecoderTest, testSkipTo)
{
    PositionListFormatOption option(of_position_list);
    NormalInDocState state(SHORT_LIST_COMPRESS_MODE, of_position_list);

    {
        BufferedByteSlice* posListBuffer =
            IE_POOL_COMPATIBLE_NEW_CLASS((&_byteSlicePool), BufferedByteSlice, &_byteSlicePool, &_bufferPool);

        InMemPositionListDecoder decoder(option, &_byteSlicePool);
        decoder.Init(100, NULL, posListBuffer);

        // current ttf over totalTF
        ASSERT_FALSE(decoder.SkipTo(101, &state));
        ASSERT_TRUE(decoder.SkipTo(50, &state));

        ASSERT_EQ((int32_t)0, state.GetRecordOffset());
        ASSERT_EQ((int32_t)50, state.GetOffsetInRecord());
    }

    {
        BufferedByteSlice* posListBuffer =
            IE_POOL_COMPATIBLE_NEW_CLASS((&_byteSlicePool), BufferedByteSlice, &_byteSlicePool, &_bufferPool);

        InMemPositionListDecoder decoder(option, &_byteSlicePool);
        // skipListReader call skipTo : return true
        MockSkipListReader* skipListReader = IE_POOL_COMPATIBLE_NEW_CLASS((&_byteSlicePool), MockSkipListReader);
        skipListReader->SetPrevKey(10);

        EXPECT_CALL(*skipListReader, SkipTo(_, _, _, _, _))
            .WillOnce(DoAll(SetArgReferee<1>(30), SetArgReferee<3>(123), Return(std::make_pair(Status::OK(), true))));

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

        EXPECT_CALL(*skipListReader, SkipTo(_, _, _, _, _)).WillOnce(Return(std::make_pair(Status::OK(), false)));

        EXPECT_CALL(*skipListReader, GetLastValueInBuffer()).WillOnce(Return(150));

        ASSERT_TRUE(decoder.SkipTo(70, &state));
        ASSERT_EQ((int32_t)150, state.GetRecordOffset());
        ASSERT_EQ((int32_t)20, state.GetOffsetInRecord());

        // currentTTF in last record
        ASSERT_TRUE(decoder.SkipTo(90, &state));
        ASSERT_EQ((int32_t)150, state.GetRecordOffset());
        ASSERT_EQ((int32_t)40, state.GetOffsetInRecord());
    }
}

TEST_F(InMemPositionListDecoderTest, testSkipAndLocateAndDecodeWithoutPayload)
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

TEST_F(InMemPositionListDecoderTest, testSkipAndLocateAndDecodeWithPayload)
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

TEST_F(InMemPositionListDecoderTest, testDestruction)
{
    PositionListFormatOption option(NO_PAYLOAD);
    InMemPositionListDecoder* decoder =
        IE_POOL_COMPATIBLE_NEW_CLASS((&_byteSlicePool), InMemPositionListDecoder, option, nullptr);

    auto* skipListReader = new PairValueSkipListReader();
    auto* posListBuffer = new BufferedByteSlice(&_byteSlicePool, &_bufferPool);
    decoder->Init(100, skipListReader, posListBuffer);

    IE_POOL_COMPATIBLE_DELETE_CLASS(&_byteSlicePool, decoder);
}

} // namespace indexlib::index
