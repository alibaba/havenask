#include "indexlib/index/normal/inverted_index/format/position_list_encoder.h"
#include "indexlib/index/normal/inverted_index/perf_test/in_mem_position_list_decoder_perf_unittest.h"
#include "indexlib/file_system/buffered_file_writer.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemPositionListDecoderPerfTest);

InMemPositionListDecoderPerfTest::InMemPositionListDecoderPerfTest()
    : mByteSlicePool(1024 * 1024 * 10)
    , mBufferPool(1024 * 1024 * 10)
    , mGlobalTTF(0)
    , mIsDumped(false)
    , mFlag(of_position_list | of_term_frequency)
{
}

InMemPositionListDecoderPerfTest::~InMemPositionListDecoderPerfTest()
{
}

void InMemPositionListDecoderPerfTest::CaseSetUp()
{
    PositionListFormatOption option(mFlag);
    mEncoder.reset(new PositionListEncoder(option, &mByteSlicePool, &mBufferPool));
    mGlobalTTF = 0;
    mIsDumped = false;
    mDir = GET_TEST_DATA_PATH();
}

void InMemPositionListDecoderPerfTest::CaseTearDown()
{
}

void InMemPositionListDecoderPerfTest::DoWrite()
{
    assert(mEncoder);
    mGlobalTTF = 0;
    pos_t lastPos = 0;
    while (!IsFinished())
    {
        size_t pos = lastPos + GetPositionDeltaByIdx(mGlobalTTF);
        if (pos > (size_t)std::numeric_limits<pos_t>::max())
        {
            break;
        }

        mEncoder->AddPosition(pos, 0);
        lastPos = pos;
        mGlobalTTF++;
    }
    mEncoder->EndDocument();

    string filePath = mDir + "dump_file";
    BufferedFileWriterPtr fileWriter(new BufferedFileWriter());
    fileWriter->Open(filePath);
    mEncoder->Dump(fileWriter);
    fileWriter->Close();

    sleep(2);
    mIsDumped = true;
}

void InMemPositionListDecoderPerfTest::DoRead(int* errCode)
{
    autil::mem_pool::Pool sessionPool(1024 * 1024 * 10);
    size_t readCount = 0;
    pos_t posBuffer[MAX_POS_PER_RECORD];
    pospayload_t payloadBuffer[MAX_POS_PER_RECORD];
    while (!IsFinished() || !mIsDumped)
    {
        readCount++;
        ttf_t ttf = mGlobalTTF;
        InMemPositionListDecoder* posListDecoder = 
            mEncoder->GetInMemPositionListDecoder(&sessionPool);

        int32_t recordOffset = 0;
        NormalInDocState state(SHORT_LIST_COMPRESS_MODE, mFlag);
        for (size_t i = 0; i < (size_t)ttf; ++i)
        {
            if (!posListDecoder->SkipTo(i, &state))
            {
                *errCode = -1;
                return;
            }
            uint32_t tempTF = 0;
            if (i % MAX_POS_PER_RECORD == 0)
            {
                if (!posListDecoder->LocateRecord(&state, tempTF))
                {
                    *errCode = -2;
                    return;
                }

                recordOffset = state.GetRecordOffset();
                // check decode buffer
                uint32_t decodeCount = posListDecoder->DecodeRecord(posBuffer, 
                        MAX_POS_PER_RECORD, payloadBuffer, MAX_POS_PER_RECORD);
                uint32_t expectDecodeCount = std::min(uint32_t(ttf - i), MAX_POS_PER_RECORD);
                if (decodeCount < expectDecodeCount)
                {
                    *errCode = -3;
                    return;
                }

                for (size_t j = 0; j < (size_t)expectDecodeCount; ++j)
                {
                    if (posBuffer[j] != GetPositionDeltaByIdx(i + j))
                    {
                        *errCode = -4;
                        return;
                    }
                }
            }
            else
            {
                if (posListDecoder->LocateRecord(&state, tempTF))
                {
                    *errCode = -5;
                    return;
                }
                if (recordOffset != state.GetRecordOffset())
                {
                    *errCode = -6;
                    return;
                }
            }

            if ((int32_t)(i % MAX_POS_PER_RECORD) != state.GetOffsetInRecord())
            {
                *errCode = -7;
                return;
            }
        }
    }
    cout << readCount << endl;
}

void InMemPositionListDecoderPerfTest::TestGetInMemPositionListDecoder()
{
    DoMultiThreadTest(10, 5);
}

IE_NAMESPACE_END(index);

