#include "indexlib/index/normal/inverted_index/format/position_list_format.h"
#include "indexlib/common/numeric_compress/encoder_provider.h"

using namespace std;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PositionListFormat);

void PositionListFormat::Init(const PositionListFormatOption& option)
{
    uint8_t rowCount = 0;
    uint32_t offset = 0;

    mPosValue = new PosValue();
    mPosValue->SetLocation(rowCount++);
    mPosValue->SetOffset(offset);
    for (size_t i = 0; i < COMPRESS_MODE_SIZE; ++i)
    {
        mPosValue->SetEncoder(i, EncoderProvider::GetInstance()->GetPosListEncoder(i));
    }
    AddAtomicValue(mPosValue);
    offset += sizeof(pos_t);

    if (option.HasPositionPayload())
    {
        mPosPayloadValue = new PosPayloadValue();
        mPosPayloadValue->SetLocation(rowCount++);
        mPosPayloadValue->SetOffset(offset);
        for (size_t i = 0; i < COMPRESS_MODE_SIZE; ++i)
        {
            mPosPayloadValue->SetEncoder(i, 
                    EncoderProvider::GetInstance()->GetPosPayloadEncoder(i));
        }
        AddAtomicValue(mPosPayloadValue);
        offset += sizeof(pospayload_t);
    }
    InitPositionSkipListFormat(option);
}

void PositionListFormat::InitPositionSkipListFormat(
        const PositionListFormatOption& option)
{
    if (mPositionSkipListFormat)
    {
        DELETE_AND_SET_NULL(mPositionSkipListFormat);
    }
    mPositionSkipListFormat = new PositionSkipListFormat(option);
    assert(mPositionSkipListFormat);
}

IE_NAMESPACE_END(index);

