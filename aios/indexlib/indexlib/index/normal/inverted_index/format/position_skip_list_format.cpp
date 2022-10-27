#include "indexlib/index/normal/inverted_index/format/position_skip_list_format.h"
#include "indexlib/common/numeric_compress/encoder_provider.h"

using namespace std;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PositionSkipListFormat);

void PositionSkipListFormat::Init(const PositionListFormatOption& option)
{
    uint8_t rowCount = 0;
    uint32_t offset = 0;

    if (!option.HasTfBitmap())
    {
        mTotalPosValue = new TotalPosValue();
        mTotalPosValue->SetLocation(rowCount++);
        mTotalPosValue->SetOffset(offset);
        for (size_t i = 0; i < COMPRESS_MODE_SIZE; ++i)
        {
            mTotalPosValue->SetEncoder(i, 
                    EncoderProvider::GetInstance()->GetSkipListEncoder(i));
        }
        AddAtomicValue(mTotalPosValue);
        offset += sizeof(uint32_t);
    }

    mOffsetValue = new OffsetValue();
    mOffsetValue->SetLocation(rowCount++);
    mOffsetValue->SetOffset(offset);
    for (size_t i = 0; i < COMPRESS_MODE_SIZE; ++i)
    {
        const Int32Encoder* encoder = NULL;
        if (option.HasTfBitmap())
        {
            encoder = EncoderProvider::GetInstance()->GetSkipListEncoder(
                    SHORT_LIST_COMPRESS_MODE);
        }
        else
        {
            encoder = EncoderProvider::GetInstance()->GetSkipListEncoder(i);
        }
        mOffsetValue->SetEncoder(i, encoder);
    }
    AddAtomicValue(mOffsetValue);
}

IE_NAMESPACE_END(index);

