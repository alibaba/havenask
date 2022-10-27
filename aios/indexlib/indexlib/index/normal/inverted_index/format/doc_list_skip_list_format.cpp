#include "indexlib/index/normal/inverted_index/format/doc_list_skip_list_format.h"
#include "indexlib/common/numeric_compress/encoder_provider.h"

using namespace std;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DocListSkipListFormat);

void DocListSkipListFormat::Init(const DocListFormatOption& option)
{
    uint8_t rowCount = 0;
    uint32_t offset = 0;

#define ATOMIC_VALUE_INIT(value_type, atomic_value_type, encoder_func)  \
    m##atomic_value_type##Value = new atomic_value_type##Value();       \
    m##atomic_value_type##Value->SetLocation(rowCount++);               \
    m##atomic_value_type##Value->SetOffset(offset);                     \
    for (size_t i = 0; i < COMPRESS_MODE_SIZE; ++i)                     \
    {                                                                   \
        m##atomic_value_type##Value->SetEncoder(i,                      \
                EncoderProvider::GetInstance()->encoder_func(i));       \
    }                                                                   \
    AddAtomicValue(m##atomic_value_type##Value);                        \
    offset += sizeof(value_type);

    ATOMIC_VALUE_INIT(uint32_t, DocId, GetSkipListEncoder);

    if (option.HasTfList())
    {
        ATOMIC_VALUE_INIT(uint32_t, TotalTF, GetSkipListEncoder);
    }

    ATOMIC_VALUE_INIT(uint32_t, Offset, GetSkipListEncoder);
}

IE_NAMESPACE_END(index);

