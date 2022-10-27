#include "indexlib/document/index_document/normal_document/summary_group_formatter.h"
#include "indexlib/document/index_document/normal_document/summary_document.h"
#include "indexlib/document/index_document/normal_document/search_summary_document.h"
#include "indexlib/util/buffer_compressor/buffer_compressor.h"
#include "indexlib/util/buffer_compressor/zlib_compressor.h"
#include "indexlib/util/buffer_compressor/zlib_default_compressor.h"
#include "indexlib/util/buffer_compressor/buffer_compressor_creator.h"
#include "indexlib/config/summary_group_config.h"
#include "indexlib/misc/exception.h"
#include <autil/ConstString.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, SummaryGroupFormatter);

SummaryGroupFormatter::SummaryGroupFormatter(const config::SummaryGroupConfigPtr& summaryGroupConfig)
    : mSummaryGroupConfig(summaryGroupConfig)
    , mSummaryFieldIdBase(summaryGroupConfig->GetSummaryFieldIdBase())
{
    InitCompressor();
}

SummaryGroupFormatter::SummaryGroupFormatter(const SummaryGroupFormatter& other)
    : mSummaryGroupConfig(other.mSummaryGroupConfig)
    , mSummaryFieldIdBase(mSummaryGroupConfig->GetSummaryFieldIdBase())
{
    InitCompressor();
}

SummaryGroupFormatter::~SummaryGroupFormatter() 
{
}

void SummaryGroupFormatter::InitCompressor()
{
    if (mSummaryGroupConfig && mSummaryGroupConfig->IsCompress())
    {
        const string& compressType = mSummaryGroupConfig->GetCompressType();

        if (compressType.empty())
        {
            mCompressor.reset(new ZlibDefaultCompressor());
        }
        else
        {
            mCompressor.reset(BufferCompressorCreator::CreateBufferCompressor(compressType));
        }
        if (!mCompressor)
        {
            IE_LOG(ERROR, "unsupported compressType[%s], use zlib_default instead",
                   compressType.c_str());
            mCompressor.reset(new ZlibDefaultCompressor());
        }
    }
}

size_t SummaryGroupFormatter::GetSerializeLength(const SummaryDocumentPtr& document) const
{
    if (mCompressor)
    {
        return GetSerializeLengthWithCompress(document);
    }
    else
    {
        size_t len = 0;
        SummaryGroupConfig::Iterator it = mSummaryGroupConfig->Begin();
        for (; it != mSummaryGroupConfig->End(); ++it)
        {
            fieldid_t fieldId = (*it)->GetFieldId();
            const ConstString& fieldValue = document->GetField(fieldId);
            uint32_t valueLen = fieldValue.length();
            len += GetVUInt32Length(valueLen) + valueLen;
        }
        return len;
    }
}

size_t SummaryGroupFormatter::GetSerializeLengthWithCompress(
        const SummaryDocumentPtr& document) const
{
    assert(mCompressor != NULL);
    assert(document);
    SummaryGroupConfig::Iterator it = mSummaryGroupConfig->Begin();
    for (; it != mSummaryGroupConfig->End(); ++it)
    {
        fieldid_t fieldId = (*it)->GetFieldId();
        const ConstString& fieldValue = document->GetField(fieldId);
        uint32_t valueLen = fieldValue.length();
        uint32_t vintLen = GetVUInt32Length(valueLen);
        uint64_t buffer;
        char* cursor = (char*)(&buffer);
        WriteVUInt32(valueLen, cursor);
        mCompressor->AddDataToBufferIn((const char*)(&buffer), vintLen);
        mCompressor->AddDataToBufferIn(fieldValue.data(), valueLen);
    }
    if (mCompressor->GetBufferInLen() > 0 && !mCompressor->Compress())
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "Compress summary FAILED.");
    }
    return mCompressor->GetBufferOutLen();
}

void SummaryGroupFormatter::SerializeSummary(const SummaryDocumentPtr& document,
                                        char* value, size_t length) const
{
    if (mCompressor)
    {
        size_t comLen = mCompressor->GetBufferOutLen();
        if (comLen > length)
        {
            INDEXLIB_FATAL_ERROR(BufferOverflow,
                    "The given buffer is too small!");
        }
        memcpy(value, mCompressor->GetBufferOut(), comLen);
        mCompressor->Reset();
    }
    else
    {
        char* cursor = value;
        char* end = value + length;
        SummaryGroupConfig::Iterator it = mSummaryGroupConfig->Begin();
        for (; it != mSummaryGroupConfig->End(); ++it)
        {
            fieldid_t fieldId = (*it)->GetFieldId();
            const ConstString& fieldValue = document->GetField(fieldId);
            uint32_t valueLen = fieldValue.length();
            if (unlikely(cursor + GetVUInt32Length(valueLen) + valueLen > end))
            {
                INDEXLIB_FATAL_ERROR(BufferOverflow, 
                        "The given buffer is too small!");
            }
            WriteVUInt32(valueLen, cursor);
            memcpy(cursor, fieldValue.data(), valueLen);
            cursor += valueLen;
        }
    }
}

bool SummaryGroupFormatter::DeserializeSummary(document::SearchSummaryDocument* document,
        const char* value, size_t length) const
{
    if (mCompressor)
    {
        mCompressor->Reset();
        mCompressor->AddDataToBufferIn(value, length);
        bool ret = mCompressor->Decompress();
        if(!ret)
        {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "Decompress summary FAILED.");
        }
        return DoDeserializeSummary(document, mCompressor->GetBufferOut(),
                mCompressor->GetBufferOutLen());
    }
    else 
    {
        return DoDeserializeSummary(document, value, length);
    }
}

bool SummaryGroupFormatter::DoDeserializeSummary(document::SearchSummaryDocument* document,
        const char* value, size_t length) const
{
    assert(document);
    if (unlikely(value == NULL || length == 0))
    {
        return false;
    }
    const char* cursor = value;
    const char* end = value + length;
    summaryfieldid_t summaryFieldId = mSummaryFieldIdBase;
    while (likely(cursor < end))
    {
        uint32_t valueLen = ReadVUInt32(cursor);
        if (likely(cursor + valueLen <= end))
        {
            document->SetFieldValue(summaryFieldId, cursor, valueLen);
            ++summaryFieldId;
            cursor += valueLen;
        }
        else
        {
            return false;
        }
    }
    if (likely(cursor == end))
    {
        return true;
    }
    return false;
}

IE_NAMESPACE_END(document);

