#ifndef __INDEXLIB_SUMMARY_GROUP_FORMATTER_H
#define __INDEXLIB_SUMMARY_GROUP_FORMATTER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(util, BufferCompressor);
DECLARE_REFERENCE_CLASS(config, SummaryGroupConfig);
DECLARE_REFERENCE_CLASS(document, SummaryDocument);
DECLARE_REFERENCE_CLASS(document, SearchSummaryDocument);

IE_NAMESPACE_BEGIN(document);

class SummaryGroupFormatter
{
public:
    SummaryGroupFormatter(const config::SummaryGroupConfigPtr& summaryGroupConfig);
    SummaryGroupFormatter(const SummaryGroupFormatter& other);
    ~SummaryGroupFormatter();

public:
    size_t GetSerializeLength(const document::SummaryDocumentPtr& document) const;
    void SerializeSummary(const document::SummaryDocumentPtr& document,
                          char* value, size_t length) const;
    bool DeserializeSummary(document::SearchSummaryDocument* document,
                            const char* value, size_t length) const;

public:
    static uint32_t GetVUInt32Length(uint32_t value);
    static void WriteVUInt32(uint32_t value, char*& cursor);
    static uint32_t ReadVUInt32(const char*& cursor);

private:
    size_t GetSerializeLengthWithCompress(
            const document::SummaryDocumentPtr& document) const;
    bool DoDeserializeSummary(document::SearchSummaryDocument* document,
                              const char* value, size_t length) const;
    void InitCompressor();
private:
    config::SummaryGroupConfigPtr mSummaryGroupConfig;
    util::BufferCompressorPtr mCompressor;
    summaryfieldid_t mSummaryFieldIdBase;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SummaryGroupFormatter);

///////////////////////////////////////////////////////////////////////
inline uint32_t SummaryGroupFormatter::GetVUInt32Length(uint32_t value)
{
    uint32_t len = 1;
    while (value > 0x7F)
    {
        ++len; 
        value >>= 7;
    }
    return len; 
}

inline void SummaryGroupFormatter::WriteVUInt32(uint32_t value, char*& cursor)
{
    while (value > 0x7F)
    {
        *cursor++ = 0x80 | (value & 0x7F);
        value >>= 7;
    }
    *cursor++ = value & 0x7F;
}

inline uint32_t SummaryGroupFormatter::ReadVUInt32(const char*& cursor)
{
    char byte = *cursor++;
    uint32_t value = byte & 0x7F;
    int shift = 7;
    while(byte & 0x80)
    {
        byte = *cursor++;
        value |= ((byte & 0x7F) << shift);
        shift += 7;
    }
    return value;
}

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_SUMMARY_GROUP_FORMATTER_H
