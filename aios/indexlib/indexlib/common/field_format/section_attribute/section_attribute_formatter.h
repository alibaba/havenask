#ifndef __INDEXLIB_SECTION_ATTRIBUTE_FORMATTER_H
#define __INDEXLIB_SECTION_ATTRIBUTE_FORMATTER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/section_attribute_config.h"
#include "indexlib/common/field_format/section_attribute/section_attribute_encoder.h"

IE_NAMESPACE_BEGIN(common);

class SectionAttributeFormatter
{
public:
    SectionAttributeFormatter(
            const config::SectionAttributeConfigPtr& sectionAttrConfig);
    ~SectionAttributeFormatter();
public:
    std::string Encode(section_len_t *lengths, section_fid_t *fids, 
                       section_weight_t *weights, uint32_t sectionCount);

    uint32_t EncodeToBuffer(
        section_len_t *lengths, section_fid_t *fids, 
        section_weight_t *weights, uint32_t sectionCount,
	uint8_t* buf);
    
    void Decode(const char* dataBuf, uint32_t dataLen, uint8_t* buffer, uint32_t bufLength);
    void Decode(const std::string& value, uint8_t* buffer, uint32_t bufLength);

    static uint32_t UnpackBuffer(const uint8_t* buffer, 
                             bool hasFieldId, 
                             bool hasSectionWeight, 
                             section_len_t* &lengthBuf, 
                             section_fid_t* &fidBuf, 
                             section_weight_t* &weightBuf);

public:
    static const uint32_t DATA_SLICE_LEN = 16 * 1024;
private:
    config::SectionAttributeConfigPtr mSectionAttrConfig;
private:
    
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SectionAttributeFormatter);
///////////////////////////////////////////////////////

inline void SectionAttributeFormatter::Decode(
        const char* dataBuf, uint32_t dataLen,
        uint8_t* buffer, uint32_t bufLength)
{
    const uint8_t* srcCursor = (const uint8_t*)dataBuf;
    uint8_t* destCursor = buffer;
    uint8_t* destEnd = buffer + bufLength;
    SectionAttributeEncoder::DecodeSectionLens(
            srcCursor, destCursor, destEnd);

    uint32_t sectionCount = *((section_len_t*)buffer);
    if (mSectionAttrConfig->HasFieldId())
    {
        SectionAttributeEncoder::DecodeSectionFids(
                srcCursor, sectionCount, destCursor, destEnd);
    }

    if (mSectionAttrConfig->HasSectionWeight())
    {
        const uint8_t* srcEnd = (const uint8_t*)dataBuf + dataLen;
        SectionAttributeEncoder::DecodeSectionWeights(
                srcCursor, srcEnd, sectionCount, destCursor, destEnd);
    }
}

inline void SectionAttributeFormatter::Decode(
        const std::string& value, uint8_t* buffer, uint32_t bufLength)
{
    Decode((const char*)value.data(), (uint32_t)value.size(), buffer, bufLength);
}


IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SECTION_ATTRIBUTE_FORMATTER_H
