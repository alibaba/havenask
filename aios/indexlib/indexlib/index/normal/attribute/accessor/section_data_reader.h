#ifndef __INDEXLIB_SECTION_DATA_READER_H
#define __INDEXLIB_SECTION_DATA_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/section_attribute_config.h"
#include "indexlib/index/normal/attribute/accessor/string_attribute_reader.h"

IE_NAMESPACE_BEGIN(index);

class SectionDataReader : public StringAttributeReader
{
public:
    using StringAttributeReader::Read;
public:
    SectionDataReader();
    SectionDataReader(const SectionDataReader& reader);
    ~SectionDataReader();

protected:
    file_system::DirectoryPtr GetAttributeDirectory(
            const index_base::SegmentData& segmentData,
            const config::AttributeConfigPtr& attrConfig) const override
    {
        std::string indexName =
            config::SectionAttributeConfig::SectionAttributeNameToIndexName(
                    attrConfig->GetAttrName());
        return segmentData.GetSectionAttributeDirectory(indexName, true);
    }

    void InitBuildingAttributeReader(const index_base::SegmentIteratorPtr& segIter) override;

public:
    bool Read(docid_t docId, std::string& attrValue,
              autil::mem_pool::Pool* pool = NULL) const override;
    
    AttributeReader* Clone() const override;
    
    AttrType GetType() const override;
    bool IsMultiValue() const override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SectionDataReader);

//////////////////////////////////////////////////////
inline bool SectionDataReader::Read(docid_t docId, std::string& attrValue,
                                    autil::mem_pool::Pool* pool) const
{
    autil::MultiValueType<char> value;
    if (!Read(docId, value, pool))
    {
        return false;
    }
    attrValue = std::string(value.data(), value.size());
    return true;
}

inline AttrType SectionDataReader::GetType() const
{
    return AT_STRING;
}

inline bool SectionDataReader::IsMultiValue() const
{
    return false;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SECTION_DATA_READER_H
