#ifndef __INDEXLIB_SECTION_ATTRIBUTE_READER_IMPL_H
#define __INDEXLIB_SECTION_ATTRIBUTE_READER_IMPL_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/index/normal/inverted_index/accessor/section_attribute_reader.h"
#include "indexlib/index/normal/attribute/multi_section_meta.h"
#include "indexlib/index/normal/attribute/in_doc_multi_section_meta.h"
#include "indexlib/index/normal/attribute/accessor/section_data_reader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/common/field_format/section_attribute/section_attribute_formatter.h"

IE_NAMESPACE_BEGIN(index);

/**
 * section attribute reader implement
 */
class SectionAttributeReaderImpl : public SectionAttributeReader
{
public:
    SectionAttributeReaderImpl();
    SectionAttributeReaderImpl(const SectionAttributeReaderImpl& sectAttrReader);
    virtual ~SectionAttributeReaderImpl();

public:
    // virtual for test
    virtual void Open(const config::PackageIndexConfigPtr& indexConfig,
                      const index_base::PartitionDataPtr& partitionData);

    // virtual for test
    virtual SectionAttributeReader* Clone() const;

    bool HasFieldId() const
    {
        return mIndexConfig->GetSectionAttributeConfig()->HasFieldId();
    }
    bool HasSectionWeight() const
    {
        return mIndexConfig->GetSectionAttributeConfig()->HasSectionWeight();
    }
    fieldid_t GetFieldId(int32_t fieldIdxInPack) const override
    {
        return mIndexConfig->GetFieldId(fieldIdxInPack);
    }
    InDocSectionMetaPtr GetSection(docid_t docId) const override;
    virtual int32_t Read(docid_t docId, uint8_t* buffer, uint32_t bufLen) const;

private:
    config::PackageIndexConfigPtr mIndexConfig;
    SectionDataReaderPtr mSectionDataReader;
    common::SectionAttributeFormatterPtr mFormatter;

private:
    friend class SectionAttributeReaderImplTest;
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<SectionAttributeReaderImpl> SectionAttributeReaderImplPtr;

//////////////////////////////////////////////////////////////
inline InDocSectionMetaPtr SectionAttributeReaderImpl::GetSection(
        docid_t docId) const
{
    InDocMultiSectionMeta* multiMeta = new InDocMultiSectionMeta(mIndexConfig);
    InDocSectionMetaPtr inDocSectionMeta(multiMeta);
    if (multiMeta->UnpackInDocBuffer(this, docId))
    {
        return inDocSectionMeta;
    }
    return InDocSectionMetaPtr();
}

inline int32_t SectionAttributeReaderImpl::Read(docid_t docId,
        uint8_t* buffer, uint32_t bufLen) const
{ 
    assert(mSectionDataReader);
    assert(mFormatter);
    autil::MultiValueType<char> value;
    if (!mSectionDataReader->Read(docId, value))
    {
        IE_LOG(ERROR, "Invalid doc id(%d).", docId);
        return -1;
    }

    // TODO: use MultiValueReader for performance
    mFormatter->Decode(value.data(), value.size(), 
                       buffer, bufLen);
    return 0;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SECTION_ATTRIBUTE_READER_IMPL_H
