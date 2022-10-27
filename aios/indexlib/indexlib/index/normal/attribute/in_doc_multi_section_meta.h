#ifndef __INDEXLIB_IN_DOC_MULTI_SECTION_META_H
#define __INDEXLIB_IN_DOC_MULTI_SECTION_META_H

#include "indexlib/common_define.h"
#include <tr1/memory>
#include <vector>
#include "indexlib/indexlib.h"
#include "indexlib/index/normal/inverted_index/accessor/in_doc_section_meta.h"
#include "indexlib/index/normal/attribute/multi_section_meta.h"
#include "indexlib/index_define.h"
#include "indexlib/config/package_index_config.h"

DECLARE_REFERENCE_CLASS(index, SectionAttributeReaderImpl);

IE_NAMESPACE_BEGIN(index);

class InDocMultiSectionMeta : public InDocSectionMeta, 
                              public MultiSectionMeta
{
public:
    InDocMultiSectionMeta(const config::PackageIndexConfigPtr& indexConfig)
        : mIndexConfig(indexConfig)
    {}

    ~InDocMultiSectionMeta() 
    {}

public:
    bool UnpackInDocBuffer(const SectionAttributeReaderImpl* reader, 
                           docid_t docId);

    section_len_t GetSectionLen(int32_t fieldPosition, 
                                sectionid_t sectId) const override;
    section_len_t GetSectionLenByFieldId(fieldid_t fieldId, 
            sectionid_t sectId) const override;

    section_weight_t GetSectionWeight(int32_t fieldPosition, 
            sectionid_t sectId) const override;
    section_weight_t GetSectionWeightByFieldId(fieldid_t fieldId, 
            sectionid_t sectId) const override;

    field_len_t GetFieldLen(int32_t fieldPosition) const override;
    field_len_t GetFieldLenByFieldId(fieldid_t fieldId) const override;

public:
    uint32_t GetSectionCountInField(int32_t fieldPosition) const override;

    uint32_t GetSectionCount() const override { return MultiSectionMeta::GetSectionCount(); }

    void GetSectionMeta(uint32_t idx, 
                        section_weight_t& sectionWeight,
                        int32_t& fieldPosition,
                        section_len_t& sectionLength) const override;

protected:
    inline int32_t GetFieldIdxInPack(fieldid_t fieldId) const
    {
        return mIndexConfig->GetFieldIdxInPack(fieldId);
    }

    inline int32_t GetFieldId(int32_t fieldPosition) const
    {
        return mIndexConfig->GetFieldId(fieldPosition);
    }

    void InitCache() const;

protected:
    uint8_t mDataBuf[MAX_SECTION_BUFFER_LEN];
    config::PackageIndexConfigPtr mIndexConfig;
    mutable std::vector<int32_t> mFieldPosition2Offset;
    mutable std::vector<int32_t> mFieldPosition2SectionCount;
    mutable std::vector<int32_t> mFieldPosition2FieldLen;

private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<InDocMultiSectionMeta> InDocMultiSectionMetaPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_DOC_MULTI_SECTION_META_H
