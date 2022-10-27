#ifndef __INDEXLIB_IN_DOC_SECTION_META_H
#define __INDEXLIB_IN_DOC_SECTION_META_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>

IE_NAMESPACE_BEGIN(index);

class InDocSectionMeta
{
public:
    InDocSectionMeta() {}
    virtual ~InDocSectionMeta() {}

public:
    // common interface
    virtual section_len_t GetSectionLen(int32_t fieldPosition, 
            sectionid_t sectId) const = 0;
    virtual section_len_t GetSectionLenByFieldId(fieldid_t fieldId, 
            sectionid_t sectId) const = 0;

    virtual section_weight_t GetSectionWeight(int32_t fieldPosition, 
            sectionid_t sectId) const = 0;
    virtual section_weight_t GetSectionWeightByFieldId(fieldid_t fieldId, 
            sectionid_t sectId) const = 0;

    virtual field_len_t GetFieldLen(int32_t fieldPosition) const = 0;
    virtual field_len_t GetFieldLenByFieldId(fieldid_t fieldId) const = 0;

public:
    // interface to iterate
    virtual uint32_t GetSectionCountInField(int32_t fieldPosition) const = 0;
    virtual uint32_t GetSectionCount() const = 0;

    virtual void GetSectionMeta(uint32_t idx, 
                                section_weight_t& sectionWeight,
                                int32_t& fieldPosition,
                                section_len_t& sectionLength) const = 0;
};

typedef std::tr1::shared_ptr<InDocSectionMeta> InDocSectionMetaPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_DOC_SECTION_META_H
