#ifndef __INDEXLIB_SECTION_META_H
#define __INDEXLIB_SECTION_META_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

IE_NAMESPACE_BEGIN(common);

// TODO: for optimize, length can be cumulative, so that position can binary search

struct SectionMeta
{
    section_weight_t weight;
    uint16_t fieldId : 5; // this is fieldIdxInPack, not real fieldId of FieldSchema
    uint16_t length : 11; 
    SectionMeta(section_weight_t w = 0, fieldid_t fid = INVALID_FIELDID, section_len_t l = 0)
        : weight(w)
        , fieldId((uint8_t)fid)
        , length((uint16_t)l)
    {}

    bool operator==(const SectionMeta& other) const
    {
        return (weight == other.weight && length == other.length && fieldId == other.fieldId);
    }

    bool operator!=(const SectionMeta& other) const
    {
        return !(*this == other);
    }
};

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SECTION_META_H
