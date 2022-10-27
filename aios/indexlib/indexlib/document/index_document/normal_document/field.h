#ifndef __INDEXLIB_WP_FIELD_H
#define __INDEXLIB_WP_FIELD_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include <tr1/memory>
#include <autil/DataBuffer.h>

IE_NAMESPACE_BEGIN(document);

#pragma pack(push, 2)

class Field
{
public:
    const static size_t MAX_SECTION_PER_FIELD = 256;
    const static uint8_t FIELD_DESCRIPTOR_MASK = 0x80;
    enum class FieldTag : int8_t {
        // field tag range [0, 127]
        TOKEN_FIELD = 0,
        RAW_FIELD = 1,
        UNKNOWN_FIELD = 127,
    };
public:
    Field(autil::mem_pool::Pool* pool, FieldTag fieldTag);
    virtual ~Field();

public:
    virtual void Reset() = 0;
    virtual void serialize(autil::DataBuffer &dataBuffer) const = 0;
    virtual void deserialize(autil::DataBuffer &dataBuffer) = 0;
    virtual bool operator==(const Field& field) const;
    virtual bool operator!=(const Field& field) const { return !(*this == field); } 

public:
    fieldid_t GetFieldId() const { return mFieldId; }
    void SetFieldId(fieldid_t fieldId) { mFieldId = fieldId; }
    FieldTag GetFieldTag() const { return mFieldTag; }
    
protected:
    autil::mem_pool::Pool* mPool;
    fieldid_t mFieldId;
    FieldTag mFieldTag;
};

#pragma pack(pop)

DEFINE_SHARED_PTR(Field);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_WP_FIELD_H
