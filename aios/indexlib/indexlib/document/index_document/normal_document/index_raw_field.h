#ifndef __INDEXLIB_INDEX_RAW_FIELD_H
#define __INDEXLIB_INDEX_RAW_FIELD_H

#include <tr1/memory>
#include <autil/ConstString.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/field.h"

IE_NAMESPACE_BEGIN(document);

class IndexRawField : public Field
{
public:
    IndexRawField(autil::mem_pool::Pool* pool = NULL);
    ~IndexRawField();
public:
    void Reset() override;
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    bool operator==(const Field& field) const override;
    bool operator!=(const Field& field) const override { return !(*this == field); }

public:
    void SetData(const autil::ConstString& data) { mData = data; }
    autil::ConstString GetData() const { return mData; }

private:
    autil::ConstString mData;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexRawField);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_INDEX_RAW_FIELD_H
