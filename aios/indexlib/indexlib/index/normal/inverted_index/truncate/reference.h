#ifndef __INDEXLIB_REFERENCE_H
#define __INDEXLIB_REFERENCE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_info.h"

IE_NAMESPACE_BEGIN(index);

class Reference
{
public:
    Reference(size_t offset, FieldType fieldType);
    virtual ~Reference();

public:
    size_t GetOffset() const;
    FieldType GetFieldType() const;

    virtual std::string GetStringValue(DocInfo* docInfo) = 0;

protected:
    size_t mOffset;
    FieldType mFieldType;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(Reference);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_REFERENCE_H
