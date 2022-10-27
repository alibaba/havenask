#ifndef __INDEXLIB_DOC_INFO_H
#define __INDEXLIB_DOC_INFO_H

#include <tr1/memory>
#include <autil/StringUtil.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(index);

class DocInfo
{
public:
    void SetDocId(docid_t docId)
    {
        mDocId = docId;
    }

    docid_t IncDocId()
    {
        return ++mDocId;
    }

    docid_t GetDocId() const
    {
        return mDocId;
    }

    uint8_t* Get(size_t offset)
    {
        return (uint8_t *)this + offset;
    }

    const uint8_t* Get(size_t offset) const
    {
        return (uint8_t *)this + offset;
    }

private:
    docid_t mDocId;
};
DEFINE_SHARED_PTR(DocInfo);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DOC_INFO_H
