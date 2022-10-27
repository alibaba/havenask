#ifndef __INDEXLIB_DOC_FILTER_H
#define __INDEXLIB_DOC_FILTER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/misc/log.h"
#include "indexlib/misc/common.h"

IE_NAMESPACE_BEGIN(merger);

class DocFilter
{
public:
    DocFilter() {}
    virtual ~DocFilter() {}
public:
    virtual bool Filter(docid_t docid) = 0;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocFilter);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_DOC_FILTER_H
