#ifndef __INDEXLIB_INDEX_ITERATOR_H
#define __INDEXLIB_INDEX_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(index, PostingDecoder);
IE_NAMESPACE_BEGIN(index);

class IndexIterator
{
public:
    IndexIterator() {}
    virtual ~IndexIterator() {}
    
public:
    virtual bool HasNext() const = 0;
    virtual PostingDecoder* Next(dictkey_t& key) = 0;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_ITERATOR_H
