#ifndef __INDEXLIB_KEY_ITERATOR_H
#define __INDEXLIB_KEY_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(index, PostingIterator);

IE_NAMESPACE_BEGIN(index);

class KeyIterator
{
public:
    KeyIterator() {}
    virtual ~KeyIterator() {}

public:
    virtual bool HasNext() const = 0;
    virtual PostingIterator* NextPosting(std::string& key) = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KeyIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_KEY_ITERATOR_H
