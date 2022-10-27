#ifndef __INDEXLIB_DICTIONARY_ITERATOR_H
#define __INDEXLIB_DICTIONARY_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(index);

class DictionaryIterator
{
public:
    DictionaryIterator(){}
    virtual ~DictionaryIterator(){}

public:
    virtual bool HasNext() const = 0;
    virtual void Next(dictkey_t& key, dictvalue_t& value) = 0;
    
private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<DictionaryIterator> DictionaryIteratorPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DICTIONARY_ITERATOR_H
