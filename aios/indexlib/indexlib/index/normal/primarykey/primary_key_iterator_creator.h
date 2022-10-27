#ifndef __INDEXLIB_PRIMARY_KEY_ITERATOR_CREATOR_H
#define __INDEXLIB_PRIMARY_KEY_ITERATOR_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/primary_key_iterator.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/index/normal/primarykey/hash_primary_key_iterator.h"
#include "indexlib/index/normal/primarykey/sequential_primary_key_iterator.h"

IE_NAMESPACE_BEGIN(index);

class PrimaryKeyIteratorCreator
{
public:
    PrimaryKeyIteratorCreator() {}
    ~PrimaryKeyIteratorCreator() {}
public:
    template <typename Key>
    static std::tr1::shared_ptr<PrimaryKeyIterator<Key> > Create(
        const config::PrimaryKeyIndexConfigPtr& indexConfig);
};

//////////////////////////////////////////////////////////////////////
template <typename Key>
std::tr1::shared_ptr<PrimaryKeyIterator<Key> > PrimaryKeyIteratorCreator::Create(
    const config::PrimaryKeyIndexConfigPtr& indexConfig)
{
    typedef std::tr1::shared_ptr<PrimaryKeyIterator<Key> > PrimaryKeyIteratorPtr;
    // TODO : check load mode
    switch(indexConfig->GetPrimaryKeyIndexType())
    {
    case pk_hash_table:
        return PrimaryKeyIteratorPtr(new HashPrimaryKeyIterator<Key>(indexConfig));
    case pk_sort_array:
        return PrimaryKeyIteratorPtr(new SequentialPrimaryKeyIterator<Key>(indexConfig));
    default:
        assert(false);
    }
    return PrimaryKeyIteratorPtr();
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_ITERATOR_CREATOR_H
