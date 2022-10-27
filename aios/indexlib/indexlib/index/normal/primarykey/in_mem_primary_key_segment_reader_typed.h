#ifndef __INDEXLIB_IN_MEM_PRIMARY_KEY_SEGMENT_READER_TYPED_H
#define __INDEXLIB_IN_MEM_PRIMARY_KEY_SEGMENT_READER_TYPED_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/hash_map.h"
#include "indexlib/index/normal/inverted_index/accessor/index_segment_reader.h"

DECLARE_REFERENCE_CLASS(index, AttributeSegmentReader);
IE_NAMESPACE_BEGIN(index);

template <typename Key>
class InMemPrimaryKeySegmentReaderTyped : public IndexSegmentReader
{
public:
    typedef util::HashMap<Key, docid_t> HashMapTyped;
    typedef std::tr1::shared_ptr<HashMapTyped> HashMapTypedPtr;
    typedef typename util::HashMap<Key, docid_t>::KeyValuePair KeyValuePair;
    typedef typename HashMapTyped::Iterator Iterator;
public:
    InMemPrimaryKeySegmentReaderTyped(HashMapTypedPtr hashMap, 
            AttributeSegmentReaderPtr pkAttrReader = AttributeSegmentReaderPtr());

public:
    docid_t Lookup(const Key& key) const
    { return mHashMap->Find(key, INVALID_DOCID); }

    AttributeSegmentReaderPtr GetPKAttributeReader() const
    { return mPKAttrReader; }
    Iterator CreateIterator()
    {
        return mHashMap->CreateIterator();
    }
    
private:
    HashMapTypedPtr mHashMap;
    AttributeSegmentReaderPtr mPKAttrReader;
private:
    IE_LOG_DECLARE();
};

template <typename Key>
InMemPrimaryKeySegmentReaderTyped<Key>::InMemPrimaryKeySegmentReaderTyped(
        HashMapTypedPtr hashMap, 
        AttributeSegmentReaderPtr pkAttrReader)
    : mHashMap(hashMap)
    , mPKAttrReader(pkAttrReader)
{
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_MEM_PRIMARY_KEY_SEGMENT_READER_TYPED_H
