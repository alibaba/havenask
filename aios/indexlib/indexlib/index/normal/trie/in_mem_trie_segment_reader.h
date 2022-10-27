#ifndef __INDEXLIB_IN_MEM_TRIE_SEGMENT_READER_H
#define __INDEXLIB_IN_MEM_TRIE_SEGMENT_READER_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_segment_reader.h"
#include "indexlib/index/normal/trie/double_array_trie.h"

IE_NAMESPACE_BEGIN(index);

class InMemTrieSegmentReader : public index::IndexSegmentReader
{
public:
    typedef std::map<autil::ConstString, docid_t, std::less<autil::ConstString>,
                     autil::mem_pool::pool_allocator<
                         std::pair<const autil::ConstString, docid_t> > > KVMap;
public:
    InMemTrieSegmentReader(autil::ReadWriteLock* dataLock, KVMap* kvMap);
    ~InMemTrieSegmentReader();
public:
    docid_t Lookup(const autil::ConstString& pkStr) const;
    //TODO: support, if need in the future
    // bool PrefixSearch(const autil::ConstString& key, KVPairVector& results,
    //                   int32_t maxMatches) const;
private:
    mutable autil::ReadWriteLock* mDataLock;
    KVMap* mKVMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemTrieSegmentReader);
//////////////////////////////////////////////////////////////////////////
inline docid_t InMemTrieSegmentReader::Lookup(
        const autil::ConstString& pkStr) const
{
    autil::ScopedReadWriteLock lock(*mDataLock, 'R');
    const KVMap::const_iterator& iter = mKVMap->find(pkStr);
    if (iter == mKVMap->end())
    {
        return INVALID_DOCID;
    }
    return iter->second;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_MEM_TRIE_SEGMENT_READER_H
