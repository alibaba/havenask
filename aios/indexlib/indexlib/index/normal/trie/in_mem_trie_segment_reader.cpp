#include "indexlib/index/normal/trie/in_mem_trie_segment_reader.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemTrieSegmentReader);

InMemTrieSegmentReader::InMemTrieSegmentReader(
        ReadWriteLock* dataLock, KVMap* kvMap)
    : mDataLock(dataLock)
    , mKVMap(kvMap)
{
}

InMemTrieSegmentReader::~InMemTrieSegmentReader() 
{
    mDataLock = NULL;
    mKVMap = NULL;
}

IE_NAMESPACE_END(index);

