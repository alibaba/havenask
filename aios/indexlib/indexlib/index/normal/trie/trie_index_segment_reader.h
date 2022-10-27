#ifndef __INDEXLIB_TRIE_INDEX_SEGMENT_READER_H
#define __INDEXLIB_TRIE_INDEX_SEGMENT_READER_H

#include <tr1/memory>
#include <autil/ConstString.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/index/normal/trie/double_array_trie.h"
#include <autil/mem_pool/pool_allocator.h>

IE_NAMESPACE_BEGIN(index);

class TrieIndexSegmentReader
{
public:
    typedef std::pair<autil::ConstString, docid_t> KVPair;
    typedef std::vector<KVPair, autil::mem_pool::pool_allocator<KVPair> > KVPairVector;

public:
    TrieIndexSegmentReader()
        : mData(NULL)
    {}
    ~TrieIndexSegmentReader() {}
public:
    void Open(const config::IndexConfigPtr& indexConfig,
              const index_base::SegmentData& segmentData);
    docid_t Lookup(const autil::ConstString& pkStr) const
    { return DoubleArrayTrie::Match(mData, pkStr); }

    size_t PrefixSearch(const autil::ConstString& key, KVPairVector& results,
                        int32_t maxMatches) const
    { return DoubleArrayTrie::PrefixSearch(mData, key, results, maxMatches); }

private:
    file_system::DirectoryPtr GetDirectory(
            const file_system::DirectoryPtr& segmentDirectory,
            const config::IndexConfigPtr& indexConfig);
private:
    file_system::FileReaderPtr mDataFile;
    char* mData;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TrieIndexSegmentReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TRIE_INDEX_SEGMENT_READER_H
