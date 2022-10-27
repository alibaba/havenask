#ifndef __INDEXLIB_BUILDING_TRIE_INDEX_READER_H
#define __INDEXLIB_BUILDING_TRIE_INDEX_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/trie/in_mem_trie_segment_reader.h"

IE_NAMESPACE_BEGIN(index);

class BuildingTrieIndexReader
{
public:
    BuildingTrieIndexReader() {}
    ~BuildingTrieIndexReader() {}

public:
    void AddSegmentReader(docid_t baseDocId,
                          const InMemTrieSegmentReaderPtr& inMemSegReader)
    {
        if (inMemSegReader)
        {
            mInnerSegReaders.push_back(std::make_pair(baseDocId, inMemSegReader));
        }
    }

    docid_t Lookup(const autil::ConstString& pkStr) const
    {
        typename SegmentReaders::const_reverse_iterator iter = mInnerSegReaders.rbegin();
        for (; iter != mInnerSegReaders.rend(); ++iter)
        {
            docid_t docId = iter->second->Lookup(pkStr);
            if (docId == INVALID_DOCID)
            {
                continue;
            }
            return iter->first + docId;
        }
        return INVALID_DOCID;
    }
    
private:
    typedef std::pair<docid_t, InMemTrieSegmentReaderPtr> SegmentReaderItem;
    typedef std::vector<SegmentReaderItem> SegmentReaders;
    SegmentReaders mInnerSegReaders;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuildingTrieIndexReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BUILDING_TRIE_INDEX_READER_H
