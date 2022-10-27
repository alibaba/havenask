#ifndef __INDEXLIB_PRIMARY_KEY_BUILDING_INDEX_READER_H
#define __INDEXLIB_PRIMARY_KEY_BUILDING_INDEX_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/in_mem_primary_key_segment_reader_typed.h"

IE_NAMESPACE_BEGIN(index);

template<typename Key>
class PrimaryKeyBuildingIndexReader
{
public:
    typedef std::tr1::shared_ptr<InMemPrimaryKeySegmentReaderTyped<Key> > SegmentReaderPtr;

public:
    PrimaryKeyBuildingIndexReader() {}
    ~PrimaryKeyBuildingIndexReader() {}
    
public:
    void AddSegmentReader(docid_t baseDocId,
                          const SegmentReaderPtr& inMemSegReader)
    {
        if (inMemSegReader)
        {
            mInnerSegReaders.push_back(std::make_pair(baseDocId, inMemSegReader));
        }
    }

    docid_t Lookup(const Key& hashKey) const
    {
        typename SegmentReaders::const_reverse_iterator iter = mInnerSegReaders.rbegin();
        for (; iter != mInnerSegReaders.rend(); ++iter)
        {
            docid_t docId = iter->second->Lookup(hashKey);
            if (docId == INVALID_DOCID)
            {
                continue;
            }
            return iter->first + docId;
        }
        return INVALID_DOCID;
    }
    
private:
    typedef std::pair<docid_t, SegmentReaderPtr> SegmentReaderItem;
    typedef std::vector<SegmentReaderItem> SegmentReaders;
    SegmentReaders mInnerSegReaders;
    
private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_BUILDING_INDEX_READER_H
