#ifndef __INDEXLIB_FAKE_PARTITION_READER_SNAPSHOT_H
#define __INDEXLIB_FAKE_PARTITION_READER_SNAPSHOT_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/partition/partition_reader_snapshot.h"

IE_NAMESPACE_BEGIN(testlib);

class FakePartitionReaderSnapshot : public partition::PartitionReaderSnapshot
{
public:
    FakePartitionReaderSnapshot(partition::TableMem2IdMap* indexName2IdMap, 
                                partition::TableMem2IdMap* attrName2IdMap, 
                                partition::TableMem2IdMap* packAttrName2IdMap, 
                                const std::vector<partition::IndexPartitionReaderInfo> &indexPartReaderInfos)
        : PartitionReaderSnapshot(indexName2IdMap, attrName2IdMap, packAttrName2IdMap, indexPartReaderInfos)
        , mIndexName2IdMap(indexName2IdMap)
        , mAttrName2IdMap(attrName2IdMap)
        , mPackAttrName2IdMap(packAttrName2IdMap)
    {
    }
    ~FakePartitionReaderSnapshot()
    {
        DELETE_AND_SET_NULL(mIndexName2IdMap);
        DELETE_AND_SET_NULL(mAttrName2IdMap);
        DELETE_AND_SET_NULL(mPackAttrName2IdMap);
    }
private:
    partition::TableMem2IdMap *mIndexName2IdMap;
    partition::TableMem2IdMap *mAttrName2IdMap;
    partition::TableMem2IdMap *mPackAttrName2IdMap;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakePartitionReaderSnapshot);

IE_NAMESPACE_END(testlib);

#endif //__INDEXLIB_FAKE_PARTITION_READER_SNAPSHOT_H
