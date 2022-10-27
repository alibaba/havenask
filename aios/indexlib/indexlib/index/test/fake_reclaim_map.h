#ifndef __INDEXLIB_FAKE_RECLAIM_MAP_H
#define __INDEXLIB_FAKE_RECLAIM_MAP_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"

IE_NAMESPACE_BEGIN(index);

class FakeReclaimMap : public ReclaimMap
{
    //used for testing sort-by-weight
    struct DocWeight
    {
        float weight;
        docid_t oldDocId;
    };
    
    struct DocWeightComparator
    {
        bool operator()(const DocWeight& left, const DocWeight& right)
        {
            return (left.weight < right.weight);
        }
    };
    
public:
    void Init(const index_base::SegmentMergeInfos& segMergeInfos, 
              const DeletionMapReaderPtr& deletionMapReader,
              bool needReverse = false);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeReclaimMap);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_FAKE_RECLAIM_MAP_H
