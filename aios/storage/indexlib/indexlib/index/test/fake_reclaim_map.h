#ifndef __INDEXLIB_FAKE_RECLAIM_MAP_H
#define __INDEXLIB_FAKE_RECLAIM_MAP_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class FakeReclaimMap : public ReclaimMap
{
    // used for testing sort-by-weight
    struct DocWeight {
        float weight;
        docid_t oldDocId;
    };

    struct DocWeightComparator {
        bool operator()(const DocWeight& left, const DocWeight& right) { return (left.weight < right.weight); }
    };

public:
    using ReclaimMap::Init;
    void Init(const index_base::SegmentMergeInfos& segMergeInfos, const DeletionMapReaderPtr& deletionMapReader,
              bool needReverse = false);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeReclaimMap);
}} // namespace indexlib::index

#endif //__INDEXLIB_FAKE_RECLAIM_MAP_H
