#ifndef __INDEXLIB_AITHETA_INDEX_REDUCE_ITEM_H
#define __INDEXLIB_AITHETA_INDEX_REDUCE_ITEM_H

#include <deque>
#include <map>
#include <memory>
#include <tuple>

#include <indexlib/common_define.h>
#include <indexlib/misc/log.h>
#include <indexlib/file_system/file_reader.h>
#include <indexlib/index/normal/inverted_index/customized_index/index_reduce_item.h>
#include <indexlib/index/normal/inverted_index/customized_index/indexer_define.h>
#include <indexlib/indexlib.h>

#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/index_segment.h"
#include "aitheta_indexer/plugins/aitheta/raw_segment.h"
#include "aitheta_indexer/plugins/aitheta/parallel_segment.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

class AithetaIndexReduceItem : public IE_NAMESPACE(index)::IndexReduceItem {
 public:
    AithetaIndexReduceItem(){};
    ~AithetaIndexReduceItem(){};

 public:
    bool LoadIndex(const file_system::DirectoryPtr &indexDir) override;
    bool UpdateDocId(const DocIdMap &docIdMap) override;
    SegmentPtr GetSegment() const { return mSegment; }

 private:
    SegmentPtr mSegment;

 private:
    IE_LOG_DECLARE();
    friend class AithetaIndexReducer;
    friend class AithetaIndexerTest;
};

DEFINE_SHARED_PTR(AithetaIndexReduceItem);

IE_NAMESPACE_END(aitheta_plugin);

#endif  //__INDEXLIB_AITHETA_INDEX_REDUCE_ITEM_H
