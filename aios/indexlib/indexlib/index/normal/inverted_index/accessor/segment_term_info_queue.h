#ifndef __INDEXLIB_SEGMENT_TERM_INFO_QUEUE_H
#define __INDEXLIB_SEGMENT_TERM_INFO_QUEUE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_term_info.h"
#include "indexlib/index/normal/inverted_index/accessor/on_disk_index_iterator_creator.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index/segment_directory_base.h"
#include "indexlib/config/index_config.h"

IE_NAMESPACE_BEGIN(index);

class SegmentTermInfoQueue
{
public:
    SegmentTermInfoQueue(
            const config::IndexConfigPtr& indexConf,
            const OnDiskIndexIteratorCreatorPtr &onDiskIndexIterCreator);
    virtual ~SegmentTermInfoQueue();

public:
    void Init(const SegmentDirectoryBasePtr& segDir,
              const index_base::SegmentMergeInfos& segMergeInfos);

    inline bool Empty() const { return mSegmentTermInfos.empty(); }
    const SegmentTermInfos& CurrentTermInfos(dictkey_t &key, 
            SegmentTermInfo::TermIndexMode &termMode);

    void MoveToNextTerm();

public:
    // for test
    SegmentTermInfo* Top() const { return mSegmentTermInfos.top(); }
    size_t Size() const { return mSegmentTermInfos.size(); }

private:
    void AddOnDiskTermInfo(const index_base::SegmentMergeInfo& segMergeInfo);
    void AddQueueItem(const index_base::SegmentMergeInfo& segMergeInfo,
                      const IndexIteratorPtr& indexIter,
                      SegmentTermInfo::TermIndexMode mode);
    //virtual for UT
    virtual IndexIteratorPtr CreateOnDiskNormalIterator(
            const index_base::SegmentMergeInfo& segMergeInfo) const;

    virtual IndexIteratorPtr CreateOnDiskBitmapIterator(
            const index_base::SegmentMergeInfo& segMergeInfo) const;

private:
    typedef std::priority_queue<SegmentTermInfo*, std::vector<SegmentTermInfo*>,
                                SegmentTermInfoComparator>  PriorityQueue;
    PriorityQueue mSegmentTermInfos;
    SegmentTermInfos mMergingSegmentTermInfos;
    config::IndexConfigPtr mIndexConfig;
    OnDiskIndexIteratorCreatorPtr mOnDiskIndexIterCreator;
    SegmentDirectoryBasePtr mSegmentDirectory;

private:
    friend class SegmentTermInfoQueueTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentTermInfoQueue);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SEGMENT_TERM_INFO_QUEUE_H
