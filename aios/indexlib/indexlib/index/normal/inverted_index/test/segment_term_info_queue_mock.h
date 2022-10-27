#ifndef __INDEXLIB_SEGMENT_TERM_INFO_QUEUE_MOCK_H
#define __INDEXLIB_SEGMENT_TERM_INFO_QUEUE_MOCK_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_term_info_queue.h"
#include "indexlib/index/normal/inverted_index/test/index_iterator_mock.h"

IE_NAMESPACE_BEGIN(index);

class SegmentTermInfoQueueMock : public SegmentTermInfoQueue
{
public:
    SegmentTermInfoQueueMock(
            const config::IndexConfigPtr& indexConf,
            const OnDiskIndexIteratorCreatorPtr &onDiskIndexIterCreator);
    ~SegmentTermInfoQueueMock();
public:
    void ClearNormalIndexIterator() { mNormalIndexIt.reset(); }
    void ClearBitmapIndexIterator() { mBitmapIndexIt.reset(); }
    void SetNormalIndexIterator(const std::vector<dictkey_t> keys);
    void SetBitmapIndexIterator(const std::vector<dictkey_t> keys);

private:
    IndexIteratorPtr CreateOnDiskNormalIterator(
            const index_base::SegmentMergeInfo& segMergeInfo) const override;
    
    virtual IndexIteratorPtr CreateOnDiskBitmapIterator(
            const index_base::SegmentMergeInfo& segMergeInfo) const override;
    
private:
    //for CreateIterator use
    IndexIteratorMockPtr mNormalIndexIt;
    IndexIteratorMockPtr mBitmapIndexIt;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentTermInfoQueueMock);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SEGMENT_TERM_INFO_QUEUE_MOCK_H
