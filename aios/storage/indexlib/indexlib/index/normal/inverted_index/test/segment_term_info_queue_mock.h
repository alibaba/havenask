#ifndef __INDEXLIB_SEGMENT_TERM_INFO_QUEUE_MOCK_H
#define __INDEXLIB_SEGMENT_TERM_INFO_QUEUE_MOCK_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/test/IndexIteratorMock.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_term_info_queue.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class SegmentTermInfoQueueMock : public legacy::SegmentTermInfoQueue
{
public:
    SegmentTermInfoQueueMock(const config::IndexConfigPtr& indexConf,
                             const legacy::OnDiskIndexIteratorCreatorPtr& onDiskIndexIterCreator);
    ~SegmentTermInfoQueueMock();

public:
    void ClearNormalIndexIterator() { mNormalIndexIt.reset(); }
    void ClearBitmapIndexIterator() { mBitmapIndexIt.reset(); }
    void SetNormalIndexIterator(const std::vector<index::DictKeyInfo> keys);
    void SetBitmapIndexIterator(const std::vector<index::DictKeyInfo> keys);

private:
    std::shared_ptr<IndexIterator>
    CreateOnDiskNormalIterator(const index_base::SegmentMergeInfo& segMergeInfo) const override;

    std::shared_ptr<IndexIterator>
    CreateOnDiskBitmapIterator(const index_base::SegmentMergeInfo& segMergeInfo) const override;

    std::shared_ptr<indexlib::index::SingleFieldIndexSegmentPatchIterator>
    CreatePatchIterator(const index_base::SegmentMergeInfo& segMergeInfo) const override;

private:
    // for CreateIterator use
    std::shared_ptr<IndexIteratorMock> mNormalIndexIt;
    std::shared_ptr<IndexIteratorMock> mBitmapIndexIt;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentTermInfoQueueMock);
}} // namespace indexlib::index

#endif //__INDEXLIB_SEGMENT_TERM_INFO_QUEUE_MOCK_H
