#include "indexlib/index/normal/inverted_index/test/segment_term_info_queue_mock.h"

using namespace std;

using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SegmentTermInfoQueueMock);

SegmentTermInfoQueueMock::SegmentTermInfoQueueMock(const config::IndexConfigPtr& indexConf,
                                                   const legacy::OnDiskIndexIteratorCreatorPtr& onDiskIndexIterCreator)
    : SegmentTermInfoQueue(indexConf, onDiskIndexIterCreator)
{
}

SegmentTermInfoQueueMock::~SegmentTermInfoQueueMock() {}

std::shared_ptr<IndexIterator>
SegmentTermInfoQueueMock::CreateOnDiskNormalIterator(const SegmentMergeInfo& segMergeInfo) const
{
    return mNormalIndexIt;
}

std::shared_ptr<IndexIterator>
SegmentTermInfoQueueMock::CreateOnDiskBitmapIterator(const SegmentMergeInfo& segMergeInfo) const
{
    return mBitmapIndexIt;
}
std::shared_ptr<indexlib::index::SingleFieldIndexSegmentPatchIterator>
SegmentTermInfoQueueMock::CreatePatchIterator(const index_base::SegmentMergeInfo& segMergeInfo) const
{
    return std::shared_ptr<indexlib::index::SingleFieldIndexSegmentPatchIterator>();
}

void SegmentTermInfoQueueMock::SetNormalIndexIterator(const vector<index::DictKeyInfo> keys)
{
    mNormalIndexIt.reset(new IndexIteratorMock(keys));
}

void SegmentTermInfoQueueMock::SetBitmapIndexIterator(const vector<index::DictKeyInfo> keys)
{
    mBitmapIndexIt.reset(new IndexIteratorMock(keys));
}
}} // namespace indexlib::index
