#include "indexlib/index/normal/inverted_index/test/segment_term_info_queue_mock.h"

using namespace std;

IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SegmentTermInfoQueueMock);

SegmentTermInfoQueueMock::SegmentTermInfoQueueMock(
        const config::IndexConfigPtr& indexConf,
        const OnDiskIndexIteratorCreatorPtr &onDiskIndexIterCreator)
    : SegmentTermInfoQueue(indexConf, onDiskIndexIterCreator)
{
}

SegmentTermInfoQueueMock::~SegmentTermInfoQueueMock() 
{
}

IndexIteratorPtr SegmentTermInfoQueueMock::CreateOnDiskNormalIterator(
        const SegmentMergeInfo& segMergeInfo) const
{
    return mNormalIndexIt;
}

IndexIteratorPtr SegmentTermInfoQueueMock::CreateOnDiskBitmapIterator(
        const SegmentMergeInfo& segMergeInfo) const
{
    return mBitmapIndexIt;
}

void SegmentTermInfoQueueMock::SetNormalIndexIterator(
        const vector<dictkey_t> keys)
{
    mNormalIndexIt.reset(new IndexIteratorMock(keys));
}

void SegmentTermInfoQueueMock::SetBitmapIndexIterator(
        const vector<dictkey_t> keys)
{
    mBitmapIndexIt.reset(new IndexIteratorMock(keys));
}

IE_NAMESPACE_END(index);

