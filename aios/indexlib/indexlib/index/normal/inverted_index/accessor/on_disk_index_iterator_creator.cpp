#include "indexlib/index/normal/inverted_index/accessor/on_disk_index_iterator_creator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/on_disk_bitmap_index_iterator.h"

using namespace std;
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);

OnDiskIndexIteratorCreator::OnDiskIndexIteratorCreator()
{}
 

OnDiskIndexIteratorCreator::~OnDiskIndexIteratorCreator()
{}

OnDiskIndexIterator* OnDiskIndexIteratorCreator::CreateBitmapIterator(
        const DirectoryPtr& indexDirectory) const
{
    return new OnDiskBitmapIndexIterator(indexDirectory);
}

IE_NAMESPACE_END(index);
