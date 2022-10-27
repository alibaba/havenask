#include "indexlib/index/normal/deletionmap/deletion_map_dump_item.h"
#include "indexlib/index/normal/deletionmap/deletion_map_segment_writer.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DeletionMapDumpItem);

DeletionMapDumpItem::DeletionMapDumpItem(
        const file_system::DirectoryPtr& directory,
        const DeletionMapSegmentWriterPtr& writer,
        segmentid_t segId)
    : mDirectory(directory)
    , mWriter(writer)
    , mSegmentId(segId)
{
}

DeletionMapDumpItem::~DeletionMapDumpItem() 
{
}

void DeletionMapDumpItem::process(autil::mem_pool::PoolBase* pool)
{
    mWriter->Dump(mDirectory, mSegmentId);
}

void DeletionMapDumpItem::destroy()
{
    delete this;
}

void DeletionMapDumpItem::drop()
{
    destroy();
}

IE_NAMESPACE_END(index);

