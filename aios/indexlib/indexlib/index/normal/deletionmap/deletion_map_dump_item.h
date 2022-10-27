#ifndef __INDEXLIB_DELETION_MAP_DUMP_ITEM_H
#define __INDEXLIB_DELETION_MAP_DUMP_ITEM_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/dump_item_typed.h"

DECLARE_REFERENCE_CLASS(index, DeletionMapSegmentWriter);
DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(index);

class DeletionMapDumpItem  : public common::DumpItem
{
public:
    DeletionMapDumpItem(const file_system::DirectoryPtr& directory,
                        const DeletionMapSegmentWriterPtr& writer,
                        segmentid_t segId);

    ~DeletionMapDumpItem();

public:
    void process(autil::mem_pool::PoolBase* pool) override;
    void destroy() override;
    void drop() override;

private:
    file_system::DirectoryPtr mDirectory;
    DeletionMapSegmentWriterPtr mWriter;
    segmentid_t mSegmentId;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DeletionMapDumpItem);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DELETION_MAP_DUMP_ITEM_H
