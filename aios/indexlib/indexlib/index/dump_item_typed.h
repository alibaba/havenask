#ifndef __INDEXLIB_DUMP_ITEM_H
#define __INDEXLIB_DUMP_ITEM_H

#include <tr1/memory>
#include <autil/WorkItem.h>
#include <autil/mem_pool/PoolBase.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/dump_item.h"
#include "indexlib/file_system/directory.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index, IndexWriter);
DECLARE_REFERENCE_CLASS(index, AttributeWriter);
DECLARE_REFERENCE_CLASS(index, SummaryWriter);
DECLARE_REFERENCE_CLASS(index, SectionAttributeWriter);
DECLARE_REFERENCE_CLASS(index, AttributeUpdater);

IE_NAMESPACE_BEGIN(index);

template <typename WriterPointerType>
class DumpItemTyped : public common::DumpItem
{
public:
    DumpItemTyped(const file_system::DirectoryPtr& directory,
             const WriterPointerType& writer);
    ~DumpItemTyped();

public:
    void process(autil::mem_pool::PoolBase* pool) override;
    void destroy() override;
    void drop() override;

private:
    file_system::DirectoryPtr mDirectory;
    WriterPointerType mWriter;

private:
    IE_LOG_DECLARE();
};


template <typename WriterPointerType>
DumpItemTyped<WriterPointerType>::DumpItemTyped(
        const file_system::DirectoryPtr& directory,
        const WriterPointerType& writer)
    : mDirectory(directory)
    , mWriter(writer)
{
}

template <typename WriterPointerType>
DumpItemTyped<WriterPointerType>::~DumpItemTyped()
{
}

template <typename WriterPointerType>
void DumpItemTyped<WriterPointerType>::process(autil::mem_pool::PoolBase* pool)
{
    mWriter->Dump(mDirectory, pool);
}

template <typename WriterPointerType>
void DumpItemTyped<WriterPointerType>::destroy()
{
    delete this;
}

template <typename WriterPointerType>
void DumpItemTyped<WriterPointerType>::drop()
{
    destroy();
}

typedef DumpItemTyped<IndexWriterPtr> IndexDumpItem;
typedef DumpItemTyped<AttributeWriterPtr> AttributeDumpItem;
typedef DumpItemTyped<SummaryWriterPtr> SummaryDumpItem;
typedef DumpItemTyped<SectionAttributeWriterPtr> SectionAttributeDumpItem;

//////////////////////////////////////////////////////////////////////
class AttributeUpdaterDumpItem : public common::DumpItem
{
public:
    AttributeUpdaterDumpItem(const file_system::DirectoryPtr& directory,
            const AttributeUpdaterPtr& updater, segmentid_t srcSegmentId)
        : mDirectory(directory)
        , mUpdater(updater)
        , mSrcSegId(srcSegmentId)
    {}

    ~AttributeUpdaterDumpItem() {}

public:
    void process(autil::mem_pool::PoolBase* pool) override;

    void destroy() override
    {
        delete this;
    }

    void drop() override
    {
        destroy();
    }

private:
    file_system::DirectoryPtr mDirectory;
    AttributeUpdaterPtr mUpdater;
    segmentid_t mSrcSegId;
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DUMP_ITEM_H
