#ifndef __INDEXLIB_OPERATION_WRITER_H
#define __INDEXLIB_OPERATION_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/segment_container.h"
#include "indexlib/partition/operation_queue/operation_factory.h"
#include "indexlib/partition/operation_queue/operation_base.h"
#include "indexlib/partition/operation_queue/in_mem_segment_operation_iterator.h"
#include "indexlib/partition/operation_queue/operation_meta.h"
#include <autil/WorkItem.h>

DECLARE_REFERENCE_CLASS(partition, OperationDumper);
DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetricsNode);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

IE_NAMESPACE_BEGIN(index);
template <typename T>
class DumpItemTyped;
IE_NAMESPACE_END(index);

IE_NAMESPACE_BEGIN(partition);

typedef index::DumpItemTyped<OperationDumperPtr> OperationDumpItem;

class OperationWriter : public index_base::SegmentContainer
{
public:
    OperationWriter();
    
    virtual ~OperationWriter() {}

public:
    virtual void Init(const config::IndexPartitionSchemaPtr& schema,
              size_t maxBlockSize = DEFAULT_MAX_OPERATION_BLOCK_SIZE);

    void SetReleaseBlockAfterDump(bool isBlockRelease)
    {
        mReleaseBlockAfterDump = isBlockRelease;
    }

    bool AddOperation(const document::DocumentPtr& doc);
    void CreateDumpItems(const file_system::DirectoryPtr& directory,
                         std::vector<common::DumpItem*>& dumpItems) override;
    
    size_t Size() const { return mBlockInfo.Size(); }

    size_t GetDumpSize() const
    { 
        return mOperationMeta.GetTotalDumpSize() 
            + mOperationMeta.GetLastBlockSerializeSize(); 
    }
    
    OperationMeta GetOperationMeta() const
    {
        autil::ScopedLock lock(mDataLock);
        OperationMeta opMeta = mOperationMeta;
        assert(!mOpBlocks.empty());
        opMeta.EndOneBlock(mBlockInfo.mCurBlock, mOpBlocks.size() - 1);
        return opMeta; 
    }

    size_t GetMaxOperationSerializeSize() const
    { return mOperationMeta.GetMaxOperationSerializeSize(); }
    
    size_t GetTotalOperationSerializeSize() const
    { return mOperationMeta.GetTotalSerializeSize(); }

    size_t GetTotalOperationCount() const
    { return mOperationMeta.GetOperationCount(); }

    size_t GetTotalMemoryUse() const override
    { return mBlockInfo.GetTotalMemoryUse() + GetMaxBuildingBufferSize();}

    bool IsDirty() const
    { return Size() > 0; }

    InMemSegmentOperationIteratorPtr CreateSegmentOperationIterator(
        segmentid_t segId, size_t offset, int64_t timestamp);

    const util::BuildResourceMetricsPtr& GetBuildResourceMetrics() const;

public:
    // for test only
    void Dump(const file_system::DirectoryPtr& directory);

private:
    virtual size_t GetMaxBuildingBufferSize() const { return 0;}

protected:
    void UpdateBuildResourceMetrics();
    virtual void FlushBuildingOperationBlock();
    bool DoAddOperation(OperationBase *operation);
    void ResetBlockInfo(const OperationBlockVec& opBlocks);
    virtual void CreateNewBlock(size_t maxBlockSize);

    // virtual for test
    virtual OperationFactoryPtr CreateOperationFactory(
        const config::IndexPartitionSchemaPtr& schema);

    virtual bool NeedCreateNewBlock() const
    { return mBlockInfo.mCurBlock->Size() >= mMaxBlockSize; }

protected:
    config::IndexPartitionSchemaPtr mSchema;
    size_t mMaxBlockSize;
    volatile bool mReleaseBlockAfterDump;
    OperationFactoryPtr mOperationFactory;
    OperationBlockVec mOpBlocks;
    OperationBlockInfo mBlockInfo;
    OperationMeta mOperationMeta;
    mutable autil::RecursiveThreadMutex mDataLock;
    util::BuildResourceMetricsPtr mBuildResourceMetrics;
    util::BuildResourceMetricsNode* mBuildResourceMetricsNode;
    
private:
    friend class OperationWriterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OperationWriter);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_OPERATION_WRITER_H
