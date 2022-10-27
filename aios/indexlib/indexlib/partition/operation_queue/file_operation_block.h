#ifndef __INDEXLIB_FILE_OPERATION_BLOCK_H
#define __INDEXLIB_FILE_OPERATION_BLOCK_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/operation_queue/operation_block.h"
#include "indexlib/partition/operation_queue/operation_meta.h"

DECLARE_REFERENCE_CLASS(file_system, FileReader);

IE_NAMESPACE_BEGIN(partition);

class FileOperationBlock : public OperationBlock
{
public:
    FileOperationBlock()
        : OperationBlock(0)
        , mReadOffset(0)
    {}
    
    ~FileOperationBlock() 
    {}

private:
    FileOperationBlock(const FileOperationBlock& other);

public:
    typedef OperationMeta::BlockMeta BlockMeta;
public:
    void Init(const file_system::FileReaderPtr& fileReader, 
              size_t beginOffset,const BlockMeta& blockMeta);

    OperationBlock* Clone() const override
    {
        assert(false);
        return NULL;
    }

    size_t Size() const override
    {
        return mBlockMeta.operationCount;
    }

    OperationBlockPtr CreateOperationBlockForRead(
            const OperationFactory& mOpFactory) override;

    void AddOperation(OperationBase *operation) override
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "file operation block not support AddOperation!");
    }

    void Dump(const file_system::FileWriterPtr& fileWriter, 
                             size_t maxOpSerializeSize, bool releaseAfterDump) override
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "file operation block not support Dump!");
    }

private:
    const char* CreateOperationBuffer(autil::mem_pool::Pool* pool);
private:
    file_system::FileReaderPtr mFileReader;
    size_t mReadOffset;
    BlockMeta mBlockMeta;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FileOperationBlock);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_FILE_OPERATION_BLOCK_H
