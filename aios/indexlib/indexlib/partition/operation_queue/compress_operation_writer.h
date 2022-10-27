#ifndef __INDEXLIB_COMPRESS_OPERATION_WRITER_H
#define __INDEXLIB_COMPRESS_OPERATION_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/operation_queue/operation_writer.h"
#include "indexlib/partition/operation_queue/compress_operation_block.h"

IE_NAMESPACE_BEGIN(partition);

class CompressOperationWriter : public OperationWriter
{
public:
    CompressOperationWriter(size_t buildTotalMemUse);
    // for test only
    CompressOperationWriter();
    ~CompressOperationWriter();

public:
    static size_t GetCompressBufferSize(size_t totalMemSize);
    
protected:
    void CreateNewBlock(size_t maxBlockSize) override;
    void FlushBuildingOperationBlock() override;
    bool NeedCreateNewBlock() const override;
    
private:
    size_t GetMaxBuildingBufferSize() const override
    {
        // compressor in-buffer + compressor out-buffer + compressed-block-data
        return 3 * mMaxOpBlockSerializeSize;
    }    
    
private:
    CompressOperationBlockPtr CreateCompressOperationBlock();

private:
    size_t mMaxOpBlockSerializeSize;

private:
    friend class CompressOperationWriterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CompressOperationWriter);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_COMPRESS_OPERATION_WRITER_H
