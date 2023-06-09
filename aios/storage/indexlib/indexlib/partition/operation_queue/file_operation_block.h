/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __INDEXLIB_FILE_OPERATION_BLOCK_H
#define __INDEXLIB_FILE_OPERATION_BLOCK_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/operation_queue/operation_block.h"
#include "indexlib/partition/operation_queue/operation_meta.h"

DECLARE_REFERENCE_CLASS(file_system, FileReader);

namespace indexlib { namespace partition {

class FileOperationBlock : public OperationBlock
{
public:
    FileOperationBlock() : OperationBlock(0), mReadOffset(0) {}

    ~FileOperationBlock() {}

private:
    FileOperationBlock(const FileOperationBlock& other);

public:
    typedef OperationMeta::BlockMeta BlockMeta;

public:
    void Init(const file_system::FileReaderPtr& fileReader, size_t beginOffset, const BlockMeta& blockMeta);

    OperationBlock* Clone() const override
    {
        assert(false);
        return NULL;
    }

    size_t Size() const override { return mBlockMeta.operationCount; }

    OperationBlockPtr CreateOperationBlockForRead(const OperationFactory& mOpFactory) override;

    void AddOperation(OperationBase* operation) override
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "file operation block not support AddOperation!");
    }

    void Dump(const file_system::FileWriterPtr& fileWriter, size_t maxOpSerializeSize, bool releaseAfterDump) override
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
}} // namespace indexlib::partition

#endif //__INDEXLIB_FILE_OPERATION_BLOCK_H
