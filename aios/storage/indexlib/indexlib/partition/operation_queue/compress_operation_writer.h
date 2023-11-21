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
#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/operation_queue/compress_operation_block.h"
#include "indexlib/partition/operation_queue/operation_writer.h"

namespace indexlib { namespace partition {

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
}} // namespace indexlib::partition
