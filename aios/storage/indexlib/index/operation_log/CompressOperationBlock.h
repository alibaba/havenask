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

#include "autil/Log.h"
#include "indexlib/index/operation_log/OperationBlock.h"

namespace indexlib::index {

class CompressOperationBlock : public OperationBlock
{
public:
    CompressOperationBlock();
    CompressOperationBlock(const CompressOperationBlock& other);
    ~CompressOperationBlock();

    void Init(size_t operationCount, const char* compressBuffer, size_t compressSize, int64_t opMinTimeStamp,
              int64_t opMaxTimeStamp);

    OperationBlock* Clone() const override;

    size_t Size() const override;

    std::pair<Status, std::shared_ptr<OperationBlock>>
    CreateOperationBlockForRead(const OperationFactory& mOpFactory) override;

    Status Dump(const std::shared_ptr<file_system::FileWriter>& fileWriter, size_t maxOpSerializeSize) override;

    size_t GetCompressSize() const;

private:
    size_t _operationCount;
    const char* _compressDataBuffer;
    size_t _compressDataLen;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
