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
#include "indexlib/index/operation_log/OperationLogMemIndexer.h"

namespace indexlib::index {
class CompressOperationBlock;

class CompressOperationLogMemIndexer : public OperationLogMemIndexer
{
public:
    CompressOperationLogMemIndexer(segmentid_t segmentid);
    ~CompressOperationLogMemIndexer();

protected:
    Status CreateNewBlock(size_t maxBlockSize) override;
    Status FlushBuildingOperationBlock() override;
    bool NeedCreateNewBlock() const override;
    virtual size_t GetCurrentMemoryUse() const override;

private:
    size_t GetMaxBuildingBufferSize() const;
    std::pair<Status, std::shared_ptr<CompressOperationBlock>> CreateCompressOperationBlock();

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
