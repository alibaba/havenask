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

#include "autil/NoCopyable.h"
#include "autil/mem_pool/Pool.h"

namespace indexlib::index {
class OperationBlock;
class OperationBlockInfo
{
public:
    OperationBlockInfo();
    OperationBlockInfo(size_t noLastBlockOpCount, size_t noLastBlockMemUse,
                       const std::shared_ptr<OperationBlock>& curBlock);

    size_t GetTotalMemoryUse() const;
    size_t Size() const;
    autil::mem_pool::Pool* GetPool() const;
    bool operator==(const OperationBlockInfo& other) const;

public:
    size_t _noLastBlockOpCount;
    size_t _noLastBlockMemUse;
    std::shared_ptr<OperationBlock> _curBlock;
};

} // namespace indexlib::index
