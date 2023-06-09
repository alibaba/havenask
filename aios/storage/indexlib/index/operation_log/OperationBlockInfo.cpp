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
#include "indexlib/index/operation_log/OperationBlockInfo.h"

#include "indexlib/index/operation_log/OperationBlock.h"

namespace indexlib::index {
OperationBlockInfo::OperationBlockInfo() : _noLastBlockOpCount(0), _noLastBlockMemUse(0) {}

OperationBlockInfo::OperationBlockInfo(size_t noLastBlockOpCount, size_t noLastBlockMemUse,
                                       const std::shared_ptr<OperationBlock>& curBlock)
    : _noLastBlockOpCount(noLastBlockOpCount)
    , _noLastBlockMemUse(noLastBlockMemUse)
    , _curBlock(curBlock)
{
}
size_t OperationBlockInfo::GetTotalMemoryUse() const
{
    assert(_curBlock);
    return _noLastBlockMemUse + _curBlock->GetTotalMemoryUse();
}

size_t OperationBlockInfo::Size() const
{
    assert(_curBlock);
    return _noLastBlockOpCount + _curBlock->Size();
}

autil::mem_pool::Pool* OperationBlockInfo::GetPool() const
{
    assert(_curBlock);
    return _curBlock->GetPool();
}

bool OperationBlockInfo::operator==(const OperationBlockInfo& other) const
{
    return _noLastBlockOpCount == other._noLastBlockOpCount && _noLastBlockMemUse == other._noLastBlockMemUse &&
           _curBlock.get() == other._curBlock.get();
}

} // namespace indexlib::index
