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
#include "indexlib/index/operation_log/SegmentOperationIterator.h"

namespace indexlib::index {

class InMemSegmentOperationIterator : public SegmentOperationIterator
{
public:
    InMemSegmentOperationIterator(const std::shared_ptr<OperationLogConfig>& opConfig,
                                  const std::shared_ptr<OperationFieldInfo>& operationFieldInfo,
                                  const OperationMeta& operationMeta, segmentid_t segmentId, size_t offset,
                                  const indexlibv2::framework::Locator& locator);
    ~InMemSegmentOperationIterator();

public:
    Status Init(const OperationBlockVec& opBlocks);
    std::pair<Status, OperationBase*> Next() override;

private:
    void ToNextReadPosition();
    void SeekNextValidOpBlock();
    Status SeekNextValidOperation();
    Status SwitchToReadBlock(int32_t blockIdx);

protected:
    OperationBlockVec _opBlocks;
    int32_t _opBlockIdx;    // point to operation block to be read by next
    int32_t _inBlockOffset; // point to in block position to be read by next
    std::shared_ptr<OperationBlock> _curBlockForRead;
    std::shared_ptr<OperationFieldInfo> _operationFieldInfo;
    int32_t _curReadBlockIdx;

    // operation is created by Pool in OpBlock.
    // Next() may trigger switch operation block.
    // In this case, clone the operation to ensure life cycle
    OperationBase* _reservedOperation;
    bool _reservedOpFromPool = false;
    std::vector<OperationBase*> _reservedOpVec;
    autil::mem_pool::Pool _pool;
    size_t _resetPoolThreshold;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
