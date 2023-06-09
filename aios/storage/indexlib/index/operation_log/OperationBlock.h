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
#include <vector>

#include "autil/NoCopyable.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Status.h"
#include "indexlib/util/MMapVector.h"
namespace indexlib::file_system {
class FileWriter;
} // namespace indexlib::file_system

namespace indexlib::index {
class OperationFactory;
class OperationBase;

class OperationBlock : public autil::NoCopyable
{
public:
    OperationBlock(size_t maxBlockSize);
    OperationBlock(const OperationBlock& other);
    virtual ~OperationBlock();

    typedef util::MMapVector<OperationBase*> OperationVec;

public:
    virtual OperationBlock* Clone() const;
    virtual std::pair<Status, std::shared_ptr<OperationBlock>>
    CreateOperationBlockForRead(const OperationFactory& mOpFactory);
    void AddOperation(OperationBase* operation);
    size_t GetTotalMemoryUse() const;
    virtual size_t Size() const;
    virtual Status Dump(const std::shared_ptr<file_system::FileWriter>& fileWriter, size_t maxOpSerializeSize);
    autil::mem_pool::Pool* GetPool() const;
    int64_t GetMinTimestamp() const;
    int64_t GetMaxTimestamp() const;
    const OperationVec& GetOperations() const;
    void Reset();

    static size_t DumpSingleOperation(OperationBase* op, char* buffer, size_t bufLen);

private:
    void UpdateTimestamp(int64_t ts);

protected:
    std::shared_ptr<OperationVec> _operations;
    int64_t _minTimestamp;
    int64_t _maxTimestamp;
    std::shared_ptr<autil::mem_pool::Pool> _pool;
    std::shared_ptr<util::MMapAllocator> _allocator;
    AUTIL_LOG_DECLARE();
};

typedef std::vector<std::shared_ptr<OperationBlock>> OperationBlockVec;
} // namespace indexlib::index
