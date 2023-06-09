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
#include "indexlib/base/Status.h"
#include "indexlib/index/operation_log/OperationBlock.h"
#include "indexlib/index/operation_log/OperationMeta.h"
namespace indexlib::file_system {
class Directory;
}
namespace indexlib::index {
class OperationBase;

class OperationDumper
{
public:
    OperationDumper(const OperationMeta& opMeta) : _operationMeta(opMeta) {}
    ~OperationDumper() {}

public:
    Status Init(const OperationBlockVec& opBlocks);
    Status Dump(const std::shared_ptr<file_system::Directory>& directory, autil::mem_pool::PoolBase* dumpPool);
    static size_t GetDumpSize(OperationBase* op);

private:
    Status CheckOperationBlocks() const;

private:
    OperationMeta _operationMeta;
    OperationBlockVec _opBlockVec;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
