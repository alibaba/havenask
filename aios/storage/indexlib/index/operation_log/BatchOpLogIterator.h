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
#include "indexlib/index/operation_log/OperationFactory.h"
#include "indexlib/index/operation_log/OperationMeta.h"

namespace indexlib::file_system {
class IDirectory;
}

namespace indexlib::index {
class OperationBase;
class OperationBlock;
class OperationLogConfig;
class OperationFieldInfo;
class OperationMeta;
class OperationFactory;

class BatchOpLogIterator
{
public:
    BatchOpLogIterator();
    ~BatchOpLogIterator();

public:
    void Init(const std::shared_ptr<OperationLogConfig>& opConfig,
              const std::shared_ptr<OperationFieldInfo>& opFieldInfo, const OperationMeta& opMeta,
              const std::shared_ptr<file_system::IDirectory>& operationDirectory);
    std::pair<Status, std::vector<OperationBase*>> LoadOperations(size_t threadNum);

private:
    void SingleThreadLoadOperations(size_t beginBlockIdx, size_t blockNum, Status* resultStatus,
                                    std::vector<std::shared_ptr<OperationBlock>>* resultOps);

private:
    OperationMeta _opMeta;
    OperationFactory _opFactory;
    std::shared_ptr<OperationFieldInfo> _opFieldInfo;
    std::shared_ptr<file_system::IDirectory> _directory;
    std::vector<std::shared_ptr<OperationBlock>> _blocks;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
