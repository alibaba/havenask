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

#include "indexlib/base/Status.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/index/operation_log/OperationBase.h"

namespace indexlibv2::document {
class NormalDocument;
}

namespace autil::mem_pool {
class Pool;
}

namespace indexlibv2::index {
class OperationLogConfig;
}

namespace indexlib::index {
class OperationCreator;

class OperationFactory
{
public:
    OperationFactory();
    virtual ~OperationFactory();

public:
    void Init(const std::shared_ptr<OperationLogConfig>& config);
    virtual bool CreateOperation(const indexlibv2::document::NormalDocument* doc, autil::mem_pool::Pool* pool,
                                 OperationBase** operation);

    std::pair<Status, OperationBase*> DeserializeOperation(const char* buffer, autil::mem_pool::Pool* pool,
                                                           size_t& opSize) const;

private:
    std::shared_ptr<OperationCreator>
    CreateRemoveOperationCreator(const std::shared_ptr<OperationLogConfig>& schemconfig);
    std::shared_ptr<OperationCreator>
    CreateUpdateOperationCreator(const std::shared_ptr<OperationLogConfig>& schemconfig);
    OperationBase* CreateUnInitializedOperation(OperationBase::SerializedOperationType opType, int64_t timestamp,
                                                uint16_t hashId, autil::mem_pool::Pool* pool) const;

private:
    InvertedIndexType _mainPkType;
    std::shared_ptr<OperationCreator> _removeOperationCreator;
    std::shared_ptr<OperationCreator> _updateOperationCreator;

private:
    AUTIL_LOG_DECLARE();
    friend class OperationFactoryTest;
};

} // namespace indexlib::index
