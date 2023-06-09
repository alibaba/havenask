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
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/index_task/IndexOperationMetrics.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"

namespace autil {
class LoopThread;
}
namespace indexlibv2::framework {

class IndexOperation : private autil::NoCopyable
{
public:
    IndexOperation(IndexOperationId opId, bool useOpFenceDir) : _useOpFenceDir(useOpFenceDir), _opId(opId) {}
    virtual ~IndexOperation() = default;

public:
    Status ExecuteWithLog(const IndexTaskContext& context);
    IndexOperationId GetOpId() const { return _opId; }

protected:
    virtual Status Execute(const IndexTaskContext& context) = 0;
    virtual std::string GetDebugString() const { return typeid(*this).name(); }

protected:
    bool _useOpFenceDir = true;

private:
    IndexOperationId _opId = INVALID_INDEX_OPERATION_ID;
    std::shared_ptr<IndexOperationMetrics> _operationMetrics;
    std::shared_ptr<autil::LoopThread> _metricsThread;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
