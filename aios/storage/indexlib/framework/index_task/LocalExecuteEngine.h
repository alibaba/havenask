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
#include "future_lite/Executor.h"
#include "future_lite/Future.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/index_task/IIndexOperationCreator.h"
#include "indexlib/framework/index_task/IndexOperationDescription.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/framework/index_task/IndexTaskPlan.h"

namespace indexlibv2::framework {

class LocalExecuteEngine : autil::NoCopyable
{
public:
    LocalExecuteEngine(future_lite::Executor* executor, std::unique_ptr<IIndexOperationCreator> operationCreator);

    future_lite::coro::Lazy<Status> Schedule(const IndexOperationDescription& desc, IndexTaskContext* context);
    future_lite::coro::Lazy<Status> ScheduleTask(const IndexTaskPlan& taskPlan, IndexTaskContext* context);

private:
    struct NodeDef {
        IndexOperationDescription desc;
        std::vector<IndexOperationId> fanouts;
        size_t readyDepends = 0;
    };
    using NodeMap = std::unordered_map<IndexOperationId, std::unique_ptr<NodeDef>>;

private:
    NodeMap InitNodeMap(const IndexTaskPlan& taskPlan);

    static std::list<std::vector<LocalExecuteEngine::NodeDef*>>
    ComputeTopoStages(const LocalExecuteEngine::NodeMap& nodeMap);

    static Status FillDependOpFences(const IndexOperationDescription& desc, IndexTaskContext* context);

private:
    future_lite::Executor* _executor;
    std::unique_ptr<IIndexOperationCreator> _operationCreator;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
