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
#include "indexlib/table/normal_table/index_task/PrepareIndexReclaimParamOperation.h"

#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/util/TaskItem.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, PrepareIndexReclaimParamOperation);

PrepareIndexReclaimParamOperation::PrepareIndexReclaimParamOperation(const framework::IndexOperationDescription& desc)
    : framework::IndexOperation(desc.GetId(), desc.UseOpFenceDir())
    , _desc(desc)
{
}

PrepareIndexReclaimParamOperation::~PrepareIndexReclaimParamOperation() {}

void PrepareIndexReclaimParamOperation::GetOpInfo(const framework::IndexOperationDescription::Parameters& params,
                                                  framework::IndexOperationId& opId, bool& useOpFenceDir)
{
    auto iter = params.find(OP_ID);
    if (iter == params.end()) {
        useOpFenceDir = false;
        return;
    }
    autil::StringUtil::fromString(iter->second, opId);
    auto useOpFenceIter = params.find(framework::IndexOperationDescription::USE_OP_FENCE_DIR);
    if (useOpFenceIter == params.end()) {
        useOpFenceDir = false;
        return;
    }
    assert(useOpFenceIter->second == "1");
    useOpFenceDir = true;
}

Status PrepareIndexReclaimParamOperation::Execute(const framework::IndexTaskContext& context)
{
    AUTIL_LOG(INFO, "begin exec prepare index reclaim op");
    auto resourceManager = context.GetResourceManager();
    assert(resourceManager);

    using PrepareTask = indexlib::util::TaskItemWithStatus<const indexlibv2::framework::IndexTaskContext*,
                                                           const std::map<std::string, std::string>&>;
    std::shared_ptr<PrepareTask> prepareTask;
    Status status = resourceManager->GetExtendResource(framework::INDEX_RECLAIM_PARAM_PREPARER, prepareTask);
    RETURN_STATUS_DIRECTLY_IF_ERROR(status);
    auto params = _desc.GetAllParameters();
    params[OP_ID] = std::to_string(_desc.GetId());
    status = prepareTask->Run(&context, params);
    AUTIL_LOG(INFO, "end exec prepare index reclaim op");
    return status;
}

} // namespace indexlibv2::table
