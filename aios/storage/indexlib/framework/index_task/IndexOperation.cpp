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
#include "indexlib/framework/index_task/IndexOperation.h"

#include "autil/LoopThread.h"
#include "autil/TimeUtility.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/relocatable/Relocator.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/util/metrics/KmonitorTagvNormalizer.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, IndexOperation);

Status IndexOperation::ExecuteWithLog(const IndexTaskContext& context)
{
    AUTIL_LOG(INFO, "start execute op[%ld][%s]", _opId, GetDebugString().c_str());
    auto manager = context.GetMetricsManager();
    if (manager) {
        _operationMetrics = std::dynamic_pointer_cast<IndexOperationMetrics>(
            manager->CreateMetrics("INDEX_OPERATION_METRICS", [&]() -> std::shared_ptr<framework::IMetrics> {
                return std::make_shared<IndexOperationMetrics>(manager->GetMetricsReporter());
            }));
    }
    autil::ScopedTime2 timer;
    kmonitor::MetricsTags tags;
    tags.AddTag("opId", std::to_string(_opId));
    tags.AddTag("opName", indexlib::util::KmonitorTagvNormalizer::GetInstance()->Normalize(GetDebugString()));

    if (_operationMetrics) {
        _metricsThread = autil::LoopThread::createLoopThread(
            [this, &tags, &timer]() { _operationMetrics->ReportOpExecuteTime(tags, timer.done_us()); },
            60 * 1000 * 1000, /*name=*/"opMetrics");
        _operationMetrics->ReportOpExecuteTime(tags, 0);
    }
    context.Log();
    auto [status, fenceRoot] = context.CreateOpFenceRoot(_opId, _useOpFenceDir);
    RETURN_IF_STATUS_ERROR(status, "create op fence root failed");
    if (fenceRoot) {
        std::string folderWorkRoot = IndexTaskContext::GetRelocatableFolderWorkRoot(fenceRoot->GetOutputPath(), _opId);
        auto [folderStatus, folder] =
            indexlib::file_system::Relocator::CreateFolder(folderWorkRoot, context.GetMetricProvider());
        RETURN_IF_STATUS_ERROR(folderStatus, "create relocatable global folder failed for op[%ld]", _opId);
        context.SetGlobalRelocatableFolder(folder);
    }

    status = Execute(context);

    if (status.IsOK()) {
        auto folder = context.GetGlobalRelocatableFolder();
        if (folder) {
            RETURN_IF_STATUS_ERROR(indexlib::file_system::Relocator::SealFolder(folder), "%s",
                                   "seal relocatable global folder failed");
        }
    }

    if (_operationMetrics) {
        _operationMetrics->ReportOpExecuteTime(tags, timer.done_us());
        _metricsThread->stop();
        _metricsThread.reset();
    }
    AUTIL_LOG(INFO, "end execute op[%ld][%s], time[%.1f]min, status[%s]", _opId, GetDebugString().c_str(),
              timer.done_sec() / 60.0, status.ToString().c_str());
    return status;
}

} // namespace indexlibv2::framework
