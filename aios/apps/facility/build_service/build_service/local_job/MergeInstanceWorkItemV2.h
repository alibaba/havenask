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
#include "autil/NoCopyable.h"
#include "autil/WorkItem.h"
#include "build_service/task_base/TaskBase.h"
#include "build_service/util/Log.h"
#include "future_lite/TaskScheduler.h"
#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/ITabletMergeController.h"
#include "indexlib/framework/IdGenerator.h"
#include "indexlib/framework/Version.h"

namespace build_service::local_job {

class MergeInstanceWorkItemV2 : public autil::WorkItem, public task_base::TaskBase, private autil::NoCopyable
{
public:
    MergeInstanceWorkItemV2(volatile bool* failFlag, task_base::TaskBase::Mode mode, uint16_t instanceId,
                            const std::string& jobParams)
        : _failFlag(failFlag)
        , _mode(mode)
        , _instanceId(instanceId)
        , _jobParams(jobParams)
    {
    }
    ~MergeInstanceWorkItemV2() = default;

public:
    void process() override;

private:
    indexlib::Status getLatestVersion(indexlibv2::framework::Version* version) const;
    void setFailFlag();
    std::unique_ptr<future_lite::Executor> createExecutor(const std::string& executorName, uint32_t threadCount) const;
    int64_t getMachineTotalMemoryMb() const;
    bool createQuotaController(int64_t buildTotalMemory);
    bool prepareResource();
    std::shared_ptr<indexlibv2::framework::ITablet> prepareTablet();
    std::shared_ptr<indexlibv2::framework::ITabletMergeController> createMergeController();
    std::shared_ptr<indexlibv2::framework::IdGenerator> getIdGenerator() const;

    std::shared_ptr<indexlibv2::config::ITabletSchema>
    getTabletSchema(const std::shared_ptr<indexlibv2::config::TabletOptions>& tabletOptions) const;

private:
    volatile bool* _failFlag;
    const task_base::TaskBase::Mode _mode;
    const uint16_t _instanceId;
    const std::string _jobParams;

    std::string _buildMode;
    std::string _indexRoot;
    std::string _finalIndexRoot;
    std::unique_ptr<future_lite::Executor> _executor;
    std::unique_ptr<future_lite::Executor> _dumpExecutor;
    std::unique_ptr<future_lite::Executor> _localMergeExecutor;
    std::unique_ptr<future_lite::TaskScheduler> _taskScheduler;
    std::shared_ptr<indexlibv2::MemoryQuotaController> _totalMemoryController;
    std::shared_ptr<indexlibv2::MemoryQuotaController> _buildMemoryController;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _tabletSchema;
    std::shared_ptr<indexlibv2::config::TabletOptions> _tabletOptions;

private:
    BS_LOG_DECLARE();
};

} // namespace build_service::local_job
