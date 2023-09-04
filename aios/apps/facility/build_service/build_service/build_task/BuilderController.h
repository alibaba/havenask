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

#include "autil/LoopThread.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/BuildTaskTargetInfo.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/task_base/Task.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/Version.h"

namespace indexlibv2::framework {
class ITablet;
}

namespace build_service::build_task {

class BuilderController : public proto::ErrorCollector
{
public:
    BuilderController(const task_base::Task::TaskInitParam& taskInitParam,
                      const std::shared_ptr<indexlibv2::framework::ITablet>& tablet)
        : _initParam(taskInitParam)
        , _tablet(tablet)
    {
    }
    ~BuilderController() = default;

public:
    bool handleTarget(const config::TaskTarget& target);
    bool isDone() const;
    bool hasFatalError() const;

private:
    indexlib::Status loadVersion(const proto::VersionProgress& vp, bool isFullBuild,
                                 indexlibv2::framework::Version& version) const;

private:
    const task_base::Task::TaskInitParam _initParam;
    std::shared_ptr<indexlibv2::framework::ITablet> _tablet;
    std::atomic<bool> _isDone = false;
    std::atomic<bool> _hasFatalError = false;

private:
    BS_LOG_DECLARE();
};

} // namespace build_service::build_task
