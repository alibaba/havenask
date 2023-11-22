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
#include "build_service/admin/taskcontroller/TaskOptimizerFactory.h"

#include <iosfwd>
#include <memory>
#include <utility>

#include "build_service/admin/taskcontroller/BuildServiceTask.h"
#include "build_service/admin/taskcontroller/ProcessorTaskOptimizer.h"

using namespace std;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, TaskOptimizerFactory);

TaskOptimizerFactory::TaskOptimizerFactory(const TaskResourceManagerPtr& resMgr) : _resMgr(resMgr) {}

TaskOptimizerFactory::~TaskOptimizerFactory() {}

TaskOptimizerPtr TaskOptimizerFactory::getOptimizer(const std::string& taskType)
{
    if (taskType == BuildServiceTask::PROCESSOR) {
        auto iter = _optimizerMap.find(taskType);
        if (iter != _optimizerMap.end()) {
            return iter->second;
        } else {
            TaskOptimizerPtr optimizer(new ProcessorTaskOptimizer(_resMgr));
            _optimizerMap.insert(make_pair(taskType, optimizer));
            return optimizer;
        }
    }
    return TaskOptimizerPtr();
}
}} // namespace build_service::admin
