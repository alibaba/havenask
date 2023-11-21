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

#include "build_service/common_define.h"
#include "build_service/config/TaskInputConfig.h"
#include "build_service/config/TaskOutputConfig.h"
#include "build_service/plugin/ModuleFactory.h"
#include "build_service/task_base/Task.h"
#include "build_service/util/Log.h"

namespace build_service { namespace task_base {

class TaskFactory : public plugin::ModuleFactory
{
public:
    TaskFactory() {}
    virtual ~TaskFactory() {}

private:
    TaskFactory(const TaskFactory&);
    TaskFactory& operator=(const TaskFactory&);

public:
    virtual TaskPtr createTask(const std::string& taskName) = 0;
    virtual io::InputCreatorPtr getInputCreator(const config::TaskInputConfig& inputConfig) = 0;
    virtual io::OutputCreatorPtr getOutputCreator(const config::TaskOutputConfig& outputConfig) = 0;

    void destroy() override { delete this; }

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskFactory);

}} // namespace build_service::task_base
