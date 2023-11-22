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

#include "build_service/admin/AdminTaskBase.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"

namespace build_service { namespace admin {

class TaskOptimizer
{
public:
    TaskOptimizer(const TaskResourceManagerPtr& resMgr);
    virtual ~TaskOptimizer();
    enum OptimizeStep { COLLECT = 0, OPTIMIZE = 1 };

private:
    TaskOptimizer(const TaskOptimizer&);
    TaskOptimizer& operator=(const TaskOptimizer&);

public:
    virtual void collect(const AdminTaskBasePtr& taskImpl) = 0;
    virtual AdminTaskBasePtr optimize(const AdminTaskBasePtr& taskImpl) = 0;

protected:
    TaskResourceManagerPtr _resMgr;
    OptimizeStep _step;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskOptimizer);

}} // namespace build_service::admin
