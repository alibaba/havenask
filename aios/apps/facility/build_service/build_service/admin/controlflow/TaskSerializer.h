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
#ifndef ISEARCH_BS_TASKSERIALIZER_H
#define ISEARCH_BS_TASKSERIALIZER_H

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

BS_DECLARE_REFERENCE_CLASS(admin, TaskBase);
BS_DECLARE_REFERENCE_CLASS(admin, TaskFactory);
BS_DECLARE_REFERENCE_CLASS(admin, TaskResourceManager);

namespace build_service { namespace admin {

class TaskSerializer
{
public:
    TaskSerializer(const TaskFactoryPtr& factory, const TaskResourceManagerPtr& taskResMgr);
    ~TaskSerializer();

private:
    TaskSerializer(const TaskSerializer&);
    TaskSerializer& operator=(const TaskSerializer&);

public:
    static void serialize(autil::legacy::Jsonizable::JsonWrapper& json, std::vector<TaskBasePtr>& taskFlows);

    bool deserialize(autil::legacy::Jsonizable::JsonWrapper& json, std::vector<TaskBasePtr>& taskFlows);

private:
    TaskFactoryPtr _factory;
    TaskResourceManagerPtr _resMgr;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskSerializer);

}} // namespace build_service::admin

#endif // ISEARCH_BS_TASKSERIALIZER_H
