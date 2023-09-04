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
#ifndef ISEARCH_BS_CHECKPOINTCREATOR_H
#define ISEARCH_BS_CHECKPOINTCREATOR_H

#include "build_service/common/BuilderCheckpointAccessor.h"
#include "build_service/common/IndexCheckpointAccessor.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class TaskResourceManager;
BS_TYPEDEF_PTR(TaskResourceManager);

class CheckpointCreator
{
public:
    CheckpointCreator();
    ~CheckpointCreator();

private:
    CheckpointCreator(const CheckpointCreator&);
    CheckpointCreator& operator=(const CheckpointCreator&);

public:
    static common::IndexCheckpointAccessorPtr createIndexCheckpointAccessor(const TaskResourceManagerPtr& resMgr);

    static common::BuilderCheckpointAccessorPtr createBuilderCheckpointAccessor(const TaskResourceManagerPtr& resMgr);

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CheckpointCreator);

}} // namespace build_service::admin

#endif // ISEARCH_BS_CHECKPOINTCREATOR_H
