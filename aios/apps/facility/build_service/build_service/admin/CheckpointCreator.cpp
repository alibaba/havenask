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
#include "build_service/admin/CheckpointCreator.h"

#include <iosfwd>
#include <memory>

#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/common/BuilderCheckpointAccessor.h"

using namespace std;

using build_service::common::BuilderCheckpointAccessor;
using build_service::common::BuilderCheckpointAccessorPtr;
using build_service::common::CheckpointAccessorPtr;
using build_service::common::IndexCheckpointAccessor;
using build_service::common::IndexCheckpointAccessorPtr;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, CheckpointCreator);

CheckpointCreator::CheckpointCreator() {}

CheckpointCreator::~CheckpointCreator() {}

IndexCheckpointAccessorPtr CheckpointCreator::createIndexCheckpointAccessor(const TaskResourceManagerPtr& resMgr)
{
    if (!resMgr) {
        return IndexCheckpointAccessorPtr();
    }

    CheckpointAccessorPtr checkpointAccessor;
    resMgr->getResource(checkpointAccessor);
    if (!checkpointAccessor) {
        return IndexCheckpointAccessorPtr();
    }
    return IndexCheckpointAccessorPtr(new IndexCheckpointAccessor(checkpointAccessor));
}

BuilderCheckpointAccessorPtr CheckpointCreator::createBuilderCheckpointAccessor(const TaskResourceManagerPtr& resMgr)
{
    if (!resMgr) {
        return BuilderCheckpointAccessorPtr();
    }

    CheckpointAccessorPtr checkpointAccessor;
    resMgr->getResource(checkpointAccessor);
    if (!checkpointAccessor) {
        return BuilderCheckpointAccessorPtr();
    }
    return BuilderCheckpointAccessorPtr(new BuilderCheckpointAccessor(checkpointAccessor));
}

}} // namespace build_service::admin
