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
#ifndef ISEARCH_BS_LEGACYCHECKPOINTLISTSYNCRONIZER_H
#define ISEARCH_BS_LEGACYCHECKPOINTLISTSYNCRONIZER_H

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/common/BuilderCheckpointAccessor.h"
#include "build_service/common/IndexCheckpointAccessor.h"
#include "build_service/common_define.h"
#include "build_service/config/CheckpointList.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class LegacyCheckpointListSyncronizer
{
public:
    LegacyCheckpointListSyncronizer(const std::string& generationZkDir, cm_basic::ZkWrapper* zkWrapper,
                                    const TaskResourceManagerPtr& resMgr, const std::vector<std::string>& clusterNames);
    ~LegacyCheckpointListSyncronizer();

private:
    LegacyCheckpointListSyncronizer(const LegacyCheckpointListSyncronizer&);
    LegacyCheckpointListSyncronizer& operator=(const LegacyCheckpointListSyncronizer&);

public:
    void syncCheckpoints();

private:
    bool isEqual(const std::set<versionid_t>& left, const std::set<versionid_t>& right);
    bool commitCheckpointList(const config::CheckpointList& ccpList, const std::string& filePath);

private:
    std::string _generationZkDir;
    cm_basic::ZkWrapper* _zkWrapper;
    TaskResourceManagerPtr _resMgr;
    std::map<std::string, config::CheckpointList> _clusterCheckpoints;
    common::IndexCheckpointAccessorPtr _indexCkpAccessor;
    common::BuilderCheckpointAccessorPtr _builderCkpAccessor;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(LegacyCheckpointListSyncronizer);

}} // namespace build_service::admin

#endif // ISEARCH_BS_LEGACYCHECKPOINTLISTSYNCRONIZER_H
