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
#include "build_service/admin/LegacyCheckpointListSyncronizer.h"

#include <cstddef>
#include <memory>
#include <stdint.h>
#include <utility>

#include "alog/Logger.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/admin/CheckpointCreator.h"
#include "build_service/admin/CheckpointMetricReporter.h"
#include "build_service/common/PathDefine.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "worker_framework/WorkerState.h"
#include "worker_framework/ZkState.h"

using namespace std;
using namespace build_service::config;
using namespace build_service::common;
using namespace worker_framework;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, LegacyCheckpointListSyncronizer);

LegacyCheckpointListSyncronizer::LegacyCheckpointListSyncronizer(const std::string& generationZkDir,
                                                                 cm_basic::ZkWrapper* zkWrapper,
                                                                 const TaskResourceManagerPtr& resMgr,
                                                                 const std::vector<std::string>& clusterNames)
    : _generationZkDir(generationZkDir)
    , _zkWrapper(zkWrapper)
    , _resMgr(resMgr)
{
    for (auto& clusterName : clusterNames) {
        _clusterCheckpoints[clusterName] = CheckpointList();
    }
    _indexCkpAccessor = CheckpointCreator::createIndexCheckpointAccessor(_resMgr);
    _builderCkpAccessor = CheckpointCreator::createBuilderCheckpointAccessor(_resMgr);
}

LegacyCheckpointListSyncronizer::~LegacyCheckpointListSyncronizer() {}

void LegacyCheckpointListSyncronizer::syncCheckpoints()
{
    auto iter = _clusterCheckpoints.begin();
    for (; iter != _clusterCheckpoints.end(); iter++) {
        string clusterName = iter->first;
        set<versionid_t> versions =
            IndexCheckpointAccessor::getReservedVersions(clusterName, _indexCkpAccessor, _builderCkpAccessor);
        auto& checkpointList = iter->second;
        auto& idSet = checkpointList.getIdSet();
        if (!isEqual(versions, idSet)) {
            idSet.swap(versions);
            string reservedCheckpointFile = PathDefine::getReservedCheckpointFileName(_generationZkDir, clusterName);
            commitCheckpointList(checkpointList, reservedCheckpointFile);
        }

        CheckpointMetricReporterPtr reporter;
        if (!_resMgr->getResource(reporter)) {
            continue;
        }

        ::google::protobuf::RepeatedPtrField<proto::IndexInfo> indexInfos;
        if (!_indexCkpAccessor->getIndexInfo(false, clusterName, indexInfos) || indexInfos.size() == 0) {
            continue;
        }

        kmonitor::MetricsTags tags;
        tags.AddTag("cluster", clusterName);

        reporter->reportIndexVersion((int64_t)indexInfos.Get(0).indexversion(), tags);
        reporter->reportIndexSchemaVersion((int64_t)indexInfos.Get(0).schemaversion(), tags);
        reporter->calculateAndReportIndexTimestampFreshness((int64_t)indexInfos.Get(0).versiontimestamp(), tags);
        int64_t indexSize = 0;
        int64_t totalRemainIndexSize = 0;
        for (size_t i = 0; i < (size_t)indexInfos.size(); i++) {
            indexSize += indexInfos.Get(i).indexsize();
            totalRemainIndexSize += indexInfos.Get(i).totalremainindexsize();
        }
        reporter->reportIndexSize(indexSize, tags);
        reporter->reportIndexRemainSize(totalRemainIndexSize, tags);
    }
}

bool LegacyCheckpointListSyncronizer::isEqual(const set<versionid_t>& left, const set<versionid_t>& right)
{
    if (left.size() != right.size()) {
        return false;
    }
    auto iter = left.begin();
    for (; iter != left.end(); iter++) {
        if (right.find(*iter) == right.end()) {
            return false;
        }
    }
    return true;
}

bool LegacyCheckpointListSyncronizer::commitCheckpointList(const CheckpointList& ccpList, const string& filePath)
{
    string fileContent = ToJsonString(ccpList, true);
    ZkState ccpListFile(_zkWrapper, filePath);
    auto ret = ccpListFile.write(fileContent);
    if (ret != WorkerState::EC_OK && ret != WorkerState::EC_UPDATE) {
        BS_LOG(ERROR, "write checkpoints to %s failed, content[%s]", filePath.c_str(), fileContent.c_str());
        return false;
    }
    return true;
}

}} // namespace build_service::admin
