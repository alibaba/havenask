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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/Lock.h"
#include "build_service/admin/CheckpointMetricReporter.h"
#include "build_service/admin/CheckpointSynchronizer.h"
#include "build_service/admin/ClusterCheckpointSynchronizer.h"
#include "build_service/common/Checkpoint.h"
#include "build_service/common/CheckpointAccessor.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "indexlib/framework/VersionMeta.h"

namespace build_service { namespace admin {

class ZKCheckpointSynchronizer : public CheckpointSynchronizer
{
public:
    explicit ZKCheckpointSynchronizer(const proto::BuildId& buildId);
    ~ZKCheckpointSynchronizer();

    bool init(const std::map<std::string, std::shared_ptr<ClusterCheckpointSynchronizer>>& clusterSynchronizers,
              bool isLeaderFollowerMode) override;
    void clear() override;
    bool sync() override;

private:
    bool updateVersionMeta(const std::string& path);
    void onDataChange(cm_basic::ZkWrapper* zk, const std::string& path, cm_basic::ZkWrapper::ZkStatus status);
    void connectionChangedNotify(cm_basic::ZkWrapper* zk, const std::string& path,
                                 cm_basic::ZkWrapper::ZkStatus status);
    bool getPathInfo(const std::string& path, common::PathInfo* pathInfo) const;
    bool getVersionMeta(const std::string& path, indexlibv2::framework::VersionMeta* meta) const;

private:
    std::map<std::string, std::string> _suezVersionZkRoots;
    std::map<std::string, common::PathInfo> _pathInfos;
    std::map<std::string, std::vector<std::string>> _clusterToPaths;
    std::shared_ptr<common::CheckpointAccessor> _checkpointAccessor;
    std::shared_ptr<CheckpointMetricReporter> _metricsReporter;

    cm_basic::ZkWrapper _zk;
    autil::ThreadMutex _mutex;
    BS_LOG_DECLARE();
};

}} // namespace build_service::admin
