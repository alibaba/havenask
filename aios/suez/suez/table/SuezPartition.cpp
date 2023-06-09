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
#include "suez/table/SuezPartition.h"

#include <assert.h>
#include <iosfwd>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "suez/sdk/PathDefine.h"
#include "suez/sdk/SuezError.h"
#include "suez/table/TablePathDefine.h"

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor

using namespace std;
using namespace kmonitor;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, SuezPartition);

namespace suez {

SuezPartition::SuezPartition(const CurrentPartitionMetaPtr &partitionMeta,
                             const PartitionId &pid,
                             const kmonitor::MetricsReporterPtr &metricsReporter)
    : _partitionMeta(partitionMeta), _pid(pid), _metricsReporter(metricsReporter), _shutdownFlag(false) {}

SuezPartition::~SuezPartition() {}

StatusAndError<DeployStatus> SuezPartition::setDeployConfigStatus(DeployStatus status,
                                                                  const TargetPartitionMeta &target) {
    StatusAndError<DeployStatus> ret;
    switch (status) {
    case DS_DEPLOYDONE:
        ret = StatusAndError<DeployStatus>(DS_DEPLOYDONE, ERROR_NONE);
        break;
    case DS_CANCELLED: {
        auto error = _partitionMeta->getError();
        if (error == DEPLOY_CONFIG_ERROR) {
            error = ERROR_NONE;
        }
        ret = StatusAndError<DeployStatus>(DS_CANCELLED, error);
    } break;
    case DS_DISKQUOTA:
        ret = StatusAndError<DeployStatus>(DS_DISKQUOTA, DEPLOY_DISK_QUOTA_ERROR);
        break;
    default:
        ret = StatusAndError<DeployStatus>(DS_FAILED, DEPLOY_CONFIG_ERROR);
        break;
    }
    return ret;
}

StatusAndError<DeployStatus> SuezPartition::setDeployStatus(DeployStatus status, const TargetPartitionMeta &target) {
    StatusAndError<DeployStatus> ret;
    switch (status) {
    case DS_DEPLOYDONE:
        _partitionMeta->setIndexRoot(target.getIndexRoot());
        _partitionMeta->setConfigPath(target.getConfigPath());
        ret = StatusAndError<DeployStatus>(DS_DEPLOYDONE, ERROR_NONE);
        break;
    case DS_CANCELLED:
        ret = StatusAndError<DeployStatus>(DS_CANCELLED, ERROR_NONE);
        break;
    case DS_DISKQUOTA:
        ret = StatusAndError<DeployStatus>(DS_DISKQUOTA, DEPLOY_DISK_QUOTA_ERROR);
        break;
    default:
        ret = StatusAndError<DeployStatus>(DS_FAILED, DEPLOY_INDEX_ERROR);
        break;
    }
    return ret;
}

std::string SuezPartition::getLocalIndexPath() const {
    return TablePathDefine::constructIndexPath(PathDefine::getLocalIndexRoot(), _pid);
}

const PartitionId &SuezPartition::getPartitionId() const { return _pid; }

int32_t SuezPartition::getTotalPartitionCount() const { return _partitionMeta->getTableMeta().totalPartitionCount; }

void SuezPartition::cleanIndexFiles(const std::set<IncVersion> &inUseIncVersions) {}

void SuezPartition::shutdown() { _shutdownFlag = true; }

bool SuezPartition::needCommit() const { return false; }

std::pair<bool, TableVersion> SuezPartition::commit() { return std::make_pair(false, TableVersion()); }

} // namespace suez
