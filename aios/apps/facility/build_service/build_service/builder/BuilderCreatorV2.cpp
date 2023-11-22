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
#include "build_service/builder/BuilderCreatorV2.h"

#include <assert.h>
#include <map>
#include <utility>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "build_service/builder/AsyncBuilderV2.h"
#include "build_service/builder/BuilderV2Impl.h"
#include "build_service/builder/OfflineBuilderV2.h"
#include "build_service/config/BuilderClusterConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/util/IndexPathConstructor.h"

#define TABLET_LOG(level, format, args...)                                                                             \
    AUTIL_LOG(level, "[%s] " format, _partitionId.clusternames(0).c_str(), ##args)

namespace build_service::builder {

BS_LOG_SETUP(builder, BuilderCreatorV2);

BuilderCreatorV2::BuilderCreatorV2(std::shared_ptr<indexlibv2::framework::ITablet> tablet) : _tablet(std::move(tablet))
{
}

BuilderCreatorV2::~BuilderCreatorV2() {}

std::unique_ptr<BuilderV2> BuilderCreatorV2::create(const config::ResourceReaderPtr& resourceReader,
                                                    const KeyValueMap& kvMap, const proto::PartitionId& partitionId,
                                                    std::shared_ptr<indexlib::util::MetricProvider> metricProvider)
{
    assert(partitionId.clusternames_size() == 1);
    _partitionId = partitionId;
    uint32_t backupId = 0;
    if (partitionId.has_backupid() && partitionId.backupid() != 0) {
        backupId = partitionId.backupid();
        TABLET_LOG(INFO, "backupId [%d], starting", backupId);
    }
    const std::string& clusterName = partitionId.clusternames(0);
    std::string mergeConfigName = getValueFromKeyValueMap(kvMap, config::MERGE_CONFIG_NAME);

    config::BuilderClusterConfig clusterConfig;
    if (!clusterConfig.init(clusterName, *resourceReader, mergeConfigName)) {
        BS_LOG(ERROR, "failed to init BuilderClusterConfig");
        return nullptr;
    }

    if (_tablet) {
        // online
        if (backupId) {
            TABLET_LOG(ERROR, "can't create backup buidler with existant tablet!");
            return nullptr;
        }
        // update indexOptions by indexPartition
        // fix job mode realtime buildConfig effective less problem
        // clusterConfig.indexOptions = _indexPart->GetOptions();
    }

    std::string workerPathVersionStr = getValueFromKeyValueMap(kvMap, config::WORKER_PATH_VERSION, "");
    if (workerPathVersionStr.empty()) {
        _workerPathVersion = -1;
    } else if (!autil::StringUtil::fromString(workerPathVersionStr, _workerPathVersion)) {
        TABLET_LOG(ERROR, "invalid workerPathVersion [%s]", workerPathVersionStr.c_str());
        return nullptr;
    }

    _indexRootPath = getValueFromKeyValueMap(kvMap, config::INDEX_ROOT_PATH);
    if (_indexRootPath.empty() && !_tablet) {
        BS_LOG(ERROR, "index root path can not be empty");
        return nullptr;
    }

    std::unique_ptr<BuilderV2> builder;
    if (!_tablet) {
        // offline build
        auto indexPath = util::IndexPathConstructor::constructIndexPath(_indexRootPath, _partitionId);
        builder = std::make_unique<OfflineBuilderV2>(clusterName, _partitionId, resourceReader, indexPath);
    } else {
        // online build
        builder = std::make_unique<BuilderV2Impl>(_tablet, _partitionId.buildid());
    }
    // v2 build use asnyc build as default
    auto impl = std::move(builder);
    bool regroup = true;
    auto docTypeIter = kvMap.find(config::INPUT_DOC_TYPE);
    if (docTypeIter != kvMap.end() && docTypeIter->second == config::INPUT_DOC_BATCHDOC) {
        regroup = false;
    }
    builder = std::make_unique<AsyncBuilderV2>(std::move(impl), regroup);
    if (!builder->init(clusterConfig.builderConfig, metricProvider)) {
        TABLET_LOG(ERROR, "init builder failed");
        return nullptr;
    }
    return builder;
}

} // namespace build_service::builder
