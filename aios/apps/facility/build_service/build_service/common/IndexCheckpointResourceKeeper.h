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

#include <string>
#include <vector>

#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/common/IndexCheckpointAccessor.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common/ResourceKeeper.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/WorkerNode.h"
#include "indexlib/base/Types.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace common {

class IndexCheckpointResourceKeeper : public ResourceKeeper
{
public:
    IndexCheckpointResourceKeeper(const std::string& name, const std::string& type,
                                  const ResourceContainerPtr& resourceContainer);
    ~IndexCheckpointResourceKeeper();

private:
    IndexCheckpointResourceKeeper(const IndexCheckpointResourceKeeper&);
    IndexCheckpointResourceKeeper& operator=(const IndexCheckpointResourceKeeper&);

public:
    bool init(const KeyValueMap& params) override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool prepareResource(const std::string& applyer, const config::ResourceReaderPtr& configReader) override;
    void deleteApplyer(const std::string& applyer) override;
    void syncResources(const std::string& applyer, const proto::WorkerNodes& workerNodes) override;
    std::string getResourceId() override;
    bool updateResource(const KeyValueMap& params) override;

    versionid_t getTargetVersionid() const { return _targetVersion; }
    std::string getClusterName() const { return _clusterName; }

private:
    void savepointLatestVersion();
    void clearUselessCheckpoints(const std::string& applyer, const proto::WorkerNodes& workerNodes);
    void updateVersion(versionid_t version);
    bool getIndexRootFromConfig();

private:
    std::string _clusterName;
    versionid_t _targetVersion;
    IndexCheckpointAccessorPtr _checkpointAccessor;
    common::ResourceContainerPtr _resourceContainer;
    bool _needLatestVersion;
    std::vector<std::string> _applyers;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(IndexCheckpointResourceKeeper);

}} // namespace build_service::common
