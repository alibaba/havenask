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
#ifndef ISEARCH_BS_CHECKPOINTRESOURCEKEEPER_H
#define ISEARCH_BS_CHECKPOINTRESOURCEKEEPER_H

#include "build_service/common/CheckpointAccessor.h"
#include "build_service/common/ResourceKeeper.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace common {

class CheckpointResourceKeeper : public ResourceKeeper
{
public:
    enum CheckpointResourceStatus {
        CRS_UNSAVEPOINTED = 0,
        CRS_SAVEPOINTED = 1,
    };
    struct CheckpointResourceInfo {
        versionid_t version;
        KeyValueMap param;
        CheckpointResourceStatus status;
    };
    typedef std::vector<CheckpointResourceInfo> CheckpointResourceInfoVec;

    CheckpointResourceKeeper(const std::string& name, const std::string& type,
                             const ResourceContainerPtr& resourceContainer);
    ~CheckpointResourceKeeper();

private:
    CheckpointResourceKeeper(const CheckpointResourceKeeper& rhs);
    CheckpointResourceKeeper& operator=(const CheckpointResourceKeeper&);

public:
    /*
      {
          "checkpoint_type":"type of checkpoint",  // required, different resource name with same checkpoint_type share
      same checkpointid "need_latest_version":"true",            // optional, default false, if true, applyer with
      automaticlly get latest version as target "resource_version":"100",                // optional, defauiult -1,
      version of the resource, will be rewrite if need_latest_version is true
      }
    */
    bool init(const KeyValueMap& params) override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool prepareResource(const std::string& applyer, const config::ResourceReaderPtr& configReader) override;
    void deleteApplyer(const std::string& applyer) override;
    void syncResources(const std::string& roleName, const proto::WorkerNodes& workerNodes) override;
    std::string getResourceId() override;

    /*
        {
            "checkpoint_type": "type of checkpoint",    // required, type of checkpoint
            "xxxx":"xxxx"
        }
     */
    bool addResource(const KeyValueMap& params) override;

    /*
        {
            "checkpoint_type": "type of checkpoint",        // required, type of checkpoint
            "resource_version":"100"                        // required, version of resource to delete
        }
     */
    bool deleteResource(const KeyValueMap& params) override;
    CheckpointResourceInfoVec getResourceStatus();

private:
    bool updateSelf(const std::string& applyer, versionid_t newResourceVersion);
    std::string getCkptName(versionid_t resourceVersion) const;
    std::string getCkptId(const std::string& checkpointType) const;
    void clearUselessCheckpoints(const std::string& applyer, const proto::WorkerNodes& workerNodes);

public:
    static constexpr const char* NEED_LATEST_VERSION = "need_latest_version";
    static constexpr const char* RESOURCE_VERSION = "resource_version";
    static constexpr const char* CHECKPOINT_TYPE = "checkpoint_type";

private:
    common::CheckpointAccessorPtr _ckptAccessor;
    std::string _checkpointType;
    versionid_t _resourceVersion;
    bool _needLatestVersion;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CheckpointResourceKeeper);
}} // namespace build_service::common

#endif // ISEARCH_BS_CHECKPOINTRESOURCEKEEPER_H
