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
#ifndef ISEARCH_BS_RESOURCEKEEPER_H
#define ISEARCH_BS_RESOURCEKEEPER_H

#include "autil/legacy/jsonizable.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/Log.h"

namespace build_service { namespace common {

class ResourceKeeper;
BS_TYPEDEF_PTR(ResourceKeeper);

class ResourceKeeper : public autil::legacy::Jsonizable
{
public:
    ResourceKeeper(const std::string& resourceName, const std::string& type);
    ~ResourceKeeper();

private:
    ResourceKeeper(const ResourceKeeper&);
    ResourceKeeper& operator=(const ResourceKeeper&);

public:
    virtual bool init(const KeyValueMap& params) = 0;
    virtual bool prepareResource(const std::string& applyer, const config::ResourceReaderPtr& configReader) = 0;
    virtual void deleteApplyer(const std::string& applyer) = 0;
    virtual void syncResources(const std::string& roleName, const proto::WorkerNodes& workerNodes) = 0;
    virtual std::string getResourceId() = 0;
    const std::string& getResourceName() { return _resourceName; };
    const std::string& getType() { return _type; }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool getParam(const std::string& key, std::string* value) const;
    static ResourceKeeperPtr deserializeResourceKeeper(const std::string& jsonStr);
    virtual bool addResource(const KeyValueMap& params)
    {
        assert(false);
        return false;
    };
    virtual bool deleteResource(const KeyValueMap& params)
    {
        assert(false);
        return false;
    };
    virtual bool updateResource(const KeyValueMap& params)
    {
        assert(false);
        return false;
    }

private:
    struct BasicInfo : public autil::legacy::Jsonizable {
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
        std::string name;
        std::string type;
        std::string resourceId;
    };

protected:
    KeyValueMap _parameters;
    std::string _resourceName;
    std::string _type;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ResourceKeeper);
typedef std::unordered_map<std::string, ResourceKeeperPtr> ResourceKeeperMap;
BS_TYPEDEF_PTR(ResourceKeeperMap);
}} // namespace build_service::common

#endif // ISEARCH_BS_RESOURCEKEEPER_H
