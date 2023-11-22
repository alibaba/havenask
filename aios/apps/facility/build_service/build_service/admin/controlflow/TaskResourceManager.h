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
#include <stdint.h>
#include <string>

#include "autil/Lock.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common/ResourceKeeper.h"
#include "build_service/common/ResourceKeeperGuard.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"

namespace build_service { namespace admin {
struct ResourceKeeperInfo : public autil::legacy::Jsonizable {
    ResourceKeeperInfo(const common::ResourceContainerPtr& container);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    std::string type;
    std::string name;
    common::ResourceKeeperPtr resourceKeeper;
    common::ResourceContainerPtr resourceContainer;
};

class TaskResourceManager
{
public:
    TaskResourceManager();
    ~TaskResourceManager();

public:
    bool declareResourceKeeper(const std::string& type, const KeyValueMap& params);
    common::ResourceKeeperPtr getResourceKeeper(const KeyValueMap& params);
    common::ResourceKeeperGuardPtr applyResource(const std::string& roleName, const std::string& resourceName,
                                                 const config::ResourceReaderPtr& configReader);
    std::string serializeResourceKeepers();
    bool deserializeResourceKeepers(const std::string& jsonStr);

public:
    template <typename T>
    bool addResource(const std::string& resId, const T& res)
    {
        return _resourceContainer->addResource(resId, res);
    }

    template <typename T>
    bool addResource(const T& res)
    {
        return _resourceContainer->addResource(res);
    }

    template <typename T>
    bool getResource(T& res)
    {
        return _resourceContainer->getResource(res);
    }

    template <typename T>
    bool getResource(const std::string& resId, T& res)
    {
        return _resourceContainer->getResource(resId, res);
    }

    void setLogPrefix(const std::string& str) { _resourceContainer->setLogPrefix(str); }
    const std::string& getLogPrefix() const { return _resourceContainer->getLogPrefix(); }

public:
    // for test
    template <typename T>
    void addResourceIgnoreExist(const T& res)
    {
        _resourceContainer->addResourceIgnoreExist(res);
    }

private:
    typedef std::map<std::string, common::ResourceKeeperPtr> ResourceKeeperMap;
    common::ResourceContainerPtr _resourceContainer;
    ResourceKeeperMap _resourceKeeperMap;
    mutable autil::ThreadMutex _lock;
    int64_t _version;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskResourceManager);
}} // namespace build_service::admin
