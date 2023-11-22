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
#include "build_service/common/ResourceKeeper.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/common/ResourceKeeperCreator.h"

using namespace std;

namespace build_service { namespace common {
BS_LOG_SETUP(common, ResourceKeeper);

ResourceKeeper::ResourceKeeper(const std::string& resourceName, const std::string& type)
    : _resourceName(resourceName)
    , _type(type)
{
}

ResourceKeeper::~ResourceKeeper() {}

void ResourceKeeper::BasicInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("name", name, name);
    json.Jsonize("type", type, type);
}

void ResourceKeeper::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("name", _resourceName);
    json.Jsonize("type", _type);
    json.Jsonize("param", _parameters, _parameters);
}

ResourceKeeperPtr ResourceKeeper::deserializeResourceKeeper(const std::string& jsonStr)
{
    BasicInfo info;
    try {
        FromJsonString(info, jsonStr);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "invalid json str [%s]", jsonStr.c_str());
        return ResourceKeeperPtr();
    }
    if (info.name.empty() || info.type.empty()) {
        BS_LOG(ERROR, "invalid resource keeper json str [%s], name or type is empty", jsonStr.c_str());
        return ResourceKeeperPtr();
    }
    ResourceContainerPtr container;
    ResourceKeeperPtr resourceKeeper = ResourceKeeperCreator::create(info.name, info.type, container);

    try {
        FromJsonString(*resourceKeeper, jsonStr);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "invalid json str [%s]", jsonStr.c_str());
        return ResourceKeeperPtr();
    }
    return resourceKeeper;
}

bool ResourceKeeper::getParam(const std::string& key, std::string* value) const
{
    auto iter = _parameters.find(key);
    if (iter == _parameters.end()) {
        return false;
    }
    *value = iter->second;
    return true;
}

}} // namespace build_service::common
