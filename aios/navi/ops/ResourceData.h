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

#include "navi/engine/Data.h"
#include "navi/engine/Resource.h"
#include "navi/engine/Type.h"

namespace navi {

class ResourceDataType : public Type
{
public:
    ResourceDataType();
private:
    ResourceDataType(const ResourceDataType &);
    ResourceDataType &operator=(const ResourceDataType &);
};

class ResourceData : public Data
{
public:
    ResourceData();
private:
    ResourceData(const ResourceData &);
    ResourceData &operator=(const ResourceData &);
public:
    bool validate(bool inputRequire) const;
    bool validateReplace() const;
    bool validateDepend() const;
public:
    std::string _name;
    bool _require : 1;
    bool _wait : 1;
    ResourcePtr _resource;
    ResourceStage _stage;
    ErrorCode _ec;
    const std::map<std::string, bool> *_dependMap = nullptr;
    std::map<std::string, std::shared_ptr<ResourceData>> _dependDatas;
    const std::unordered_set<std::string> *_replaceSet = nullptr;
};

NAVI_TYPEDEF_PTR(ResourceData);

}

