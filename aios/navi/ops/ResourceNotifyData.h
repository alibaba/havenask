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

class ResourceNotifyDataType : public Type {
public:
    ResourceNotifyDataType();
private:
    ResourceNotifyDataType(const ResourceNotifyDataType &);
    ResourceNotifyDataType &operator=(const ResourceNotifyDataType &);
};

class ResourceNotifyData : public Data
{
public:
    ResourceNotifyData();
    ~ResourceNotifyData();
    ResourceNotifyData(const ResourceNotifyData &) = delete;
    ResourceNotifyData &operator=(const ResourceNotifyData &) = delete;
public:
    Resource *_resource = nullptr;
    ResourcePtr _result;
};

NAVI_TYPEDEF_PTR(ResourceNotifyData);

}

