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

#include "navi/engine/Resource.h"

namespace navi {

class MetaInfoResourceBase : public navi::Resource
{
public:
    MetaInfoResourceBase();
    ~MetaInfoResourceBase();
    MetaInfoResourceBase(const MetaInfoResourceBase &) = delete;
    MetaInfoResourceBase &operator=(const MetaInfoResourceBase &) = delete;
public:
    virtual void preSerialize() = 0;
    virtual void serialize(autil::DataBuffer &dataBuffer) const = 0;
    virtual bool merge(autil::DataBuffer &dataBuffer) = 0;
    virtual void finalize() = 0;
};

NAVI_TYPEDEF_PTR(MetaInfoResourceBase);

}
