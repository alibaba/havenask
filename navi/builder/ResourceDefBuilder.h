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
#ifndef NAVI_RESOURCEDEFBUILDER_H
#define NAVI_RESOURCEDEFBUILDER_H

#include <functional>
#include "navi/common.h"
#include "navi/proto/KernelDef.pb.h"

namespace navi {

class ResourceDefBuilder
{
public:
    ResourceDefBuilder(ResourceDef *def);
    ~ResourceDefBuilder();
private:
    ResourceDefBuilder(const ResourceDefBuilder &);
    ResourceDefBuilder &operator=(const ResourceDefBuilder &);
public:
    ResourceDefBuilder &name(const std::string &name);
    ResourceDefBuilder &depend(const std::string &name,
                               bool require,
                               const StaticResourceBindFunc &binder);
    ResourceDefBuilder &dynamicDepend(const DynamicResourceBindFunc &binder,
            const std::set<std::string> &resourceSet = {});
    ResourceDefBuilder &loadType(ResourceLoadType type);
private:
    ResourceDef *def() const;
    const ResourceBindInfos &getBinderInfos() const;
    friend class ResourceCreator;
private:
    ResourceDef *_def;
    ResourceBindInfos _binderInfos;
};

}

#endif //NAVI_RESOURCEDEFBUILDER_H
