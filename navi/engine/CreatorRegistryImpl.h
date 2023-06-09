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
#ifndef NAVI_CREATORREGISTRYIMPL_H
#define NAVI_CREATORREGISTRYIMPL_H

#include "navi/common.h"
#include "navi/engine/CreatorRegistry.h"

namespace navi {

class NaviLogger;
class Creator;

class CreatorRegistryImpl
{
public:
    CreatorRegistryImpl();
    ~CreatorRegistryImpl();
private:
    CreatorRegistryImpl(const CreatorRegistryImpl &);
    CreatorRegistryImpl &operator=(const CreatorRegistryImpl &);
public:
    void addModuleInitFunc(const NaviModuleInitFunc &func);
    void addCreatorFunc(const NaviCreatorFunc &func);
    void merge(const CreatorRegistryImpl *impl);
    bool initModule();
    bool initCreators();
    Creator *getCreator(const std::string &name) const;
    const CreatorMap &getCreatorMap() const;
    void clear();
private:
    std::vector<NaviModuleInitFunc> _moduleInitFuncList;
    std::vector<NaviCreatorFunc> _funcList;
    CreatorMap _creatorMap;
};

}

#endif //NAVI_CREATORREGISTRYIMPL_H
