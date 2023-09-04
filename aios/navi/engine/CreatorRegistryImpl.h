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
    bool addModuleInitFunc(const NaviModuleInitFunc &func);
    bool addCreatorFunc(const NaviCreatorFunc &func);
public:
    void addSubRegistry(const CreatorRegistryImpl *impl);
    void addJsonizeFunc(const NaviJsonizeInfoFunc &func);
    bool initModule(Module *module);
    bool initCreators(bool isBuildin);
    Creator *getCreator(const std::string &name) const;
    const CreatorMap &getCreatorMap() const;
    void clear();
    void setInited(bool inited=true);
    void fillJsonizeInfo(const NaviJsonizeInfoMap &jsonizeInfoMap);
    const NaviJsonizeInfoMap &getJsonizeInfoMap() const {
        return _jsonizeInfoMap;
    }
private:
    bool getCreators(bool isBuildin, CreatorMap &creatorMap) const;
    void addJsonizeInfo(const NaviJsonizeInfoFunc &func);
private:
    bool _inited;
    Module *_module = nullptr;
    std::vector<NaviModuleInitFunc> _moduleInitFuncList;
    std::vector<NaviCreatorFunc> _funcList;
    std::set<NaviJsonizeInfoFunc> _jsonizeFuncSet;
    NaviJsonizeInfoMap _jsonizeInfoMap;
    CreatorMap _creatorMap;
    std::vector<const CreatorRegistryImpl *> _subRegistrys;
};

}

#endif //NAVI_CREATORREGISTRYIMPL_H
