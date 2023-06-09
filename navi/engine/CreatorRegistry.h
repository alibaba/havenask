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
#ifndef NAVI_CREATORREGISTRY_H
#define NAVI_CREATORREGISTRY_H

#include "navi/common.h"
#include "navi/engine/CreatorStats.h"
#include <unordered_map>

namespace navi {

class NaviLogger;
class CreatorRegistryImpl;

class Creator {
public:
    Creator() {
    }
    virtual ~Creator() {
    }
    virtual const std::string &getName() const = 0;
    virtual bool init() {
        return true;
    }
public:
    CreatorStats &getCreatorStats() const {
        return _stats;
    }
protected:
    mutable CreatorStats _stats;
};

NAVI_TYPEDEF_PTR(Creator);

typedef std::unordered_map<std::string, CreatorPtr> CreatorMap;

class CreatorRegistry
{
public:
    CreatorRegistry();
    ~CreatorRegistry();
private:
    CreatorRegistry(const CreatorRegistry &);
    CreatorRegistry &operator=(const CreatorRegistry &);
public:
    void addModuleInitFunc(const NaviModuleInitFunc &func);
    void addCreatorFunc(const NaviCreatorFunc &func);
    void merge(const CreatorRegistry &registry);
    bool initModule();
    bool initCreators();
    Creator *getCreator(const std::string &name) const;
    const CreatorMap &getCreatorMap() const;
    void clear();
public:
    static CreatorRegistry *current(RegistryType type);
    static void setCurrent(RegistryType type, CreatorRegistry *current);
    static CreatorRegistry *buildin(RegistryType type);
private:
    CreatorRegistryImpl *_impl;
private:
    static CreatorRegistry *CURRENT[RT_MAX];
    static CreatorRegistry *NAVI_BUILDIN[RT_MAX];
};

}

#endif //NAVI_CREATORREGISTRY_H
