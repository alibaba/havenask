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
#include "autil/legacy/jsonizable.h"
#include <unordered_map>

namespace navi {

class NaviLogger;
class CreatorRegistryImpl;
class Module;
struct RegistrySummary;
class NaviJsonizeInfo;

typedef std::unordered_map<std::string, std::vector<NaviJsonizeInfo>> NaviJsonizeInfoMap;

class Creator : public autil::legacy::Jsonizable {
public:
    Creator(const char *sourceFile);
    virtual ~Creator();
    virtual const std::string &getName() const = 0;
    virtual bool init(bool isBuildin, Module *module) {
        setBuildin(isBuildin);
        setModule(module);
        return true;
    }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    const char *getSourceFile() const {
        return _sourceFile;
    }
    void setBuildin(bool isBuildin) {
        _isBuildin = isBuildin;
    }
    bool isBuildin() const {
        return _isBuildin;
    }
    void setModule(Module *module) {
        _module = module;
    }
    Module *getModule() const {
        return _module;
    }
    const std::string &getModulePath() const;
    void initJsonizeInfo(const NaviJsonizeInfoMap &jsonizeInfoMap);
public:
    CreatorStats &getCreatorStats() const {
        return _stats;
    }
protected:
    virtual std::string getClassName() const {
        return std::string();
    }
protected:
    bool _isBuildin;
    const char *_sourceFile;
    Module *_module;
    std::vector<NaviJsonizeInfo> _jsonizeInfoVec;
    mutable CreatorStats _stats;
};

NAVI_TYPEDEF_PTR(Creator);

typedef std::unordered_map<std::string, CreatorPtr> CreatorMap;

typedef void (*NaviJsonizeInfoFunc)(NaviJsonizeInfo &info);

class CreatorRegistry
{
public:
    CreatorRegistry();
    ~CreatorRegistry();
private:
    CreatorRegistry(const CreatorRegistry &);
    CreatorRegistry &operator=(const CreatorRegistry &);
public:
    bool addModuleInitFunc(const NaviModuleInitFunc &func);
    bool addCreatorFunc(const NaviCreatorFunc &func);
public:
    void addSubRegistry(const CreatorRegistry &registry);
    void addJsonizeFunc(NaviJsonizeInfoFunc func);
    bool initModule(Module *module);
    bool initCreators(bool isBuildin);
    Creator *getCreator(const std::string &name) const;
    const CreatorMap &getCreatorMap() const;
    void clear();
    void setInited(bool inited=true);
    const NaviJsonizeInfoMap &getJsonizeInfoMap() const;
    void fillJsonizeInfo(const NaviJsonizeInfoMap &jsonizeInfoMap);
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
