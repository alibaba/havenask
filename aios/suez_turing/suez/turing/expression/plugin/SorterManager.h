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
#include <set>
#include <string>

#include "autil/Log.h"
#include "build_service/plugin/PlugInManager.h"
#include "resource_reader/ResourceReader.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/function/FunctionCreatorResource.h"

namespace suez {
namespace turing {

class SorterManager;
class Sorter;
class SorterConfig;

SUEZ_TYPEDEF_PTR(SorterManager);

class SorterManager {
public:
    SorterManager(const std::string &configPath);
    ~SorterManager();

private:
    SorterManager(const SorterManager &);
    SorterManager &operator=(const SorterManager &);

public:
    static SorterManagerPtr create(const resource_reader::ResourceReaderPtr &resourceReader, const std::string &conf);
    static SorterManagerPtr create(const std::string &configPath, const SorterConfig &sorterConfig);
    Sorter *getSorter(const std::string &sorterName) const;
    bool addSorter(const std::string &name, Sorter *sorter);
    void setPlugInManager(const build_service::plugin::PlugInManagerPtr &plugInManagerPtr) {
        _plugInManagerPtr = plugInManagerPtr;
    }
    void getSorterNames(std::set<std::string> &sorterNames) const;
    suez::ResourceReaderPtr getResourceReader() { return _resourceReaderPtr; }

private:
    void clear();

private:
    typedef std::map<std::string, Sorter *> SorterMap;
    suez::ResourceReaderPtr _resourceReaderPtr;
    build_service::plugin::PlugInManagerPtr _plugInManagerPtr;
    SorterMap _sorters;

private:
    AUTIL_LOG_DECLARE();

private:
    friend class SorterManagerCreatorTest;
};

SUEZ_TYPEDEF_PTR(SorterManager);

typedef std::map<std::string, SorterManagerPtr> ClusterSorterManagers;
typedef std::map<std::string, std::set<std::string>> ClusterSorterNames;

SUEZ_TYPEDEF_PTR(ClusterSorterManagers);
SUEZ_TYPEDEF_PTR(ClusterSorterNames);

} // namespace turing
} // namespace suez
