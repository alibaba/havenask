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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "build_service/plugin/PlugInManager.h"
#include "build_service/config/ResourceReader.h"
#include "ha3/sql/ops/tvf/SqlTvfProfileInfo.h"
#include "ha3/sql/ops/tvf/TvfResourceBase.h"

namespace iquan {
class TvfModels;
} // namespace iquan
namespace isearch {
namespace sql {
class SqlTvfPluginConfig;
class TvfFunc;
class TvfFuncCreatorR;
class TvfFuncCreatorFactory;

typedef std::shared_ptr<TvfFuncCreatorFactory> TvfFuncCreatorFactoryPtr;
} // namespace sql
} // namespace isearch

namespace isearch {
namespace sql {

class TvfFuncManager : public build_service::plugin::PlugInManager {
public:
    TvfFuncManager(const build_service::config::ResourceReaderPtr &resourceReader);
    ~TvfFuncManager();

private:
    TvfFuncManager(const TvfFuncManager &);
    TvfFuncManager &operator=(const TvfFuncManager &);

public:
    void addFactoryPtr(TvfFuncCreatorFactoryPtr fatcory) {
        _builtinFactoryVec.push_back(fatcory);
    }
    bool init(const SqlTvfPluginConfig &tvfPluginConfig, bool useBuiltin = true);
    TvfFunc *createTvfFunction(const std::string &tvfName);
    TvfResourceContainer *getTvfResourceContainer() {
        return &_resourceContainer;
    }
    bool regTvfModels(iquan::TvfModels &tvfModels);
    std::map<std::string, TvfFuncCreatorR *> getTvfNameToCreator() {
        return _tvfNameToCreator;
    }

private:
    bool initBuiltinFactory();
    bool addDefaultTvfInfo();
    bool initPluginFactory(const SqlTvfPluginConfig &tvfPluginConfig);
    bool initTvfProfile(const SqlTvfProfileInfos &sqlTvfProfileInfos);
    bool addFunctions(TvfFuncCreatorFactory *factory);
    bool initTvfFuncCreator();

private:
    std::vector<TvfFuncCreatorFactoryPtr> _builtinFactoryVec;
    std::vector<std::pair<std::string, TvfFuncCreatorFactory *>> _pluginsFactory;
    std::map<std::string, TvfFuncCreatorR *> _funcToCreator;
    std::map<std::string, TvfFuncCreatorR *> _tvfNameToCreator;
    TvfResourceContainer _resourceContainer;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<TvfFuncManager> TvfFuncManagerPtr;
} // namespace sql
} // namespace isearch
