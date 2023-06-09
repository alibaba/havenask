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
#include "suez/service/BuiltinSearchManagerList.h"

#include "autil/Log.h"
#include "suez/service/DdlServiceImpl.h"
#include "suez/service/LocalCatalog.h"
#include "suez/service/TableServiceImpl.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, BuiltinSearchManagerList);

BuiltinSearchManagerList::BuiltinSearchManagerList(bool enableLocalCatalog) {
    _builtinSearchManagers.emplace_back(std::make_unique<TableServiceImpl>());
    if (autil::EnvUtil::getEnv<bool>("enableDdlService", false)) {
        _builtinSearchManagers.emplace_back(std::make_unique<DdlServiceImpl>());
    }
    if (enableLocalCatalog) {
        _builtinSearchManagers.emplace_back(std::make_unique<LocalCatalog>());
    }
}

bool BuiltinSearchManagerList::init(const SearchInitParam &initParam) {
    for (auto &it : _builtinSearchManagers) {
        if (!it->init(initParam)) {
            return false;
        }
    }
    return true;
}

UPDATE_RESULT BuiltinSearchManagerList::update(const UpdateArgs &updateArgs,
                                               UpdateIndications &updateIndications,
                                               SuezErrorCollector &errorCollector) {
    for (auto &it : _builtinSearchManagers) {
        it->update(updateArgs, updateIndications, errorCollector);
    }
    return UR_REACH_TARGET;
}

void BuiltinSearchManagerList::stopService() {
    for (auto &it : _builtinSearchManagers) {
        it->stopService();
    }
}

void BuiltinSearchManagerList::stopWorker() {
    for (auto &it : _builtinSearchManagers) {
        it->stopWorker();
    }
}

} // namespace suez
