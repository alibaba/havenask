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
#include "ha3/sql/ops/tvf/TvfFuncCreatorFactory.h"

#include <iosfwd>
#include <map>
#include <utility>

#include "alog/Logger.h"
#include "ha3/sql/ops/tvf/TvfFuncCreatorR.h"

namespace isearch {
namespace sql {
class TvfResourceContainer;
} // namespace sql
} // namespace isearch

using namespace std;
using namespace iquan;
namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, TvfFuncCreatorFactory);

TvfFuncCreatorFactory::TvfFuncCreatorFactory() {}

TvfFuncCreatorFactory::~TvfFuncCreatorFactory() {
    for (auto &iter : _funcCreatorMap) {
        delete iter.second;
    }
}

bool TvfFuncCreatorFactory::init(const KeyValueMap &parameters) {
    return true;
}

bool TvfFuncCreatorFactory::init(
    const KeyValueMap &parameters,
    const build_service::config::ResourceReaderPtr &resourceReaderPtr) {
    return init(parameters);
}

bool TvfFuncCreatorFactory::init(const KeyValueMap &parameters,
                                 const build_service::config::ResourceReaderPtr &resourceReaderPtr,
                                 TvfResourceContainer *resourceContainer) {
    return init(parameters, resourceReaderPtr);
}

std::map<std::string, TvfFuncCreatorR *> &TvfFuncCreatorFactory::getTvfFuncCreators() {
    return _funcCreatorMap;
}

TvfFuncCreatorR *TvfFuncCreatorFactory::getTvfFuncCreator(const std::string &name) {
    auto it = _funcCreatorMap.find(name);
    if (it != _funcCreatorMap.end()) {
        return it->second;
    }
    return nullptr;
}

bool TvfFuncCreatorFactory::addTvfFunctionCreator(TvfFuncCreatorR *creator) {
    if (creator == nullptr) {
        return false;
    }
    const std::string &name = creator->getName();
    if (_funcCreatorMap.find(name) != _funcCreatorMap.end()) {
        SQL_LOG(ERROR, "add tvf func [%s] creator failed, already exists.", name.c_str());
        return false;
    }
    _funcCreatorMap[name] = creator;
    return true;
}

} // namespace sql
} // namespace isearch
