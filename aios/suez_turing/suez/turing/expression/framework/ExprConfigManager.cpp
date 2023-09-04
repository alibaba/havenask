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
#include "suez/turing/expression/framework/ExprConfigManager.h"

#include <iosfwd>
#include <utility>

#include "autil/Log.h"

using namespace std;

AUTIL_DECLARE_AND_SETUP_LOGGER(expression, ExprConfigManager);

namespace suez {
namespace turing {

ExprConfigManager::ExprConfigManager() {}

ExprConfigManager::~ExprConfigManager() {}

bool ExprConfigManager::init(const std::vector<ExprConfig> &exprConfigs) {
    for (auto &exprConfig : exprConfigs) {
        for (auto &item : exprConfig.namedExprs) {
            auto it = _namedExprs.find(item.first);
            if (it != _namedExprs.end()) {
                return false;
            }
            _namedExprs[item.first] = item.second;
        }
    }
    return true;
}

const std::string &ExprConfigManager::getExpr(const std::string &key) {
    auto it = _namedExprs.find(key);
    if (it != _namedExprs.end()) {
        return it->second;
    }
    static std::string empty;
    return empty;
}

} // namespace turing
} // namespace suez
