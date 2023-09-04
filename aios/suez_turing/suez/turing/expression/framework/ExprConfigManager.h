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
#include <string>
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "suez/turing/expression/common.h"

namespace suez {
namespace turing {

class ExprConfig : public autil::legacy::Jsonizable {
public:
    ExprConfig() {}
    ~ExprConfig() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) { json.Jsonize("expressions", namedExprs, namedExprs); }

public:
    std::map<std::string, std::string> namedExprs;
};

class ExprConfigManager {
public:
    ExprConfigManager();
    ~ExprConfigManager();

private:
    ExprConfigManager(const ExprConfigManager &);
    ExprConfigManager &operator=(const ExprConfigManager &);

public:
    bool init(const std::vector<ExprConfig> &exprConfigs);
    const std::string &getExpr(const std::string &key);

private:
    std::map<std::string, std::string> _namedExprs;
};

SUEZ_TYPEDEF_PTR(ExprConfigManager);

} // namespace turing
} // namespace suez
