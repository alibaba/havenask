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
#include <stddef.h>
#include <string>
#include <tuple>

#include "autil/Log.h"
#include "cava/ast/StmtExprVisitor.h"
#include "cava/plugin/ASTRewriter.h"
#include "suez/turing/expression/common.h"

namespace cava {
class ASTContext;
class CallExpr;
class ClassDecl;
class LocalVarDeclStmt;
class MethodDecl;
} // namespace cava

namespace suez {
namespace turing {
class GenRule;
}
} // namespace suez

namespace suez {
namespace turing {

using UdfModuleTuple = std::tuple<::cava::ClassDecl *, ::cava::MethodDecl *, GenRule *>;
using UdfModuleMap = std::map<std::string, UdfModuleTuple>;

class UdfModuleManager {
public:
    UdfModuleManager() {}
    ~UdfModuleManager() {}

private:
    UdfModuleManager(const UdfModuleManager &);
    UdfModuleManager &operator=(const UdfModuleManager &);

public:
    bool init() { return true; }

    bool addUdfModule(const std::string &methodName,
                      ::cava::ClassDecl *classDecl,
                      ::cava::MethodDecl *methodDecl,
                      GenRule *genRule);
    const UdfModuleMap &getUdfModuleMap() const { return _udfModsMap; }

private:
    UdfModuleMap _udfModsMap;
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(UdfModuleManager);

} // namespace turing
} // namespace suez
