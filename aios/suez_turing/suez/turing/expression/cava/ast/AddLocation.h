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

class AddLocation : public ::cava::ASTRewriter, public ::cava::StmtExprVisitor {
public:
    AddLocation() : _astCtx(NULL), _methodDecl(NULL), _classDecl(NULL) {}
    ~AddLocation() {}

private:
    AddLocation(const AddLocation &);
    AddLocation &operator=(const AddLocation &);

public:
    bool init(const std::map<std::string, std::string> &parameters, const std::string &configPath) override {
        return true;
    }
    bool process(::cava::ASTContext &astCtx) override;
    void visitCallExpr(::cava::CallExpr *expr) override;
    bool handleLocalVarDeclStmt(::cava::LocalVarDeclStmt *localVarDeclStmt) override;
    void genClassMemberVar2Type();

private:
    ::cava::ASTContext *_astCtx;
    ::cava::MethodDecl *_methodDecl;
    ::cava::ClassDecl *_classDecl;
    std::map<std::string, std::string> _methodLocalVar2Type;
    std::map<std::string, std::string> _classMemberVar2Type;
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(AddLocation);

} // namespace turing
} // namespace suez
