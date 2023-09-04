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
#include <unordered_map>

#include "autil/Log.h"
#include "cava/plugin/ASTRewriter.h"
#include "suez/turing/expression/common.h"

namespace cava {
class ASTContext;
class ClassDecl;
class MethodDecl;
} // namespace cava

namespace suez {
namespace turing {

class FunctionRewrite : public ::cava::ASTRewriter {
public:
    FunctionRewrite() : _astCtx(NULL), _methodDecl(NULL), _classDecl(NULL) {}
    ~FunctionRewrite() {}

private:
    FunctionRewrite(const FunctionRewrite &);
    FunctionRewrite &operator=(const FunctionRewrite &);

public:
    bool init(const std::map<std::string, std::string> &parameters, const std::string &configPath) override {
        return true;
    }
    bool process(::cava::ASTContext &astCtx) override;

private:
    bool needRewriteClass(::cava::ClassDecl *classDecl);
    bool matchProcess(::cava::MethodDecl *methodDecl);
    bool addClassFields(const std::string &newMethodName);
    bool addNewMethods(const std::string &newMethodName);

private:
    ::cava::ASTContext *_astCtx;
    ::cava::MethodDecl *_methodDecl;
    ::cava::ClassDecl *_classDecl;

    static std::unordered_map<std::string, std::string> Type2MethodName;
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(FunctionRewrite);

} // namespace turing
} // namespace suez
