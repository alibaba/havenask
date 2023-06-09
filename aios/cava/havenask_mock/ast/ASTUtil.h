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

#include <functional>
#include <string>
#include <vector>



namespace cava {
class ASTContext;
class CanonicalTypeNode;
class ClassDecl;
class Expr;
class ForStmt;
class MethodDecl;
class TypeNode;

class ASTUtil
{
private:
    ASTUtil() {}
    ~ASTUtil() {}
private:
    ASTUtil(const ASTUtil &);
    ASTUtil& operator=(ASTUtil &);
public:
    typedef std::function<bool(ClassDecl*)> ClassMatcher;
    static void getClassDecls(ASTContext &astCtx, ClassMatcher matcher, std::vector<ClassDecl*>& classDecls) {
        
    }
    // match name and return type and modifier
    static bool matchMethod(MethodDecl *methodDecl, const std::string &funcName,
                            CanonicalTypeNode &typeNode, bool isStatic) {
                                return false;
                            }

    static ForStmt* createForStmt(ASTContext &astCtx,
                                  Expr* conditionEndExpr,
                                  const std::string &indexVarName = "i") {
        return nullptr;
    }
    static std::vector<MethodDecl *> getMethodDecls(ClassDecl* classDecl,
            const std::string& name) {
                std::vector<MethodDecl *> ret;
    return ret;
            }
    static std::string getTypeShortName(TypeNode *typeNode) {
        return "";
    }
    static bool hasNativeFunc(ClassDecl *classDecl) {
        return false;
    }
    static bool hasNativeFunc(ASTContext &astCtx) {
        return false;
    }
};

} // end namespace cava
