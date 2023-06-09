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

#include <string>
#include <vector>

#include "cava/ast/ASTContext.h"
#include "cava/ast/ASTNode.h"
#include "cava/ast/AmbExpr.h"
#include "cava/ast/AmbTypeNode.h"
#include "cava/ast/CallExpr.h"
#include "cava/ast/CanonicalTypeNode.h"
#include "cava/ast/AssignExpr.h"
#include "cava/ast/ClassDecl.h"
#include "cava/ast/Expr.h"
#include "cava/ast/ExprStmt.h"
#include "cava/ast/CompoundStmt.h"
#include "cava/ast/FieldDecl.h"
#include "cava/ast/ClassBodyDecl.h"
#include "cava/ast/ReturnStmt.h"
#include "cava/ast/Formal.h"
#include "cava/ast/LiteralExpr.h"
#include "cava/ast/LocalVarDeclStmt.h"
#include "cava/ast/MethodDecl.h"
#include "cava/ast/Import.h"
#include "cava/ast/Modifier.h"
#include "cava/ast/Name.h"
#include "cava/ast/Package.h"
#include "cava/ast/Stmt.h"
#include "cava/ast/TypeNode.h"

namespace cava {

class NodeFactory
{
public:
    NodeFactory() {}
    ~NodeFactory() {}
private:
    NodeFactory(const NodeFactory &);
    NodeFactory& operator=(const NodeFactory &);
public:


#define DECARE_CREATE_NODE_FUNC(T, ...)                                 \
    static T *create##T(ASTContext &astCtx, Location &location,         \
                        ##__VA_ARGS__)



    // createPackage
    DECARE_CREATE_NODE_FUNC(Package,
                            Name *) {
        return nullptr;
    }


    // createModifier
    DECARE_CREATE_NODE_FUNC(Modifier){
        return nullptr;
    }


    // createFormal
    DECARE_CREATE_NODE_FUNC(Formal,
                            TypeNode *,
                            VarDecl *){
        return nullptr;
    }

    // createVarDecl
    DECARE_CREATE_NODE_FUNC(VarDecl,
                            std::string *){
        return nullptr;
    }

    // createName
    DECARE_CREATE_NODE_FUNC(Name,
                            Name *,
                            std::string *){
        return nullptr;
    }

    // createCanonicalTypeNode
    DECARE_CREATE_NODE_FUNC(CanonicalTypeNode,
                            CanonicalTypeNode::CanonicalType){
        return nullptr;
    }

    // createAmbTypeNode
    DECARE_CREATE_NODE_FUNC(AmbTypeNode,
                            Name *){
        return nullptr;
    }


    // createClassDecl
    DECARE_CREATE_NODE_FUNC(ClassDecl,
                            Modifier *,
                            std::string *,
                            ClassBodyDecl *){
        return nullptr;
    }

    // createClassBodyDecl
    DECARE_CREATE_NODE_FUNC(ClassBodyDecl){
        return nullptr;
    }

    // createFieldDecl
    DECARE_CREATE_NODE_FUNC(FieldDecl,
                            Modifier *,
                            TypeNode *,
                            VarDecl *) {
        return nullptr;
    }

    // createMethodDecl
    DECARE_CREATE_NODE_FUNC(MethodDecl,
                            Modifier *,
                            std::string *,
                            std::vector<Formal *> *,
                            TypeNode *){
        return nullptr;
    }

    // createAmbExpr
    DECARE_CREATE_NODE_FUNC(AmbExpr,
                            Name *){
        return nullptr;
    }

    // createTypedLiteralExpr
    template<typename Type>
    static LiteralExpr *createTypedLiteralExpr(
            ASTContext &astCtx, Location &location,
            LiteralExpr::LiteralType, Type){
        return nullptr;
    }

    template<std::string* >
    static LiteralExpr *createTypedLiteralExpr(
            ASTContext &astCtx, Location &location,
            LiteralExpr::LiteralType, std::string*){
        return nullptr;
    }

    template<int>
    static LiteralExpr *createTypedLiteralExpr(
            ASTContext &astCtx, Location &location,
            LiteralExpr::LiteralType, int){
        return nullptr;
    }

    template<bool>
    static LiteralExpr *createTypedLiteralExpr(
            ASTContext &astCtx, Location &location,
            LiteralExpr::LiteralType, bool){
        return nullptr;
    }


    // createCallExpr
    DECARE_CREATE_NODE_FUNC(CallExpr,
                            Expr *,
                            std::string *,
                            std::vector<Expr *> *){
        return nullptr;
    }

    // createLocalVarDeclStmt
    DECARE_CREATE_NODE_FUNC(LocalVarDeclStmt,
                            TypeNode *,
                            VarDecl *){
        return nullptr;
    }
    
    // createCompoundStmt
    DECARE_CREATE_NODE_FUNC(CompoundStmt,
                            std::vector<Stmt *> *){
        return nullptr;
    }

    // createExprStmt
    DECARE_CREATE_NODE_FUNC(ExprStmt,
                            Expr *){
        return nullptr;
    }

    // createReturnStmt
    DECARE_CREATE_NODE_FUNC(ReturnStmt,
                            Expr *){
        return nullptr;
    }

    // createAssignExpr
    DECARE_CREATE_NODE_FUNC(AssignExpr,
                            Expr *,
                            AssignExpr::AssignType,
                            Expr *){
        return nullptr;
    }
       
    // createImport
    DECARE_CREATE_NODE_FUNC(Import,
                            Name *,
                            Import::ImportType){
        return nullptr;
    }

public:
    static AmbTypeNode *allocAmbTypeNode(ASTContext &astCtx,
            Name *name,
            std::vector<TypeNode *> &typeInsts) {
                return nullptr;
            }
    static Modifier *allocModifier(ASTContext &astCtx) {
        return nullptr;
    }
    static DeclId allocVarDecl(ASTContext &astCtx,
                                std::string *field,
                                CGClassInfo *classInfo) {
        return nullptr;
    }
};

} // end namespace cava
