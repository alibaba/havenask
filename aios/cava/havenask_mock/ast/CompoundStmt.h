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

#include "cava/ast/Stmt.h"

namespace cava {

class CompoundStmt : public Stmt
{
public:
    CompoundStmt(std::vector<Stmt *> *stmts)
        : Stmt(SK_NONE)
        , _stmts(stmts)
    {
    }
    ~CompoundStmt() {}
private:
    CompoundStmt(const CompoundStmt &);
    CompoundStmt& operator=(const CompoundStmt &);
public:
    std::string toString() override {
        return "";
    }
public:
    std::vector<Stmt *> &getStmts() { return *_stmts; }
public:
    void append(Stmt *stmt) {
    }
private:
    std::vector<Stmt *> *_stmts;
};

} // end namespace cava
