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

#include "autil/Log.h"
#include "cava/plugin/ASTRewriter.h"
#include "suez/turing/expression/common.h"

namespace cava {
class ASTContext;
class CompoundStmt;
class MethodDecl;
} // namespace cava

namespace suez {
namespace turing {

class AddBatchScore : public ::cava::ASTRewriter {
public:
    AddBatchScore();
    ~AddBatchScore();

private:
    AddBatchScore(const AddBatchScore &);
    AddBatchScore &operator=(const AddBatchScore &);

public:
    bool init(const std::map<std::string, std::string> &parameters, const std::string &configPath) override {
        return true;
    }
    bool process(::cava::ASTContext &astCtx) override;
    ::cava::MethodDecl *genBatchProcessDecl(::cava::ASTContext &astCtx);
    ::cava::CompoundStmt *genBatchProcessBody(::cava::ASTContext &astCtx);

private:
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(AddBatchScore);

} // namespace turing
} // namespace suez
