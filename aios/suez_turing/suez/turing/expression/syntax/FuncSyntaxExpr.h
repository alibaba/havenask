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

#include "autil/Log.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"

namespace autil {
class DataBuffer;
} // namespace autil

namespace suez {
namespace turing {
class SyntaxExprVisitor;

class FuncSyntaxExpr : public SyntaxExpr {
public:
    typedef std::vector<SyntaxExpr *> SubExprType;

public:
    FuncSyntaxExpr(const std::string &funcName, const SubExprType &param = SubExprType());
    ~FuncSyntaxExpr();

public:
    const std::string &getFuncName() const { return _funcName; }
    const SubExprType &getParam() const { return _exprs; }
    virtual std::string getExprString() const;
    void addParam(SyntaxExpr *expr);

public:
    bool operator==(const SyntaxExpr *expr) const;
    void accept(SyntaxExprVisitor *visitor) const;

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);

protected:
    std::string _funcName;
    SubExprType _exprs;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
