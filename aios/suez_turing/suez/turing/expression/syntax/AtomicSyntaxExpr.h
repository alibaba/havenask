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

#include "autil/Log.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"

namespace autil {
class DataBuffer;
} // namespace autil

namespace suez {
namespace turing {
class SyntaxExprVisitor;

enum AtomicSyntaxExprType {
    UNKNOWN,
    INTEGER_VALUE, // int64_t
    FLOAT_VALUE,   // double
    STRING_VALUE,
    ATTRIBUTE_NAME,
    // support more exect value type
    INT8_VT,
    UINT8_VT,
    INT16_VT,
    UINT16_VT,
    INT32_VT,
    UINT32_VT,
    UINT64_VT,
    FLOAT_VT,
    INT64_VT = INTEGER_VALUE,
    DOUBLE_VT = FLOAT_VALUE,
    STRING_VT = STRING_VALUE
};

class AtomicSyntaxExpr : public SyntaxExpr {
public:
    AtomicSyntaxExpr(const std::string &exprStr,
                     ExprResultType resultType = vt_unknown,
                     AtomicSyntaxExprType atomicSyntaxExprType = UNKNOWN,
                     const std::string &prefixStr = "");
    ~AtomicSyntaxExpr();

public:
    virtual bool operator==(const SyntaxExpr *expr) const;
    void accept(SyntaxExprVisitor *visitor) const;

    AtomicSyntaxExprType getAtomicSyntaxExprType() const { return _atomicSyntaxExprType; }
    void setAtomicSyntaxExprType(AtomicSyntaxExprType atomicSyntaxExprType) {
        _atomicSyntaxExprType = atomicSyntaxExprType;
    }

    std::string getPrefixString() const { return _prefixString; }
    std::string getValueString() const { return _valueString; }

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);

private:
    AtomicSyntaxExprType _atomicSyntaxExprType;
    std::string _valueString;
    std::string _prefixString;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
