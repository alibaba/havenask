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

namespace cava {
class AmbTypeNode;
class CanonicalTypeNode;
class ClassDecl;
class Formal;
class MethodDecl;
class TypeNode;
} // namespace cava

using ::cava::AmbTypeNode;
using ::cava::ClassDecl;
using ::cava::Formal;
using ::cava::MethodDecl;
using ::cava::TypeNode;

namespace suez {
namespace turing {

class CavaASTUtil {
public:
    CavaASTUtil();
    ~CavaASTUtil();

private:
    CavaASTUtil(const CavaASTUtil &);
    CavaASTUtil &operator=(const CavaASTUtil &);

public:
    static bool functionMatcher(::cava::ClassDecl *classDecl);
    static bool scorerMatcher(::cava::ClassDecl *classDecl);
    static bool summaryMatcher(::cava::ClassDecl *classDecl);
    static bool featureLibMatcher(::cava::ClassDecl *classDecl);
    static bool matchCreateMethod(::cava::MethodDecl *methodDecl, const std::string &className);

private:
    static bool checkFormal(const std::string &shortClassName, ::cava::Formal *formal);
    static bool matcher(::cava::ClassDecl *classDecl,
                        ::cava::CanonicalTypeNode &initResultType,
                        const std::string &initFirstParam,
                        const std::string &processFirstParam);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
