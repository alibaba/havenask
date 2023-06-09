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

#include "suez/turing/expression/syntax/SyntaxExpr.h"

#include "suez/turing/expression/syntax/AtomicSyntaxExpr.h"
#include "suez/turing/expression/syntax/BinarySyntaxExpr.h"
#include "suez/turing/expression/syntax/FuncSyntaxExpr.h"
#include "suez/turing/expression/syntax/RankSyntaxExpr.h"


namespace isearch {
namespace queryparser {

typedef suez::turing::SyntaxExpr SyntaxExpr; 
typedef suez::turing::SyntaxExprType SyntaxExprType; 

typedef suez::turing::AtomicSyntaxExpr AtomicSyntaxExpr; 
typedef suez::turing::BinarySyntaxExpr BinarySyntaxExpr; 
typedef suez::turing::FuncSyntaxExpr FuncSyntaxExpr; 
typedef suez::turing::RankSyntaxExpr RankSyntaxExpr; 

} // namespace queryparser
} // namespace isearch



