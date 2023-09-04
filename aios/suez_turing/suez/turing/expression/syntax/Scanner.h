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

#ifndef __FLEX_LEXER_H
#include <FlexLexer.h>
#endif

#include "autil/Log.h"
#include "suez/turing/expression/syntax/parser/BisonParser.hh"

namespace suez {
namespace turing {

class Scanner : public yyFlexLexer {
public:
    Scanner(std::istream *input, std::ostream *output);
    virtual ~Scanner();

public:
    typedef suez_turing_bison::BisonParser::token token;
    typedef suez_turing_bison::BisonParser::token_type token_type;
    typedef suez_turing_bison::BisonParser::semantic_type semantic_type;
    typedef suez_turing_bison::BisonParser::location_type location_type;

public:
    virtual token_type lex(semantic_type *yylval, location_type *yylloc);
    std::string orgContext() {
        std::istringstream *iss = dynamic_cast<std::istringstream *>(yyin);
        return iss->str();
    }

private:
    std::ostringstream oss;
    int yycolumn;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
