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

#ifndef YY_DECL
#define YY_DECL                                                                                    \
    isearch_sp::SPQueryParser::token_type sql::SPQueryScanner::lex(                                \
        isearch_sp::SPQueryParser::semantic_type *yylval,                                          \
        isearch_sp::SPQueryParser::location_type *yylloc)
#endif

#ifndef __FLEX_LEXER_H
#define yyFlexLexer Ha3SPQueryFlexLexer
#include <FlexLexer.h>
#endif

#include <iosfwd>

#include "autil/Log.h"
#include "sql/ops/scan/udf_to_query/SpQueryParser.hh"

class Ha3SPQueryFlexLexer;

namespace sql {

class SPQueryScanner : public Ha3SPQueryFlexLexer {
public:
    SPQueryScanner(std::istream *yyin = 0, std::ostream *yyout = 0);
    ~SPQueryScanner();

public:
    virtual isearch_sp::SPQueryParser::token_type
    lex(isearch_sp::SPQueryParser::semantic_type *yylval,
        isearch_sp::SPQueryParser::location_type *yylloc);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace sql
