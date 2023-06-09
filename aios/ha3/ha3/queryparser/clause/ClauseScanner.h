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

#include <sstream>

#include "autil/Log.h" // IWYU pragma: keep

class ClauseFlexLexer;

#ifndef __FLEX_LEXER_H
#define yyFlexLexer ClauseFlexLexer
#include <FlexLexer.h>

#undef yyFlexLexer
#endif

#include "ha3/queryparser/ClauseBisonParser/ClauseBisonParser.hh"

namespace isearch {
namespace queryparser {
class ClauseScanner : public ClauseFlexLexer
{
public:
    ClauseScanner(std::istream *input, std::ostream *output);
    virtual ~ClauseScanner();
public:
    typedef isearch_bison::ClauseBisonParser::token token;
    typedef isearch_bison::ClauseBisonParser::token_type token_type;
    typedef isearch_bison::ClauseBisonParser::semantic_type semantic_type;
    typedef isearch_bison::ClauseBisonParser::location_type location_type;
public:
    virtual token_type lex(semantic_type* yylval, location_type* yylloc);
public:
    void setDebug(bool debug);
private:
    std::ostringstream oss;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace queryparser
} // namespace isearch
