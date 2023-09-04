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
#include "suez/turing/expression/syntax/SyntaxParser.h"

#include <iostream>
#include <utility>

#include "alog/Logger.h"
#include "suez/turing/expression/syntax/Scanner.h"
#include "suez/turing/expression/syntax/parser/BisonParser.hh"

using namespace std;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, SyntaxParser);

SyntaxParser::ParseState::ParseState(SyntaxParser::ParseState &&rhs)
    : _syntax(rhs.stealSyntax()), _errorMsg(std::move(rhs._errorMsg)) {}

SyntaxParser::ParseState &SyntaxParser::ParseState::operator=(SyntaxParser::ParseState rhs) {
    _syntax = rhs.stealSyntax();
    _errorMsg = std::move(rhs._errorMsg);
    return *this;
}

SyntaxParser::SyntaxParser() {}

SyntaxParser::~SyntaxParser() {}

SyntaxParser::ParseState SyntaxParser::parseSyntax(const char *syntaxStr, size_t size) {
    istringstream iss(syntaxStr);
    Scanner scanner(&iss, &cout);
    ParseState state;
    suez_turing_bison::BisonParser parser(scanner, state);
    if (parser.parse() != 0) {
        AUTIL_LOG(DEBUG, "parse failed");
    }
    return state;
}

SyntaxExpr *SyntaxParser::parseSyntax(const string &syntaxStr) {
    SyntaxParser::ParseState ps = parseSyntax(syntaxStr.c_str(), syntaxStr.length());
    if (ps.hasError()) {
        AUTIL_LOG(WARN, "parse [%s] failed, error: [%s]", syntaxStr.c_str(), ps.getErrorMsg().c_str());
        return NULL;
    } else {
        return ps.stealSyntax();
    }
}

} // namespace turing
} // namespace suez
