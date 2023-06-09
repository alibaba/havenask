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
#include "expression/syntax/SyntaxParser.h"
#include "expression/syntax/Scanner.h"
using namespace std;

namespace expression {
AUTIL_LOG_SETUP(expression, SyntaxParser);

SyntaxParser::SyntaxParser() {
}

SyntaxParser::~SyntaxParser() {
}

SyntaxParser::ParseState SyntaxParser::parseSyntax(const char *syntaxStr, size_t size, bool enableSyntaxParseOpt) {
    istringstream iss(syntaxStr);
    Scanner scanner(&iss, &cout);
    ParseState state;
    syntax_bison::BisonParser parser(scanner, state, enableSyntaxParseOpt);
    if (parser.parse() != 0) {
        AUTIL_LOG(DEBUG, "parse failed");
    }
    return state;
}

SyntaxExpr *SyntaxParser::parseSyntax(const string &syntaxStr, bool enableSyntaxParseOpt) {
    SyntaxParser::ParseState ps = parseSyntax(syntaxStr.c_str(), syntaxStr.length(), enableSyntaxParseOpt);
    if (ps.hasError()) {
        AUTIL_LOG(WARN, "parse [%s] failed, error: [%s]",
                  syntaxStr.c_str(), ps.getErrorMsg().c_str());
        return NULL;
    } else {
        return ps.stealSyntax();
    }
}

}
