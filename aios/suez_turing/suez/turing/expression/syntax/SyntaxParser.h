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

#include <stddef.h>
#include <string>

#include "autil/Log.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"

namespace suez {
namespace turing {

class SyntaxParser {
public:
    class ParseState {
    public:
        ParseState() : _syntax(NULL) {}
        ~ParseState() {
            if (_syntax) {
                delete _syntax;
                _syntax = NULL;
            }
        }
        ParseState(const ParseState &) = delete;
        ParseState &operator=(const ParseState &) = delete;
        ParseState(ParseState &&rhs);
        ParseState &operator=(ParseState rhs);

    public:
        void fail(const std::string &error) { _errorMsg = error; }

        bool hasError() const { return !_errorMsg.empty(); }
        SyntaxExpr *stealSyntax() {
            SyntaxExpr *syntax = _syntax;
            _syntax = NULL;
            return syntax;
        }
        SyntaxExpr *getSyntax() { return _syntax; }
        void setSyntax(SyntaxExpr *syntax) { _syntax = syntax; }
        const std::string &getErrorMsg() const { return _errorMsg; }

    private:
        SyntaxExpr *_syntax;
        std::string _errorMsg;
    };

public:
    SyntaxParser();
    ~SyntaxParser();

private:
    SyntaxParser(const SyntaxParser &);
    SyntaxParser &operator=(const SyntaxParser &);

public:
    static ParseState parseSyntax(const char *syntaxStr, size_t size);
    // helper function
    static SyntaxExpr *parseSyntax(const std::string &syntaxStr);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
