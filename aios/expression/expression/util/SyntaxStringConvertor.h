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
#ifndef ISEARCH_EXPRESSION_SYNTAXSTRINGCONVERTOR_H
#define ISEARCH_EXPRESSION_SYNTAXSTRINGCONVERTOR_H

#include "expression/common.h"

namespace expression {

class SyntaxStringConvertor
{
public:
    SyntaxStringConvertor();
    ~SyntaxStringConvertor();
private:
    SyntaxStringConvertor(const SyntaxStringConvertor &);
    SyntaxStringConvertor& operator=(const SyntaxStringConvertor &);
    
public:
    static std::string encodeEscapeString(const std::string& exprStr)
    {
        std::string ret;
        ret.reserve(exprStr.size() + 8);
        ret += "\"";
        for (size_t i = 0; i < exprStr.size(); ++i) {
            char ch = exprStr[i];
            if (ch == '\\' || ch == '\"') {
                ret += '\\';
            }
            ret += ch;
        }
        ret += "\"";
        return ret;
    }

    // "abc" -> abc
    // "a\"bc" -> a"bc
    // "a\\bc" -> a\bc
    static std::string decodeEscapeString(const std::string& exprStr)
    {
        std::string ret;
        ret.reserve(exprStr.size());
        
        size_t i = 0;
        size_t len = exprStr.size();
        if (exprStr.size() > 1 && *exprStr.begin() == '\"' && *exprStr.rbegin() == '\"')
        {
            i = 1;
            len = len - 1;
        }
                   
        for (; i < len; ++i) {
            if (exprStr[i] == '\\') {
                ++i;
            }
            ret += exprStr[i];
        }
        return ret;
    }
};

}

#endif //ISEARCH_EXPRESSION_SYNTAXSTRINGCONVERTOR_H
