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
#include "autil/StringTokenizer.h"
#include "autil/Log.h"

using namespace std;
namespace autil {
AUTIL_DECLARE_AND_SETUP_LOGGER(autil, StringTokenizer);

StringTokenizer::StringTokenizer(const string& str,
                                 const string& sep, Option opt) {
    tokenize(str, sep, opt);
}

StringTokenizer::StringTokenizer() {
}

StringTokenizer::~StringTokenizer() {
}

size_t StringTokenizer::tokenize(const string& str,
                                 const string& sep, Option opt) {
    m_tokens = tokenize(StringView(str), sep, opt);
    return m_tokens.size();
}

std::vector<StringView> StringTokenizer::constTokenize(
        const StringView& str, const std::string& sep, Option opt)
{
    vector<StringView> ret;
    tokenize(str, sep, ret, opt);
    return ret;
}

std::vector<StringView> StringTokenizer::constTokenize(
        const StringView& str, char c, Option opt)
{
    vector<StringView> ret;
    tokenize(str, c, ret, opt);
    return ret;
}

std::vector<std::string> StringTokenizer::tokenize(
        const StringView& str, const std::string &sep, Option opt)
{
    vector<string> ret;
    tokenize(str, sep, ret, opt);
    return ret;
}

std::vector<std::string> StringTokenizer::tokenize(
        const StringView& str, char c, Option opt)
{
    vector<string> ret;
    tokenize(str, c, ret, opt);
    return ret;
}
}
