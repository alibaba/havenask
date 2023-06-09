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
#include "indexlib/util/RegularExpression.h"

#include "autil/CommonMacros.h"

using namespace std;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, RegularExpression);

RegularExpression::RegularExpression() : _latestErrorCode(0), _initialized(false) {}

RegularExpression::~RegularExpression()
{
    if (_initialized) {
        regfree(&_regex);
    }
}

bool RegularExpression::Init(const string& pattern)
{
    if (_initialized) {
        return false;
    }

    _initialized = true;
    _latestErrorCode = regcomp(&_regex, pattern.c_str(), REG_EXTENDED | REG_NOSUB);
    return _latestErrorCode == 0;
}

bool RegularExpression::Match(const string& string) const
{
    if (unlikely(!_initialized)) {
        return false;
    }

    int ret = regexec(&_regex, string.c_str(), 0, NULL, 0);
    _latestErrorCode = ret;
    return ret == 0;
}

bool RegularExpression::Match(const autil::StringView& string) const
{
    if (unlikely(!_initialized)) {
        return false;
    }
    int ret = regexec(&_regex, string.data(), 0, NULL, 0);
    _latestErrorCode = ret;
    return ret == 0;
}

std::string RegularExpression::GetLatestErrMessage() const
{
    if (!_initialized) {
        return std::string("Not Call Init");
    }
    static char errorMessage[1024] = {0};
    size_t len = regerror(_latestErrorCode, &_regex, errorMessage, sizeof(errorMessage));
    len = len < sizeof(errorMessage) ? len : sizeof(errorMessage) - 1;
    errorMessage[len] = '\0';
    return std::string(errorMessage);
}

bool RegularExpression::Match(const string& pattern, const string& str)
{
    string errMsg;
    return Match(pattern, str, errMsg);
}

bool RegularExpression::Match(const string& pattern, const string& str, string& errMsg)
{
    if (pattern.empty()) {
        return str.empty();
    }
    RegularExpression exp;
    bool ret = exp.Init(pattern) && exp.Match(str);
    errMsg = ret ? string("") : exp.GetLatestErrMessage();
    return ret;
}

bool RegularExpression::CheckPattern(const string& pattern)
{
    assert(!pattern.empty());
    RegularExpression exp;
    return exp.Init(pattern);
}
}} // namespace indexlib::util
