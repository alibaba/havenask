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

#include <memory>
#include <regex.h>
#include <sys/types.h>

#include "autil/ConstString.h"
#include "autil/Log.h"

namespace indexlib { namespace util {

class RegularExpression
{
public:
    RegularExpression();
    ~RegularExpression();

public:
    bool Init(const std::string& pattern);
    bool Match(const std::string& string) const;
    bool Match(const autil::StringView& string) const;
    std::string GetLatestErrMessage() const;

    static bool CheckPattern(const std::string& pattern);
    static bool Match(const std::string& pattern, const std::string& str);
    static bool Match(const std::string& pattern, const std::string& str, std::string& errorMsg);

private:
    regex_t _regex;
    mutable int _latestErrorCode;
    volatile bool _initialized;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<RegularExpression> RegularExpressionPtr;
typedef std::vector<RegularExpressionPtr> RegularExpressionVector;
}} // namespace indexlib::util
