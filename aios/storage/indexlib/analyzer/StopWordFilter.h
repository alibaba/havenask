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
#include <set>
#include <string>

#include "autil/Log.h"

namespace indexlibv2 { namespace analyzer {

class StopWordFilter
{
public:
    explicit StopWordFilter(const std::set<std::string>& stopWords);
    StopWordFilter() = default;
    virtual ~StopWordFilter() = default;

    bool isStopWord(const std::string& word);
    void setStopWords(const std::set<std::string>& stopWords);

private:
    std::set<std::string> _stopWords;

    AUTIL_LOG_DECLARE();
};

using StopWordFilterPtr = std::shared_ptr<StopWordFilter>;

}} // namespace indexlibv2::analyzer
