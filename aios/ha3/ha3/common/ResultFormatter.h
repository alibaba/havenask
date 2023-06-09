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

#include "ha3/common/Result.h"
#include "autil/Log.h" // IWYU pragma: keep


namespace isearch {
namespace common {

class ResultFormatter
{
public:
    ResultFormatter() {};
    virtual ~ResultFormatter() {};
public:
    virtual void format(const ResultPtr &result,
                        std::stringstream &retString) = 0;
    static double getCoveredPercent(const Result::ClusterPartitionRanges &ranges);
};

} // namespace common
} // namespace isearch
