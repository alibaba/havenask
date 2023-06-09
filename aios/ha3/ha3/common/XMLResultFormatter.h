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
#include <string>

#include "ha3/common/AggregateResult.h"
#include "ha3/common/Hit.h"
#include "ha3/common/Result.h"
#include "ha3/common/ResultFormatter.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace common {
class ErrorResult;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace common {

class XMLResultFormatter : public ResultFormatter
{
public:
    XMLResultFormatter();
    ~XMLResultFormatter();
public:
    void format(const ResultPtr &result,
                       std::stringstream &ss);
    static std::string xmlFormatResult(const ResultPtr &result);
    static std::string xmlFormatErrorResult(const ErrorResult &error);
    static void addCacheInfo(std::string &resultString, bool isFromCache);

private:
    static void fillHeader(std::stringstream &ss);

    static void formatHits(const ResultPtr &resultPtr, std::stringstream &ss);
    static void formatMeta(const ResultPtr &resultPtr, std::stringstream &ss);
    static void formatSortExpressionMeta(const ResultPtr &resultPtr,
                                  std::stringstream &ss);
    static void formatAggregateResults(const ResultPtr &resultPtr,
                                std::stringstream &ss);
    static void formatAggregateResult(const AggregateResultPtr &aggResultPtr,
                               std::stringstream &ss);

    static void formatErrorResult(const ResultPtr &resultPtr,
                                  std::stringstream &ss);

    static void formatTotalTime(const ResultPtr &resultPtr,
                                std::stringstream &ss);

    static void formatRequestTracer(const ResultPtr &resultPtr,
                             std::stringstream &ss);

    static void formatAttributeMap(const AttributeMap &attributeMap,
                                   const std::string &tag,
                                   std::stringstream &ss);

    static std::string getCoveredPercentStr(const Result::ClusterPartitionRanges &ranges);

private:
    friend class XMLResultFormatterTest;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace common
} // namespace isearch


