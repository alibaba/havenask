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

#include "ha3/isearch.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ResultFormatter.h"
#include "ha3/common/Hits.h"
#include "rapidjson/document.h"

namespace isearch {
namespace common {

class FullJsonResultFormatter : public ResultFormatter
{
public:
    FullJsonResultFormatter();
    ~FullJsonResultFormatter();
public:
    void format(const ResultPtr &result, std::stringstream &ss);

private:
    void formatResult(const ResultPtr &resultPtr,
                      rapidjson::Value &resultValue,
                      rapidjson::Document::AllocatorType &allocator);
    void formatGlobalInfo(const ResultPtr &resultPtr,
                          rapidjson::Value &resultValue,
                          rapidjson::Document::AllocatorType &allocator);
    void formatHits(const ResultPtr &resultPtr,
                    rapidjson::Value &resultValue,
                    rapidjson::Document::AllocatorType &allocator);
    void formatHit(const HitPtr &hitPtr,
                   rapidjson::Value &hitValue,
                   rapidjson::Document::AllocatorType &allocator);
    void formatSummaryFields(const HitPtr &hitPtr,
                             rapidjson::Value &summary,
                             rapidjson::Document::AllocatorType &allocator);
    void formatPropety(const HitPtr &hitPtr,
                       rapidjson::Value &propertyValue,
                       rapidjson::Document::AllocatorType &allocator);
    void formatAttributes(const HitPtr &hitPtr,
                          rapidjson::Value &attrsValue,
                          rapidjson::Document::AllocatorType &allocator);
    void formatVariables(const HitPtr &hitPtr,
                         rapidjson::Value &variablesValue,
                         rapidjson::Document::AllocatorType &allocator);
    void formatMetaHits(const MetaHitMap& metaHitMap,
                        rapidjson::Value &metaHitValues,
                        rapidjson::Document::AllocatorType &allocator);
    void formatSortExprValues(const HitPtr &hitPtr,
                              rapidjson::Value &sortExpValues,
                              rapidjson::Document::AllocatorType &allocator);
    void formatAttribute(const std::string& attrName,
                         const AttributeItem* attrItem,
                         rapidjson::Value &value,
                         rapidjson::Document::AllocatorType &allocator);
    void formatFieldValue(const std::string& srcStr, std::string& dstStr);

    bool consumeValidChar(const std::string &text, size_t &pos);
    bool getUnicodeChar(const std::string &utf8Str, size_t &pos, uint32_t &unicode);
    bool isValidUnicodeValue(uint32_t unicode);
    void formatAggregateResults(const ResultPtr &resultPtr,
                                rapidjson::Value &resultValue,
                                rapidjson::Document::AllocatorType &allocator);
    void formatAggregateResult(AggregateResultPtr aggResultPtr,
                               rapidjson::Value &aggResultValues,
                               rapidjson::Document::AllocatorType &allocator);
    void formatErrorResult(const ResultPtr & resultPtr, rapidjson::Document &document);
    void formatRequestTracer(const ResultPtr &resultPtr, rapidjson::Document &document);
    void formatMeta(const ResultPtr &resultPtr, rapidjson::Document &document);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FullJsonResultFormatter> FullJsonResultFormatterPtr;

} //end namespace common
} //end namespace isearch
