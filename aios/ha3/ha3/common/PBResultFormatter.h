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

#include <stdint.h>
#include <map>
#include <sstream>
#include <string>
#include <memory>
#include <vector>

#include "autil/DataBuffer.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"

#include "ha3/common/AggregateResult.h"
#include "ha3/common/Result.h"
#include "ha3/common/ResultFormatter.h"
#include "ha3/common/SortExprMeta.h"
#include "ha3/common/PBResult.pb.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace common {
class AttributeItem;
class Hits;
class Hit;
class Result;

typedef std::shared_ptr<Hit> HitPtr;
typedef std::shared_ptr<Result> ResultPtr;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace common {

class PBResultFormatter : public ResultFormatter
{
public:
    const static uint32_t SUMMARY_IN_BYTES = PBResultType::SUMMARY_IN_BYTES;
    const static uint32_t PB_MATCHDOC_FORMAT = PBResultType::PB_MATCHDOC_FORMAT;
public:
    PBResultFormatter();
    ~PBResultFormatter();
public:
    void format(const ResultPtr &result, std::stringstream &ss);

    void format(const ResultPtr &result, PBResult &pbResult);

public:
    void setOption(uint32_t option) {
        _option = option;
    }
    void setAttrNameTransMap(const std::map<std::string, std::string>& attrNameMap) {
        _attrNameMap = attrNameMap;
    }
private:
    void formatHits(const ResultPtr &resultPtr, PBResult &pbResult);
    void formatMetaHits(const Hits *hits, PBHits *pbHits);
    void formatMeta(const ResultPtr &resultPtr, PBResult &pbResult);
    void formatHit(const HitPtr &hitPtr, PBHit *pbHit, bool isIndependentQuery);
    void formatMatchDocs(const ResultPtr &resultPtr, PBResult &pbResult);
    void formatSummary(const HitPtr &hitPtr, PBHit *pbHit);
    void formatProperty(const HitPtr &hitPtr, PBHit *pbHit);
    void formatAttributes(const HitPtr &hitPtr, PBHit *pbHit);
    void formatVariableValues(const HitPtr &hitPtr, PBHit *pbHit);
    void formatAggregateResults(const ResultPtr &resultPtr,
                                PBResult &pbResult);
    void formatAggregateResult(const AggregateResultPtr &aggResultPtr,
                               PBAggregateResults &pbAggregateResults);
    void formatErrorResult(const ResultPtr &resultPtr,
                           PBResult &pbResult);
    void formatTotalTime(const ResultPtr &resultPtr,
                         PBResult &pbResult);
    void formatRequestTracer(const ResultPtr &resultPtr,
                              PBResult &pbResult);
    void formatPBMatchDocVariableReferences(const matchdoc::ReferenceVector &refVec,
            const std::vector<matchdoc::MatchDoc> &matchDocs,
            PBMatchDocs *pbMatchDocs,
            ValueType valueType);
    void formatPBMatchDocSortValues(const std::vector<SortExprMeta> &sortExprMetas,
                                    const std::vector<matchdoc::MatchDoc> &matchDocs,
                                    PBMatchDocs *pbMatchDocs);
    template<typename T>
    double readNumericData2Double(const matchdoc::ReferenceBase *ref,
                                  const matchdoc::MatchDoc matchDoc) const;
private:
    void fillTypedValue(const std::string &key,
                        const AttributeItem *attrItem,
                        PBAttrKVPair *value);
private:
    uint32_t _option;
    autil::DataBuffer _dataBuffer;
    std::map<std::string, std::string> _attrNameMap;
private:
    friend class PBResultFormatterTest;

private:
    AUTIL_LOG_DECLARE();
};

template<typename T>
inline double PBResultFormatter::readNumericData2Double(
        const matchdoc::ReferenceBase *ref,
        const matchdoc::MatchDoc matchDoc) const
{
    auto typedRef = static_cast<const matchdoc::Reference<T> *>(ref);
    return typedRef->getReference(matchDoc);
}

} // namespace common
} // namespace isearch
