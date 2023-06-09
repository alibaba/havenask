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
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep

namespace autil {
class DataBuffer;
}  // namespace autil
namespace isearch {
namespace common {
class AggFunDescription;
class FilterClause;
}  // namespace common
}  // namespace isearch
namespace suez {
namespace turing {
class SyntaxExpr;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace common {

enum class AggSortType {
    AggSortByKey,
    AggSortByCount
};

class AggregateDescription
{
public:
    AggregateDescription();
    AggregateDescription(const std::string &originalString);
    ~AggregateDescription();
public:
    suez::turing::SyntaxExpr* getGroupKeyExpr() const;
    void setGroupKeyExpr(suez::turing::SyntaxExpr*);

    const std::vector<AggFunDescription*>& getAggFunDescriptions() const;
    void setAggFunDescriptions(const std::vector<AggFunDescription*>&);
    void appendAggFunDescription(AggFunDescription*);

    bool isRangeAggregate() const;
    const std::vector<std::string>& getRange() const;
    void setRange(const std::vector<std::string>&);

    uint32_t getMaxGroupCount() const;
    void setMaxGroupCount(int32_t);
    void setMaxGroupCount(const std::string &);

    //filter used in aggregate process
    FilterClause* getFilterClause() const;
    void setFilterClause(FilterClause*);

    std::string getOriginalString() const;
    void setOriginalString(const std::string &originString);

    uint32_t getAggThreshold() const;
    void setAggThreshold(uint32_t aggThreshold);
    void setAggThreshold(const std::string &aggThresholdStr);

    uint32_t getSampleStep() const;
    void setSampleStep(uint32_t sampleStep);
    void setSampleStep(const std::string &sampleStepStr);

    AggSortType getSortType() const;
    void setSortType(AggSortType sortType);
    void setSortType(const std::string &sortTypeStr);

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
    std::string toString() const;

private:
    void clearAggDescriptions();
private:
    std::string _originalString;
    suez::turing::SyntaxExpr* _groupKeyExpr;
    std::vector<AggFunDescription*> _aggFunDescriptions;
    std::vector<std::string> _ranges;

    int32_t _maxGroupCount;
    FilterClause* _filterClause;

    uint32_t _aggThreshold;
    uint32_t _sampleStep;
    AggSortType _sortType;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace common
} // namespace isearch

