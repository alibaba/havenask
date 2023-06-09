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
#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep

namespace autil {
class DataBuffer;
}  // namespace autil
namespace isearch {
namespace common {
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

#define DEFAULT_DISTINCT_MODULE_NAME ""

class DistinctDescription
{
public:
    DistinctDescription(const std::string &moduleName = DEFAULT_DISTINCT_MODULE_NAME,
                        const std::string &originalString = "",
                        int32_t distinctCount = 1,
                        int32_t distinctTimes = 1,
                        int32_t maxItemCount = 0,
                        bool reservedFlag = true,
                        bool updateTotalHitFlag = false,
                        suez::turing::SyntaxExpr *syntaxExpr = NULL,
                        FilterClause *filterClause = NULL);

    ~DistinctDescription();
private:
    DistinctDescription(const DistinctDescription &) = delete;
    DistinctDescription& operator=(const DistinctDescription &) = delete;
public:
    int32_t getDistinctCount() const;
    void setDistinctCount(int32_t distinctCount);

    int32_t getDistinctTimes() const;
    void setDistinctTimes(int32_t distinctTimes);

    int32_t getMaxItemCount() const;
    void setMaxItemCount(int32_t maxItemCount);

    bool getReservedFlag() const;
    void setReservedFlag(bool reservedFalg);

    bool getUpdateTotalHitFlag() const;
    void setUpdateTotalHitFlag(bool uniqFlag);

    const std::string& getOriginalString() const;
    void setOriginalString(const std::string& originalString);

    const std::string& getModuleName() const;
    void setModuleName(const std::string& moduleName);

    suez::turing::SyntaxExpr *getRootSyntaxExpr() const;
    void setRootSyntaxExpr(suez::turing::SyntaxExpr* syntaxExpr);

    FilterClause* getFilterClause() const;
    void setFilterClause(FilterClause* filterClause);

    const std::vector<double>& getGradeThresholds() const;
    void setGradeThresholds(const std::vector<double>& gradeThresholds);

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
    std::string toString() const;
private:
    std::string _moduleName;
    std::string _originalString;
    int32_t _distinctTimes;
    int32_t _distinctCount;
    int32_t _maxItemCount;
    bool _reservedFlag;
    bool _updateTotalHitFlag;
    suez::turing::SyntaxExpr *_rootSyntaxExpr;
    FilterClause* _filterClause;
    std::vector<double> _gradeThresholds;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<DistinctDescription> DistinctDescriptionPtr;

} // namespace common
} // namespace isearch
