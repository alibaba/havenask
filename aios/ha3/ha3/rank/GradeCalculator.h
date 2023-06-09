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
#include <algorithm>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/rank/DistinctInfo.h"

namespace matchdoc {
template <typename T> class Reference;
}  // namespace matchdoc

namespace isearch {
namespace rank {
class GradeCalculatorBase {
public:
    virtual ~GradeCalculatorBase() {};
public:
    virtual void calculate(DistinctInfo* distInfo) const {}
    uint32_t calculate(double score) const {
        size_t i = 0;
        size_t gradeSize = _gradeThresholds.size();
        for (; i < gradeSize; ++i)
        {
            if (score < _gradeThresholds[i]) {
                break;
            }
        }
        return i;
    }
    void setGradeThresholds(const std::vector<double> &gradeThresholds) {
        _gradeThresholds = gradeThresholds;
    }
protected:
    std::vector<double> _gradeThresholds;
};

template<typename T>
class GradeCalculator : public GradeCalculatorBase
{
public:
    GradeCalculator(matchdoc::Reference<T> *ref) {
        _rawScoreRef = ref;
    }
    ~GradeCalculator() {}
private:
    GradeCalculator(const GradeCalculator &);
    GradeCalculator& operator=(const GradeCalculator &);
public:
    void calculate(DistinctInfo* distInfo) const {
        size_t gradeSize = _gradeThresholds.size();
        if (gradeSize == 0) {
            return;
        }
        T value;
        value = _rawScoreRef->get(distInfo->getMatchDoc());
        double score = (double)value;
        uint32_t gradeValue = GradeCalculatorBase::calculate(score);
        distInfo->setGradeBoost(gradeValue);
    }
private:
    matchdoc::Reference<T> *_rawScoreRef;
private:
    AUTIL_LOG_DECLARE();
};


} // namespace rank
} // namespace isearch

