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
#include "build_service/util/StatisticUtil.h"

namespace build_service { namespace util {

// Walker M L, Dovoedo Y H, Chakraborti S, et al. An improved boxplot for univariate data[J]. The American Statistician,
// 2018, 72(4): 348-353.
//
//  |                     |----------IQR----------|                      |
//  min--------whiskers---q1-------median--------q3-------whiskers-----max
//  |                     |---SIQRL---|---SIQRU---|                      |
//
// IQR : inter quartile range
// SIQRL : semi inter quartile range lower
// SIQRU : semi inter quartile range upper

std::pair<int64_t, int64_t> StatisticUtil::CalculateBoxplotBound(std::vector<int64_t>& samples, float coefficient)
{
    int64_t firstQuartile = GetPercentile(samples, 0.25);
    int64_t thirdQuartile = GetPercentile(samples, 0.75);
    int64_t interQuartileRange = thirdQuartile - firstQuartile;

    int64_t median = GetPercentile(samples, 0.5);
    int64_t semiLowerIQR = median - firstQuartile;
    int64_t semiUpperIQR = thirdQuartile - median;

    float ratioL = (float)semiLowerIQR / (1 + semiUpperIQR);
    float ratioU = (float)semiUpperIQR / (1 + semiLowerIQR);

    int64_t min = firstQuartile - coefficient * interQuartileRange * ratioL;
    int64_t max = thirdQuartile + coefficient * interQuartileRange * ratioU;

    return {min, max};
}

}} // namespace build_service::util
