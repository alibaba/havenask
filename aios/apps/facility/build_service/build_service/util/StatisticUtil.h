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

#include <algorithm>
#include <assert.h>
#include <cmath>
#include <stddef.h>
#include <stdint.h>
#include <utility>
#include <vector>

namespace build_service { namespace util {

class StatisticUtil
{
public:
    StatisticUtil() = default;
    ~StatisticUtil() = default;

    template <typename VectorType>
    static typename VectorType::value_type GetPercentile(VectorType& values, float percent);
    static std::pair<int64_t, int64_t> CalculateBoxplotBound(std::vector<int64_t>& samples, float coefficient);
};

template <typename VectorType>
typename VectorType::value_type StatisticUtil::GetPercentile(VectorType& values, float percent)
{
    assert(values.size());
    float k = 1 + (values.size() - 1) * percent;
    size_t idx = std::floor(k);
    std::sort(values.begin(), values.end());
    return values[idx - 1] + (values[idx] - values[idx - 1]) * (k - idx);
}

}} // namespace build_service::util
