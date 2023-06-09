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

#include <limits>

#include "autil/MultiValueType.h"

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace util {

template<typename T>
struct NumericLimits {
    static T min() {return std::numeric_limits<T>::min();} 
    static T max() {return std::numeric_limits<T>::max();} 
};

template<>
struct NumericLimits<double> {
    static double min() {return -std::numeric_limits<double>::max();} 
    static double max() {return std::numeric_limits<double>::max();} 
};

template<>
struct NumericLimits<float> {
    static float min() {return -std::numeric_limits<float>::max();} 
    static float max() {return std::numeric_limits<float>::max();} 
};

template<>
struct NumericLimits<autil::MultiChar> {
    static autil::MultiChar min() {return autil::MultiChar();} 
    static autil::MultiChar max() {return autil::MultiChar();} 
};

} // namespace util
} // namespace isearch

