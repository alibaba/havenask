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

#include <cmath>

#include "indexlib/base/Status.h"

namespace indexlibv2::index::ann {

struct EmbeddingUtil {
    static Status ConvertFloatToBinary(const float* floatEmb, uint32_t dimension, std::vector<int32_t>& binaryEmb)
    {
        binaryEmb.resize(GetBinaryDimension(dimension) / 32);
        static const float epsilon = 1e-6;
        for (uint32_t fi = 0; fi < dimension; ++fi) {
            if (std::fabs(floatEmb[fi]) < epsilon || std::fabs(1.0f - floatEmb[fi]) < epsilon) {
                binaryEmb[fi / 32] <<= 1;
                binaryEmb[fi / 32] += (std::fabs(floatEmb[fi]) < epsilon ? 0 : 1);
            } else {
                return Status::ParseError("convert[%f] to binary failed", floatEmb[fi]);
            }
        }
        return Status::OK();
    }

    static uint32_t GetBinaryDimension(uint32_t dimension)
    {
        if (dimension % 32 == 0) {
            return dimension;
        }
        return (dimension / 32) * 32 + 32;
    }
};
} // namespace indexlibv2::index::ann