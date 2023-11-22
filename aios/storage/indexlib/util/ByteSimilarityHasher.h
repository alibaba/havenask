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

/**********************************************************************************
 * 用于计算字节序列的simHash值，token选择字节流中的所有相邻字节组成, token词频加权
 * 任意两个simHash值的海明距离用于描述两个value间的相似度，simHash数值本身没有线性含义
 ** 相似度越高的value簇拥在一起则压缩效果应该越好
 ** 简单使用策略:基于simHash排序用于等值聚类,直观感觉单个value越长等值聚类效果越明显
 **********************************************************************************/

#include <emmintrin.h>
#include <stddef.h>
#include <stdint.h>

#include "autil/Log.h"

namespace indexlib::util {

class U16TokenHashTable;

class ByteSimilarityHasher
{
public:
    ByteSimilarityHasher();
    ~ByteSimilarityHasher();

public:
    static uint32_t GetSimHash(const char* data, size_t len);
    static uint8_t GetSimDistance(uint32_t leftHashValue, uint32_t rightHashValue);

public:
    void AppendData(const char* data, size_t len);
    uint32_t GetHashValue();

private:
    static inline void AddHashBits(int32_t* hashBitCounts, int32_t* weights)
    {
        for (size_t i = 0; i + 4 <= 32; i += 4) {
            __m128i ma = _mm_loadu_si128((__m128i*)&hashBitCounts[i]);
            __m128i mb = _mm_loadu_si128((__m128i*)&weights[i]);
            ma = _mm_add_epi32(ma, mb);
            _mm_storeu_si128((__m128i*)&hashBitCounts[i], ma);
        }
    }

    static inline uint32_t CalculateHashValue(int32_t* hashBitCounts)
    {
        uint32_t simHash = 0;
        for (size_t i = 0; i < 32; i++) {
            simHash = simHash << 1;
            if (hashBitCounts[i] > 0) {
                ++simHash;
            }
        }
        return simHash;
    }

private:
    U16TokenHashTable* _tokenWeightTable = nullptr;
    bool _needReset = false;
    char _lastChar = 0;
    uint32_t _totalLen = 0;
    int32_t _hashBitCounts[32] = {0};

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::util
