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
#if !defined(__clang__)
#pragma GCC diagnostic ignored "-Wunknown-pragmas"
#else
#pragma clang diagnostic ignored "-Wunknown-pragmas"
#endif

#ifdef NDEBUG
#pragma GCC optimize(3) // O3 is friendly to vector, speedup +80%
#endif

#include "indexlib/util/ByteSimilarityHasher.h"

#include <cstring>

#include "indexlib/util/Singleton.h"

using namespace std;

namespace indexlib::util {
AUTIL_LOG_SETUP(indexlib.util, ByteSimilarityHasher);

class U16TokenHashTable : public util::Singleton<U16TokenHashTable>
{
public:
    U16TokenHashTable();
    ~U16TokenHashTable();

    int32_t* GetU16TokenWeightTable(uint16_t token) const
    {
        return (int32_t*)_tokenHashWeights.data() + (uint32_t)token * 32;
    }

private:
    static inline uint32_t GetU16TokenHash(uint16_t token) { return (uint32_t)12520051 + (uint32_t)token * 65535; }

    void InitWeights(uint16_t token)
    {
        static int32_t weights[] = {-1, 1};
        uint32_t tokenHash = GetU16TokenHash(token);
        int32_t* localWeights = GetU16TokenWeightTable(token);
        localWeights[0] = weights[(tokenHash & ((uint32_t)1 << 31)) >> 31];
        localWeights[1] = weights[(tokenHash & ((uint32_t)1 << 30)) >> 30];
        localWeights[2] = weights[(tokenHash & ((uint32_t)1 << 29)) >> 29];
        localWeights[3] = weights[(tokenHash & ((uint32_t)1 << 28)) >> 28];
        localWeights[4] = weights[(tokenHash & ((uint32_t)1 << 27)) >> 27];
        localWeights[5] = weights[(tokenHash & ((uint32_t)1 << 26)) >> 26];
        localWeights[6] = weights[(tokenHash & ((uint32_t)1 << 25)) >> 25];
        localWeights[7] = weights[(tokenHash & ((uint32_t)1 << 24)) >> 24];
        localWeights[8] = weights[(tokenHash & ((uint32_t)1 << 23)) >> 23];
        localWeights[9] = weights[(tokenHash & ((uint32_t)1 << 22)) >> 22];
        localWeights[10] = weights[(tokenHash & ((uint32_t)1 << 21)) >> 21];
        localWeights[11] = weights[(tokenHash & ((uint32_t)1 << 20)) >> 20];
        localWeights[12] = weights[(tokenHash & ((uint32_t)1 << 19)) >> 19];
        localWeights[13] = weights[(tokenHash & ((uint32_t)1 << 18)) >> 18];
        localWeights[14] = weights[(tokenHash & ((uint32_t)1 << 17)) >> 17];
        localWeights[15] = weights[(tokenHash & ((uint32_t)1 << 16)) >> 16];
        localWeights[16] = weights[(tokenHash & ((uint32_t)1 << 15)) >> 15];
        localWeights[17] = weights[(tokenHash & ((uint32_t)1 << 14)) >> 14];
        localWeights[18] = weights[(tokenHash & ((uint32_t)1 << 13)) >> 13];
        localWeights[19] = weights[(tokenHash & ((uint32_t)1 << 12)) >> 12];
        localWeights[20] = weights[(tokenHash & ((uint32_t)1 << 11)) >> 11];
        localWeights[21] = weights[(tokenHash & ((uint32_t)1 << 10)) >> 10];
        localWeights[22] = weights[(tokenHash & ((uint32_t)1 << 9)) >> 9];
        localWeights[23] = weights[(tokenHash & ((uint32_t)1 << 8)) >> 8];
        localWeights[24] = weights[(tokenHash & ((uint32_t)1 << 7)) >> 7];
        localWeights[25] = weights[(tokenHash & ((uint32_t)1 << 6)) >> 6];
        localWeights[26] = weights[(tokenHash & ((uint32_t)1 << 5)) >> 5];
        localWeights[27] = weights[(tokenHash & ((uint32_t)1 << 4)) >> 4];
        localWeights[28] = weights[(tokenHash & ((uint32_t)1 << 3)) >> 3];
        localWeights[29] = weights[(tokenHash & ((uint32_t)1 << 2)) >> 2];
        localWeights[30] = weights[(tokenHash & ((uint32_t)1 << 1)) >> 1];
        localWeights[31] = weights[(tokenHash & (uint32_t)1)];
    }

private:
    std::vector<int32_t> _tokenHashWeights;
};

U16TokenHashTable::U16TokenHashTable()
{
    _tokenHashWeights.resize(65536 * 32);
    for (size_t i = 0; i < 65536; i++) {
        InitWeights((uint16_t)i);
    }
}

U16TokenHashTable::~U16TokenHashTable() {}

ByteSimilarityHasher::ByteSimilarityHasher() { _tokenWeightTable = U16TokenHashTable::GetInstance(); }

ByteSimilarityHasher::~ByteSimilarityHasher() {}

void ByteSimilarityHasher::AppendData(const char* data, size_t len)
{
    if (len == 0) {
        return;
    }

    if (_needReset) {
        memset(_hashBitCounts, 0, sizeof(int32_t) * 32);
        _needReset = false;
    }

    if (_totalLen > 0) {
        uint16_t u16Token = 0;
        char* tmp = (char*)&u16Token;
        tmp[0] = _lastChar;
        tmp[1] = data[0];
        int32_t* weights = _tokenWeightTable->GetU16TokenWeightTable(u16Token);
        AddHashBits(_hashBitCounts, weights);
    }

    if (len > 1) {
        for (size_t i = 0; i < len - 1; i++) {
            uint16_t* d = (uint16_t*)(data + i);
            int32_t* weights = _tokenWeightTable->GetU16TokenWeightTable(*d);
            AddHashBits(_hashBitCounts, weights);
        }
    }
    _totalLen += len;
    _lastChar = data[len - 1];
}

uint32_t ByteSimilarityHasher::GetHashValue()
{
    if (unlikely(_totalLen == 0)) {
        return 0;
    }
    if (unlikely(_totalLen == 1)) {
        return (unsigned char)_lastChar;
    }
    _needReset = true;
    _totalLen = 0;
    _lastChar = 0;
    return CalculateHashValue(_hashBitCounts);
}

uint32_t ByteSimilarityHasher::GetSimHash(const char* data, size_t len)
{
    if (unlikely(len == 0)) {
        return 0;
    }

    if (unlikely(len == 1)) {
        return (unsigned char)(*data);
    }

    auto table = U16TokenHashTable::GetInstance();
    int32_t hashBitCount[32] = {0};
    for (size_t i = 0; i < len - 1; i++) {
        uint16_t* tokenPtr = (uint16_t*)(data + i);
        int32_t* weights = table->GetU16TokenWeightTable(*tokenPtr);
        AddHashBits(hashBitCount, weights);
    }
    return CalculateHashValue(hashBitCount);
}

uint8_t ByteSimilarityHasher::GetSimDistance(uint32_t leftHashValue, uint32_t rightHashValue)
{
    if (leftHashValue == rightHashValue) {
        return 0;
    }
    int32_t exo = (int32_t)(leftHashValue ^ rightHashValue);
    uint8_t dist = 0;
    while (exo != 0) {
        ++dist;
        exo = exo & (exo - 1);
    }
    return dist;
}

} // namespace indexlib::util
