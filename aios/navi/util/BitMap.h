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

#include <vector>

#include "navi/common.h"

namespace navi {

constexpr uint32_t BIT_POWER = 4u;
constexpr uint32_t BIT_UNIT = 1u << BIT_POWER;

typedef uint16_t BitType;
constexpr BitType BIT_TYPE_SET = ~((BitType)0);
constexpr BitType BIT_TYPE_RESET = ((BitType)0);

class BitMap16 {
public:
    BitMap16(uint32_t size) {
        bits = 0;
        mask = (1u << size) - 1u;
    }

public:
    void set(uint32_t pos, bool value) {
        BitType mask = 1u << pos;
        BitType currentValue;
        BitType newValue;
        do {
            currentValue = bits;
            newValue = value ? (currentValue | mask) : (currentValue & (~mask));
        } while (!cmpxchg(&bits, currentValue, newValue));
    }
    void set() {
        BitType currentValue;
        BitType newValue = mask & BIT_TYPE_SET;
        do {
            currentValue = bits;
        } while (!cmpxchg(&bits, currentValue, newValue));
    }
    void reset() {
        BitType currentValue;
        BitType newValue = mask & BIT_TYPE_RESET;
        do {
            currentValue = bits;
        } while (!cmpxchg(&bits, currentValue, newValue));
    }
    bool operator[](std::size_t pos) const { return bits & (1u << pos); }
    bool all() const { return (bits & mask) == (BIT_TYPE_SET & mask); }
    bool none() const { return (bits & mask) == BIT_TYPE_RESET; }
    bool any() const { return (bits & mask) != BIT_TYPE_RESET; }
    size_t count() const { return __builtin_popcount(bits & mask); }
    std::string toString() const {
        BitType size = __builtin_popcount(mask);
        std::string s;
        s.reserve(size);
        for (int i = size - 1; i >= 0; --i) {
            s.push_back((*this)[i] ? '1' : '0');
        }
        return s;
    }

public:
    friend std::ostream &operator<<(std::ostream &os, const BitMap16 &bitMap16);

private:
    BitType bits;
    BitType mask;
};

class BitMap {
public:
    BitMap();
    BitMap(uint32_t size);
    ~BitMap();

public:
    void init(uint32_t size);

public:
    bool all() const;
    bool any() const;
    bool none() const;
    size_t size() const;
    size_t count() const;
    void set();
    void reset();
    void set(uint32_t pos, bool value);
    bool operator[](std::size_t pos) const;

public:
    friend std::ostream &operator<<(std::ostream &os, const BitMap &bitMap);
    std::string toString() const;

private:
    inline void calcIndexAndPos(uint32_t bit, uint32_t &index, uint8_t &pos) const;

private:
    size_t _size;
    std::vector<BitMap16> _bitMaps;
};

NAVI_TYPEDEF_PTR(BitMap);

} // namespace navi
