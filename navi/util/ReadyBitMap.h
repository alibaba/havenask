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
#ifndef NAVI_READYBITMAP_H
#define NAVI_READYBITMAP_H

#include "navi/common.h"

namespace navi {

class ReadyBitMap;

constexpr uint32_t POWER = 4u;
constexpr uint32_t UNIT = 1u << POWER;
constexpr uint32_t MASK = UNIT - 1;

typedef uint16_t BitType;
constexpr BitType BIT_TYPE_MASK = ~((BitType)0);

class BitState16 {
public:
    BitState16(uint32_t size)
    {
        bitMap.finish = BIT_TYPE_MASK;
        bitMap.ready = 0;
        bitMap.optional = ~((1u << size) - 1u);
        bitMap.mask = (1u << size) - 1u;
        assert(size <= 16);
    }
public:
    friend std::ostream& operator<<(std::ostream& os,
                                    const ReadyBitMap &readyBitMap);
private:
    struct BitState {
        BitType finish;
        BitType ready;
        BitType optional;
        BitType mask;
    };
private:
    BitState bitMap;
public:
    inline void setReady(bool value) {
        if (value) {
            setBitByMask(&bitMap.ready, BIT_TYPE_MASK);
        } else {
            clearBitByMask(&bitMap.ready, (BitType)~BIT_TYPE_MASK);
        }
    }
    inline void setReady(uint32_t index, bool value) {
        if (value) {
            setBit(&bitMap.ready, index);
        } else {
            clearBit(&bitMap.ready, index);
        }
    }
    inline void setFinish(bool value) {
        if (value) {
            setBitByMask(&bitMap.finish, BIT_TYPE_MASK);
        } else {
            clearBitByMask(&bitMap.finish, (BitType)~BIT_TYPE_MASK);
        }
    }
    inline void setFinish(uint32_t index, bool value) {
        if (value) {
            setBit(&bitMap.finish, index);
        } else {
            clearBit(&bitMap.finish, index);
        }
    }

    inline void setOptional(bool value) {
        if (value) {
            setBitByMask(&bitMap.optional, BIT_TYPE_MASK);
        } else {
            clearBitByMask(&bitMap.optional, (BitType)~BIT_TYPE_MASK);
        }
    }

    inline void setOptional(uint32_t index, bool value) {
        if (value) {
            setBit(&bitMap.optional, index);
        } else {
            clearBit(&bitMap.optional, index);
        }
    }

    void setValid(bool value);
    void setValid(uint32_t index, bool value);

    // input: init(false), graph-input(true)
    // output: init(false), linked(true), graph-output(true)
    bool isReady(uint32_t index) const;
    bool isReady() const;
    bool hasReady() const;

    // input: init(true), linked(false)
    // output : init(true),linked(false)
    bool isFinish(uint32_t index) const;
    bool isOptional(uint32_t index) const;
    bool isValid(uint32_t index) const;

    bool isOk() const;
    bool isFinish() const;
    bool hasFinishBit() const;
private:
    inline void setBit(BitType *bits, uint32_t index) {
        BitType mask = 1u << index;
        setBitByMask(bits, mask);
    }
    inline void setBitByMask(BitType *bits, BitType mask) {
        mask &= bitMap.mask; // exclude invalid bit
        BitType currentValue;
        BitType newValue;
        do {
            currentValue = *bits;
            newValue = currentValue | mask;
        } while (!cmpxchg(bits, currentValue, newValue));
    }
    inline void clearBit(BitType *bits, uint32_t index) {
        BitType mask = ~(1u << index);
        clearBitByMask(bits, mask);
    }
    inline void clearBitByMask(BitType *bits, BitType mask) {
        mask |= ~bitMap.mask; // exclude invaild bit
        BitType currentValue;
        BitType newValue;
        do {
            currentValue = *bits;
            newValue = currentValue & mask;
        } while (!cmpxchg(bits, currentValue, newValue));
    }
};

class ReadyBitMap
{
public:
    ReadyBitMap(uint32_t bitSize);
    ~ReadyBitMap();
private:
    ReadyBitMap(const ReadyBitMap &);
    ReadyBitMap& operator=(const ReadyBitMap &);
public:
    friend std::ostream& operator<<(std::ostream& os,
                                    const ReadyBitMap &readyBitMap);
public:
    static ReadyBitMap *createReadyBitMap(autil::mem_pool::Pool *pool, uint32_t size);
    static inline void freeReadyBitMap(ReadyBitMap *readyBitMap) {
        free(readyBitMap);
    }
    static size_t getBitMapSize(size_t bitSize);
public:
    void setReady(uint32_t bit, bool value);
    void setReady(bool value);
    void setFinish(uint32_t bit, bool value);
    void setFinish(bool value);
    void setOptional(uint32_t bit, bool value);
    void setOptional(bool value);
    void setValid(uint32_t bit, bool value);
    void setValid(bool value);

    bool isReady(uint32_t bit) const;
    bool hasReady() const;
    bool isReady() const;
    bool isFinish(uint32_t bit) const;
    bool isOptional(uint32_t bit) const;
    bool isValid(uint32_t bit) const;
    bool isOk() const;
    bool isFinish() const;
    bool hasFinishBit() const;
    uint32_t size() const;
private:
    void initBitMaps();
private:
    inline static void getIndexAndBitMask(uint32_t bit, uint32_t &index, uint8_t &offset);
private:
    uint32_t _bitSize;
    uint32_t _size;
    BitState16 _bitMaps[0]; // finish map first, then ready
};

NAVI_TYPEDEF_PTR(ReadyBitMap);

extern std::ostream &operator<<(std::ostream &os, const ReadyBitMap &readyBitMap);
extern std::ostream &operator<<(std::ostream &os, const ReadyBitMap *readyBitMap);
}

#endif //NAVI_READYBITMAP_H
