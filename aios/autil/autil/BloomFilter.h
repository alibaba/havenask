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

#include <stdint.h>
#include <string>
#include <vector>


namespace autil {

template <typename T>
struct BFHelperTraits
{
    static const char* getAddr(const T& t) { return (const char*)&t; }
    static uint32_t getLen(const T& t) { return sizeof(T); }

};

template <>
struct BFHelperTraits<std::string>
{
    static const char* getAddr(const std::string& t) { return t.c_str(); }
    static uint32_t getLen(const std::string& t) { return t.length(); };
};

class BloomFilter
{
public:
    typedef uint64_t (*HashFunctionType) (const char *str, int32_t len);
public:
    /*
     * If you construct bloom filter yourself, then you should specify the 
     * bits_size and bitbuffer_size parameters.
     * If you construct bloom filter via method DecodeFromString, then you
     * should not specify bits_size and bitbuffer_size parameters.
     * Note: you should calculate the optimal bits_size and hash_num according 
     * to your application scenario.
     */
    BloomFilter(uint32_t bits_size = 0, uint32_t hash_num = 0);
    ~BloomFilter();
private:
    BloomFilter(const BloomFilter &);
    BloomFilter& operator = (const BloomFilter &);
public:
    uint32_t getHashNumber() const { 
        return _hashFuncNum; 
    }
    void setHashNumber(uint32_t hashNum) {
        _hashFuncNum = (hashNum <= 10 ? hashNum : 10);
    }
    uint32_t getElementsNumber() const { 
        return _elementNum; 
    }
    //added for test
    uint32_t getBitsBufferSize() const {
        return _bitsBufferSize;
    }
    //added for test
    uint32_t getBitNumber() const {
        return _bits;
    }

    void Insert(const char* element, uint32_t len);

    template <typename T>
    void Insert(T t) { this->Insert((const char*)&t, sizeof(T)); }
    template <typename T>
    bool Contains(T t) { return this->Contains((const char*)&t, sizeof(T)); }

    bool Contains(const char* element, uint32_t len);

    bool StreamToString(std::string& filterStr) const;
    bool DecodeFromString(std::string& filterStr);
private:
    void RegisterHashFunctions();
    void GenerateHeaderInfo(char* headerInfo, uint32_t& size) const;
    void SetBit(uint32_t byte_offset, uint32_t bit_offset);
    bool TestBit(uint32_t byte_offset, uint32_t bit_offset);
private:
    uint32_t _bits; //12007 bits
    uint32_t _bitsBufferSize; //1503 in byte
    uint32_t _hashFuncNum; //5
    uint32_t _elementNum; // dynamic
    uint8_t* _arraryBits; // pointer to array bits
    std::vector<HashFunctionType> _hashFunctions; // hash function list
private:
    static const uint8_t bit_mask[8];
};

}

