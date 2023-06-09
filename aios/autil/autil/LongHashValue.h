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

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <sstream>
#include <string>

namespace autil {

template<int COUNT = 2>
class LongHashValue
{
public:
 
    LongHashValue(uint64_t integer) {
        for(int i = 0; i < COUNT - 1 ; i++) {
            value[i] = 0;
        }
        value[COUNT - 1] = integer;
    }

    LongHashValue() {
        for(int i = 0; i < COUNT ; i++) {
            value[i] = 0;
        }
    }

    size_t size() {
        return sizeof(value);
    }
    
    int count() {
        return COUNT;
    }
    
    bool operator ==(const LongHashValue<COUNT>& other) const
    {
        return std::equal(value, value + COUNT, other.value);
    }
    
    bool operator !=(const LongHashValue<COUNT>& other) const
    {
        return !(*this == other);
    }
    
    bool operator < (const LongHashValue<COUNT>& other) const
    {
        return std::lexicographical_compare(value, value + COUNT,
                other.value, other.value + COUNT);
    }
    
    bool operator > (const LongHashValue<COUNT>& other) const {
        return other < *this;
    }

    bool operator <= (const LongHashValue<COUNT> &other) const {
        return (*this < other) || (*this == other);
    }

    bool operator >= (const LongHashValue<COUNT> &other) const { return !(*this < other); }

    void serialize(std::string& str) const
    {
        str.resize(sizeof(uint64_t) * COUNT, '\0');
        
        uint64_t tmpPrimaryKey ;
        int len = sizeof(tmpPrimaryKey);
        for(int seg = 0; seg < COUNT; seg++)
        {
            tmpPrimaryKey = value[seg];
            for (int i = seg * len + len - 1; i >= seg * len; --i) 
            {
                str[i] = (char)(tmpPrimaryKey & 0xFF);
                tmpPrimaryKey >>= 8;
            }
        }
    }

    void deserialize(const std::string& str)
    {
        assert(str.length() == COUNT * sizeof(uint64_t));
        int len = sizeof(uint64_t);
        uint64_t tmpPrimaryKey;
        for(int seg = 0; seg < COUNT; seg++)
        {
            tmpPrimaryKey = (uint64_t)0;
            for(int i = seg * len; i < (seg+1) * len; i++)
            {
                tmpPrimaryKey <<= 8;
                tmpPrimaryKey |= (unsigned char)str[i];
                
            }
            value[seg] = tmpPrimaryKey;
        }
    }

    bool hexStrToUint128(const char* str);
    void uint128ToHexStr(char* hexStr, int len);
  
    std::string toString();

public:
    uint64_t value[COUNT];
};

typedef LongHashValue<2> uint128_t;
typedef LongHashValue<4> uint256_t;

std::ostream& operator <<(std::ostream& stream, uint128_t v);
std::ostream& operator <<(std::ostream& stream, uint256_t v);

std::istream& operator >>(std::istream& stream, uint128_t& v);
std::istream& operator >>(std::istream& stream, uint256_t& v);

}
