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

#include <stddef.h>
#include <stdint.h>


namespace autil {

typedef uint64_t blockid_t;
#define INVALID_BLOCKID (blockid_t)(-1)

class Block
{
public:
    Block();
    ~Block();
public:
    uint32_t incRefCount();
    uint32_t decRefCount();
    uint32_t getRefCount() const;

public:
    blockid_t _id;
    uint8_t* _data;

private:
    uint32_t _refCount;
};

/////////////////////////////////////////

inline Block::Block()
    : _id(INVALID_BLOCKID)
    , _data(NULL)
    , _refCount(0)
{
}

inline Block::~Block()
{
}

inline uint32_t Block::incRefCount()
{
    return __sync_add_and_fetch(&_refCount, (uint32_t)1);
}

inline uint32_t Block::getRefCount() const
{
    return _refCount;
}

inline uint32_t Block::decRefCount()
{
    return __sync_sub_and_fetch(&_refCount, (uint32_t)1);
}


}

