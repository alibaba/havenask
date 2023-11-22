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

#include <memory>

#include "autil/ConstString.h"
#include "autil/DataBuffer.h"
#include "autil/Lock.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class Locator;
class ThreadSafeLocator;

class Locator
{
public:
    Locator(autil::mem_pool::Pool* pool = NULL);
    ~Locator();
    explicit Locator(const std::string& locator, autil::mem_pool::Pool* pool = NULL);
    explicit Locator(const autil::StringView& locator, autil::mem_pool::Pool* pool = NULL);
    Locator(const Locator& other);
    Locator(const ThreadSafeLocator& other);

    Locator(Locator&& other);
    Locator& operator=(Locator&& other);

public:
    void SetLocator(const std::string& locator);
    void SetLocator(const autil::StringView& locator);
    const autil::StringView& GetLocator() const;
    bool operator==(const Locator& other) const;
    bool operator!=(const Locator& locator) const;
    void operator=(const Locator& other);
    void operator=(const ThreadSafeLocator& other);
    bool IsValid() const;
    std::string ToString() const;
    void serialize(autil::DataBuffer& dataBuffer) const;
    void deserialize(autil::DataBuffer& dataBuffer, autil::mem_pool::Pool* pool);

private:
    void CopyLocatorStr(const void* data, size_t size);
    void ReleaseBuffer();
    char* AllocBuffer(size_t size);

private:
    autil::StringView mLocatorStr;
    char* mBuffer;
    size_t mCapacity;
    autil::mem_pool::Pool* mPool;
};

class ThreadSafeLocator
{
public:
    ThreadSafeLocator(autil::mem_pool::Pool* pool = NULL);
    explicit ThreadSafeLocator(const std::string& locator, autil::mem_pool::Pool* pool = NULL);
    explicit ThreadSafeLocator(const autil::StringView& locator, autil::mem_pool::Pool* pool = NULL);
    ThreadSafeLocator(const ThreadSafeLocator& other);
    ThreadSafeLocator(const Locator& other);

public:
    void SetLocator(const std::string& locator);
    void SetLocator(const autil::StringView& locator);
    const autil::StringView& GetLocator() const;
    bool operator==(const ThreadSafeLocator& other) const;
    bool operator==(const Locator& other) const;
    bool operator!=(const ThreadSafeLocator& other) const;
    bool operator!=(const Locator& other) const;
    void operator=(const Locator& other);
    void operator=(const ThreadSafeLocator& other);
    bool IsValid() const;

    std::string ToString() const;
    void serialize(autil::DataBuffer& dataBuffer) const;
    void deserialize(autil::DataBuffer& dataBuffer, autil::mem_pool::Pool* pool);

private:
    Locator mLocator;
    mutable autil::ThreadMutex mLock;

private:
    friend class Locator;
};

DEFINE_SHARED_PTR(Locator);
}} // namespace indexlib::document
