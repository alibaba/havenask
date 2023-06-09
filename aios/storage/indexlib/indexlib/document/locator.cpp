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
#include "indexlib/document/locator.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

namespace indexlib { namespace document {

Locator::Locator(Pool* pool) : mBuffer(NULL), mCapacity(0), mPool(pool) {}

Locator::~Locator() { ReleaseBuffer(); }

Locator::Locator(const string& locator, Pool* pool) : mBuffer(NULL), mCapacity(0), mPool(pool) { SetLocator(locator); }

Locator::Locator(const StringView& locator, Pool* pool) : mBuffer(NULL), mCapacity(0), mPool(pool)
{
    SetLocator(locator);
}

Locator::Locator(const Locator& other) : mBuffer(NULL), mCapacity(0), mPool(other.mPool)
{
    SetLocator(other.mLocatorStr);
}

Locator::Locator(Locator&& other)
    : mLocatorStr(std::exchange(other.mLocatorStr, autil::StringView::empty_instance()))
    , mBuffer(std::exchange(other.mBuffer, nullptr))
    , mCapacity(std::exchange(other.mCapacity, 0))
    , mPool(std::exchange(other.mPool, nullptr))
{
}

Locator& Locator::operator=(Locator&& other)
{
    if (this != &other) {
        ReleaseBuffer();
        mLocatorStr = std::exchange(other.mLocatorStr, autil::StringView::empty_instance());
        mBuffer = std::exchange(other.mBuffer, nullptr);
        mCapacity = std::exchange(other.mCapacity, 0);
        mPool = std::exchange(other.mPool, nullptr);
    }
    return *this;
}

Locator::Locator(const ThreadSafeLocator& other) : Locator(other.mLocator) {}

void Locator::SetLocator(const string& locator) { CopyLocatorStr(locator.data(), locator.size()); }

void Locator::SetLocator(const StringView& locator) { CopyLocatorStr(locator.data(), locator.size()); }

const StringView& Locator::GetLocator() const { return mLocatorStr; }

bool Locator::operator==(const Locator& other) const { return mLocatorStr == other.mLocatorStr; }

bool Locator::operator!=(const Locator& locator) const { return !(locator == *this); }

void Locator::operator=(const Locator& other) { SetLocator(other.mLocatorStr); }

void Locator::operator=(const ThreadSafeLocator& other) { SetLocator(other.GetLocator()); }

bool Locator::IsValid() const { return mLocatorStr != StringView::empty_instance(); }

string Locator::ToString() const
{
    if (mLocatorStr.empty()) {
        return "";
    }
    return string(mLocatorStr.data(), mLocatorStr.size());
}

void Locator::serialize(DataBuffer& dataBuffer) const { dataBuffer.write(mLocatorStr); }

void Locator::deserialize(DataBuffer& dataBuffer, Pool* pool) { dataBuffer.read(mLocatorStr, pool); }

void Locator::CopyLocatorStr(const void* data, size_t size)
{
    if (size == 0) {
        mLocatorStr = StringView::empty_instance();
        return;
    }

    if (mCapacity < size) {
        ReleaseBuffer();
        mBuffer = AllocBuffer(size);
        mCapacity = size;
    }

    memcpy(mBuffer, data, size);
    StringView locator(mBuffer, size);
    mLocatorStr = locator;
}

void Locator::ReleaseBuffer()
{
    if (!mBuffer) {
        assert(mCapacity == 0);
        return;
    }

    if (mPool) {
        // no need dealloc mBuffer when use Pool
        mBuffer = NULL;
    } else {
        ARRAY_DELETE_AND_SET_NULL(mBuffer);
    }
    mCapacity = 0;
}

char* Locator::AllocBuffer(size_t size)
{
    if (mPool) {
        return (char*)mPool->allocate(size);
    }
    return new char[size];
}

ThreadSafeLocator::ThreadSafeLocator(Pool* pool) : mLocator(pool) {}

ThreadSafeLocator::ThreadSafeLocator(const string& locator, Pool* pool) : mLocator(locator, pool) {}

ThreadSafeLocator::ThreadSafeLocator(const StringView& locator, Pool* pool) : mLocator(locator, pool) {}

ThreadSafeLocator::ThreadSafeLocator(const ThreadSafeLocator& other)
{
    ScopedLock lock(mLock);
    mLocator = other.mLocator;
}

ThreadSafeLocator::ThreadSafeLocator(const Locator& other)
{
    ScopedLock lock(mLock);
    mLocator = other;
}

void ThreadSafeLocator::SetLocator(const string& locator)
{
    ScopedLock lock(mLock);
    mLocator.SetLocator(locator);
}

void ThreadSafeLocator::SetLocator(const StringView& locator)
{
    ScopedLock lock(mLock);
    mLocator.SetLocator(locator);
}

const StringView& ThreadSafeLocator::GetLocator() const
{
    ScopedLock lock(mLock);
    return mLocator.GetLocator();
}

bool ThreadSafeLocator::operator==(const ThreadSafeLocator& other) const { return mLocator == other.mLocator; }

bool ThreadSafeLocator::operator==(const Locator& other) const { return mLocator == other; }

bool ThreadSafeLocator::operator!=(const ThreadSafeLocator& other) const { return mLocator != other.mLocator; }

bool ThreadSafeLocator::operator!=(const Locator& other) const { return mLocator != other; }

void ThreadSafeLocator::operator=(const Locator& other) { SetLocator(other.GetLocator()); }

void ThreadSafeLocator::operator=(const ThreadSafeLocator& other) { SetLocator(other.GetLocator()); }

bool ThreadSafeLocator::IsValid() const
{
    ScopedLock lock(mLock);
    return mLocator.IsValid();
}

string ThreadSafeLocator::ToString() const
{
    ScopedLock lock(mLock);
    return mLocator.ToString();
}

void ThreadSafeLocator::serialize(DataBuffer& dataBuffer) const
{
    ScopedLock lock(mLock);
    mLocator.serialize(dataBuffer);
}

void ThreadSafeLocator::deserialize(DataBuffer& dataBuffer, Pool* pool)
{
    ScopedLock lock(mLock);
    mLocator.deserialize(dataBuffer, pool);
}
}} // namespace indexlib::document
