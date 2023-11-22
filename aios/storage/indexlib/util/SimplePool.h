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
#include <stddef.h>

#include "autil/mem_pool/PoolBase.h"

namespace indexlib { namespace util {

class SimplePool : public autil::mem_pool::PoolBase
{
public:
    SimplePool();
    virtual ~SimplePool();

private:
    SimplePool(const SimplePool&);
    SimplePool& operator=(const SimplePool&);

public:
    void* allocate(size_t numBytes) override;
    void deallocate(void* ptr, size_t size) override;
    void release() override;
    size_t reset() override;
    bool isInPool(const void* ptr) const override;

    size_t getUsedBytes() const { return _usedBytes; }
    size_t getPeakOfUsedBytes() const { return _peakOfUsedBytes; }

private:
    size_t _usedBytes;
    size_t _peakOfUsedBytes;
};

typedef std::shared_ptr<SimplePool> SimplePoolPtr;
}} // namespace indexlib::util
