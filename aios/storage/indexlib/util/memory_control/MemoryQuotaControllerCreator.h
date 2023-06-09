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

#include <limits>
#include <memory>

#include "autil/Log.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"
#include "indexlib/util/memory_control/MemoryQuotaController.h"
#include "indexlib/util/memory_control/PartitionMemoryQuotaController.h"
#include "indexlib/util/memory_control/SimpleMemoryQuotaController.h"

namespace indexlib { namespace util {

class MemoryQuotaControllerCreator
{
public:
    MemoryQuotaControllerCreator() {}
    ~MemoryQuotaControllerCreator() {}

public:
    static MemoryQuotaControllerPtr CreateMemoryQuotaController(int64_t quota = std::numeric_limits<int64_t>::max())
    {
        return std::make_shared<MemoryQuotaController>(/*name*/ "", quota);
    }
    static PartitionMemoryQuotaControllerPtr
    CreatePartitionMemoryController(int64_t quota = std::numeric_limits<int64_t>::max())
    {
        return PartitionMemoryQuotaControllerPtr(
            new PartitionMemoryQuotaController(CreateMemoryQuotaController(quota)));
    }
    static BlockMemoryQuotaControllerPtr
    CreateBlockMemoryController(int64_t quota = std::numeric_limits<int64_t>::max())
    {
        return BlockMemoryQuotaControllerPtr(new BlockMemoryQuotaController(CreatePartitionMemoryController(quota)));
    }
    static SimpleMemoryQuotaControllerPtr
    CreateSimpleMemoryController(int64_t quota = std::numeric_limits<int64_t>::max())
    {
        return SimpleMemoryQuotaControllerPtr(new SimpleMemoryQuotaController(CreateBlockMemoryController(quota)));
    }

    static UnsafeSimpleMemoryQuotaControllerPtr
    CreateUnsafeSimpleMemoryQuotaController(int64_t quota = std::numeric_limits<int64_t>::max())
    {
        return UnsafeSimpleMemoryQuotaControllerPtr(
            new UnsafeSimpleMemoryQuotaController(CreateBlockMemoryController(quota)));
    }

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MemoryQuotaControllerCreator> MemoryQuotaControllerCreatorPtr;
}} // namespace indexlib::util
