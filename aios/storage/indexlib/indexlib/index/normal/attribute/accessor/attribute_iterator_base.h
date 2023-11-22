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

#include "autil/mem_pool/Pool.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/CoroutineConfig.h"

namespace indexlib { namespace index {

class AttributeIteratorBase
{
public:
    AttributeIteratorBase(autil::mem_pool::Pool* pool);
    virtual ~AttributeIteratorBase();

public:
    /**
     * Reset iterator to begin state
     */
    virtual void Reset() = 0;
    virtual future_lite::coro::Lazy<index::ErrorCodeVec> BatchSeek(const std::vector<docid_t>& docIds,
                                                                   file_system::ReadOption readOption,
                                                                   std::vector<std::string>* values) noexcept = 0;

protected:
    autil::mem_pool::Pool* mPool;
};

typedef std::shared_ptr<AttributeIteratorBase> AttributeIteratorBasePtr;
}} // namespace indexlib::index
