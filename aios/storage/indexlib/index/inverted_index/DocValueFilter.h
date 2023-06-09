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

#include "indexlib/base/Types.h"

namespace autil::mem_pool {
class Pool;
}

namespace indexlib::index {

class DocValueFilter
{
public:
    DocValueFilter(autil::mem_pool::Pool* pool) : _sessionPool(pool) {}
    virtual ~DocValueFilter() {}

public:
    virtual bool Test(docid_t docid) = 0;
    virtual DocValueFilter* Clone() const = 0;
    autil::mem_pool::Pool* GetSessionPool() const { return _sessionPool; }

protected:
    autil::mem_pool::Pool* _sessionPool;
};

} // namespace indexlib::index
