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
#include "indexlib/index/orc/OrcPoolAdapter.h"

#include "autil/mem_pool/Pool.h"

namespace indexlibv2::index {

OrcPoolAdapter::OrcPoolAdapter(autil::mem_pool::Pool* pool) : _pool(pool) {}

char* OrcPoolAdapter::malloc(uint64_t size) { return reinterpret_cast<char*>(_pool->allocate(size)); }

uint64_t OrcPoolAdapter::malloc(uint64_t size, char** dest)
{
    *dest = this->malloc(size);
    return size;
}

void OrcPoolAdapter::free(char* p, uint64_t size) { _pool->deallocate(p, size); }

uint64_t OrcPoolAdapter::currentUsage() const { return _pool->getUsedBytes(); }

} // namespace indexlibv2::index
