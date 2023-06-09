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

#include "orc/MemoryPool.hh"

namespace autil::mem_pool {
class Pool;
}

namespace indexlibv2::index {

class OrcPoolAdapter final : public orc::MemoryPool
{
public:
    explicit OrcPoolAdapter(autil::mem_pool::Pool* pool);

public:
    char* malloc(uint64_t size) override;
    uint64_t malloc(uint64_t size, char** dest) override;
    void free(char* p, uint64_t size) override;
    uint64_t currentUsage() const override;

private:
    autil::mem_pool::Pool* _pool;
};

} // namespace indexlibv2::index
