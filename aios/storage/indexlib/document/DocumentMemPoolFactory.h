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

#include "autil/Log.h"
#include "autil/mem_pool/MemPoolFactory.h"
#include "indexlib/util/Singleton.h"

namespace indexlibv2::document {

class DocumentMemPoolFactory : public indexlib::util::Singleton<DocumentMemPoolFactory>
{
public:
    static constexpr size_t DEFAULT_CHUNK_SIZE = 5 * 1024 * 1024; // 5M
    static constexpr size_t MAX_ALLOCATE_SIZE = 10 * 1024 * 1024; // 10M
    static constexpr size_t MAX_RESERVE_POOL_SIZE = 100;
    DocumentMemPoolFactory();

public:
    autil::mem_pool::Pool* GetPool() { return _poolFactory->getPool(); }
    void Recycle(autil::mem_pool::Pool* pool) { _poolFactory->recycle(pool); }

private:
    std::unique_ptr<autil::mem_pool::MemPoolFactory<autil::mem_pool::UnsafePool>> _poolFactory;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::document
