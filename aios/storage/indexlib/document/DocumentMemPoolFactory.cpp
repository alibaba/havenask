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
#include "indexlib/document/DocumentMemPoolFactory.h"

#include "autil/EnvUtil.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"

namespace indexlibv2::document {
AUTIL_LOG_SETUP(indexlib.document, DocumentMemPoolFactory);

class DocMemPoolParam : public autil::legacy::Jsonizable
{
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("default_chunk_size", _defaultChunkSize, _defaultChunkSize);
        json.Jsonize("max_allocate_size", _maxAllocateSize, _maxAllocateSize);
        json.Jsonize("max_reserve_pool_size", _maxReservePoolSize, _maxReservePoolSize);
    }

public:
    size_t GetDefaultChunkSize() const { return _defaultChunkSize; }
    size_t GetMaxAllocateSize() const { return _maxAllocateSize; }
    size_t GetMaxReservePoolSize() const { return _maxReservePoolSize; }

private:
    size_t _defaultChunkSize = DocumentMemPoolFactory::DEFAULT_CHUNK_SIZE;
    size_t _maxAllocateSize = DocumentMemPoolFactory::MAX_ALLOCATE_SIZE;
    size_t _maxReservePoolSize = DocumentMemPoolFactory::MAX_RESERVE_POOL_SIZE;
};

DocumentMemPoolFactory::DocumentMemPoolFactory()
{
    DocMemPoolParam param;
    auto envStr = autil::EnvUtil::getEnv("DOC_MEM_POOL_PARAM");
    if (!envStr.empty()) {
        DocMemPoolParam tmpParam;
        try {
            autil::legacy::FastFromJsonString(tmpParam, envStr);
            param = tmpParam;
        } catch (const std::exception& e) {
            AUTIL_LOG(ERROR, "parse doc_mem_pool_param: %s failed, error: %s", envStr.c_str(), e.what());
        }
    }
    AUTIL_LOG(INFO,
              "init document mem pool factory: defaultChunkSize[%lu], maxAllocateSize[%lu], maxReservePoolSize[%lu]",
              param.GetDefaultChunkSize(), param.GetMaxAllocateSize(), param.GetMaxReservePoolSize());
    _poolFactory = std::make_unique<autil::mem_pool::MemPoolFactory<autil::mem_pool::UnsafePool>>(
        param.GetDefaultChunkSize(), param.GetMaxAllocateSize(), param.GetMaxReservePoolSize());
}

} // namespace indexlibv2::document
