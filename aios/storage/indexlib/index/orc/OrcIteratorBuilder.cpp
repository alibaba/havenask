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
#include "indexlib/index/orc/OrcIteratorBuilder.h"

#include "autil/mem_pool/Pool.h"
#include "future_lite/Executor.h"
#include "indexlib/index/orc/OrcConfig.h"
#include "indexlib/index/orc/OrcPoolAdapter.h"

namespace indexlibv2::index {

OrcIteratorBuilder::OrcIteratorBuilder(OrcReader* reader) : _reader(reader) {}

OrcIteratorBuilder::~OrcIteratorBuilder() {}

OrcIteratorBuilder& OrcIteratorBuilder::SetAsync(bool async)
{
    _opts.async = async;
    return *this;
}

OrcIteratorBuilder& OrcIteratorBuilder::SetBatchSize(uint32_t batchSize)
{
    _opts.batchSize = batchSize;
    return *this;
}

OrcIteratorBuilder& OrcIteratorBuilder::SetPool(autil::mem_pool::Pool* pool)
{
    if (pool != nullptr) {
        _opts.pool = std::make_shared<OrcPoolAdapter>(pool);
    }
    return *this;
}

OrcIteratorBuilder& OrcIteratorBuilder::SetExecutor(future_lite::Executor* executor)
{
    _opts.executor = executor;
    return *this;
}

OrcIteratorBuilder& OrcIteratorBuilder::SetSegmentIds(const std::vector<segmentid_t>& segmentIds)
{
    _segmentIds = segmentIds;
    return *this;
}

OrcIteratorBuilder& OrcIteratorBuilder::SetFieldNames(const std::vector<std::string>& fieldNames)
{
    _fieldNames = fieldNames;
    return *this;
}

Status OrcIteratorBuilder::Build(std::unique_ptr<IOrcIterator>& orcIter)
{
    auto orcReader = _reader->Slice(_segmentIds);
    if (orcReader == nullptr) {
        return Status::InternalError("orc reader slice failed");
    }

    const auto& config = _reader->GetConfig();
    if (_fieldNames.size() > 0) {
        for (const auto& fieldName : _fieldNames) {
            size_t fieldId = config->GetFieldId(fieldName);
            if (fieldId == -1) {
                return Status::InternalError("field [%s] not found ", fieldName.c_str());
            }
            _opts.fieldIds.push_back(fieldId);
        }
    }
    return orcReader->CreateIterator(_opts, orcIter);
}

} // namespace indexlibv2::index
