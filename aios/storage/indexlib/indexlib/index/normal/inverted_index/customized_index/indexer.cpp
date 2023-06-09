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
#include "indexlib/index/normal/inverted_index/customized_index/indexer.h"

#include "indexlib/config/field_schema.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;

using namespace indexlib::config;
namespace indexlib { namespace index {
IE_LOG_SETUP(index, Indexer);

Indexer::Indexer(const util::KeyValueMap& parameters)
    : mParameters(parameters)
    , mAllocator()
    , mPool(&mAllocator, DEFAULT_CHUNK_SIZE * 1024 * 1024)
{
}

Indexer::~Indexer() {}

bool Indexer::Init(const IndexerResourcePtr& resource)
{
    if (!resource->schema) {
        IE_LOG(ERROR, "indexer init failed: schema is NULL");
        return false;
    }
    mFieldSchema = resource->schema->GetFieldSchema();
    if (!mFieldSchema) {
        IE_LOG(ERROR, "indexer init failed: fieldSchema is NULL");
        return false;
    }
    return DoInit(resource);
}

bool Indexer::DoInit(const IndexerResourcePtr& resource) { return true; }

bool Indexer::GetFieldName(fieldid_t fieldId, string& fieldName)
{
    if (!mFieldSchema) {
        IE_LOG(ERROR, "field schema is NULL");
        return false;
    }
    FieldConfigPtr fieldConfig = mFieldSchema->GetFieldConfig(fieldId);
    if (!fieldConfig) {
        IE_LOG(ERROR, "get fieldConfig failed for field[%d]", fieldId);
        return false;
    }
    fieldName = fieldConfig->GetFieldName();
    return true;
}

// this section provides default implementation
uint32_t Indexer::GetDistinctTermCount() const { return 0u; }

size_t Indexer::EstimateInitMemoryUse(size_t lastSegmentDistinctTermCount) const { return 0u; }

// estimate current memory use
int64_t Indexer::EstimateCurrentMemoryUse() const { return mPool.getUsedBytes() + mSimplePool.getUsedBytes(); }

// estimate memory allocated by pool in Dumping
int64_t Indexer::EstimateExpandMemoryUseInDump() const { return 0; }
}} // namespace indexlib::index
