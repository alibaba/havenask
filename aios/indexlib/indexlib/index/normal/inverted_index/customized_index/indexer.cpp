#include "indexlib/index/normal/inverted_index/customized_index/indexer.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/field_schema.h"

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, Indexer);

Indexer::Indexer(const util::KeyValueMap& parameters)
    : mParameters(parameters)
    , mAllocator()
    , mPool(&mAllocator, DEFAULT_CHUNK_SIZE * 1024 * 1024)
{}

Indexer::~Indexer() {}

bool Indexer::Init(const IndexerResourcePtr& resource)
{
    if (!resource->schema)
    {
        IE_LOG(ERROR, "indexer init failed: schema is NULL");
        return false;
    }
    mFieldSchema = resource->schema->GetFieldSchema();
    if (!mFieldSchema)
    {
        IE_LOG(ERROR, "indexer init failed: fieldSchema is NULL");
        return false;
    }
    return DoInit(resource);
}

bool Indexer::DoInit(const IndexerResourcePtr& resource)
{
    return true;
}

bool Indexer::GetFieldName(fieldid_t fieldId, string& fieldName)
{
    if (!mFieldSchema)
    {
        IE_LOG(ERROR, "field schema is NULL");
        return false;
    }
    FieldConfigPtr fieldConfig = mFieldSchema->GetFieldConfig(fieldId);
    if (!fieldConfig)
    {
        IE_LOG(ERROR, "get fieldConfig failed for field[%d]", fieldId);
        return false;        
    }
    fieldName = fieldConfig->GetFieldName();
    return true;
}

// this section provides default implementation
uint32_t Indexer::GetDistinctTermCount() const
{
    return 0u;
}

size_t Indexer::EstimateInitMemoryUse(size_t lastSegmentDistinctTermCount) const
{
    return 0u;
}

// estimate current memory use
int64_t Indexer::EstimateCurrentMemoryUse() const
{
    return mPool.getUsedBytes() + mSimplePool.getUsedBytes();
}

// estimate memory allocated by pool in Dumping
int64_t Indexer::EstimateExpandMemoryUseInDump() const
{
    return 0;
}


IE_NAMESPACE_END(index);

