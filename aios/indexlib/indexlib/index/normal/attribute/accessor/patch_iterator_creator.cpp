#include "indexlib/index/normal/attribute/accessor/patch_iterator_creator.h"
#include "indexlib/index/normal/attribute/accessor/patch_iterator.h"
#include "indexlib/index/normal/attribute/accessor/multi_field_patch_iterator.h"
#include "indexlib/index/normal/attribute/accessor/sub_doc_patch_iterator.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PatchIteratorCreator);

PatchIteratorCreator::PatchIteratorCreator() 
{
}

PatchIteratorCreator::~PatchIteratorCreator() 
{
}

PatchIteratorPtr PatchIteratorCreator::Create(const IndexPartitionSchemaPtr& schema,
        const PartitionDataPtr& partitionData, bool isIncConsistentWithRt,
        const Version& lastLoadVersion, segmentid_t startLoadSegment)
{
    if (schema->GetSubIndexPartitionSchema())
    {
        SubDocPatchIteratorPtr iterator(new SubDocPatchIterator(schema));
        iterator->Init(partitionData, isIncConsistentWithRt, lastLoadVersion, startLoadSegment);
        return iterator;
    }
    MultiFieldPatchIteratorPtr iterator(new MultiFieldPatchIterator(schema));
    iterator->Init(partitionData, isIncConsistentWithRt, lastLoadVersion, startLoadSegment);
    return iterator;
}

IE_NAMESPACE_END(index);

