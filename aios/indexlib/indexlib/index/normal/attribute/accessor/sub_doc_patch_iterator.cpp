#include "indexlib/index/normal/attribute/accessor/sub_doc_patch_iterator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_factory.h"
#include "indexlib/index/normal/attribute/accessor/attr_field_value.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SubDocPatchIterator);

SubDocPatchIterator::SubDocPatchIterator(const IndexPartitionSchemaPtr& schema) 
    : mSchema(schema)
    , mMainIterator(schema)
    , mSubIterator(schema->GetSubIndexPartitionSchema())
{
}

SubDocPatchIterator::~SubDocPatchIterator() 
{
}

void SubDocPatchIterator::Init(const PartitionDataPtr& partitionData,
                               bool isIncConsistentWithRt,
                               const Version& lastLoadVersion,
                               segmentid_t startLoadSegment)
{
    mMainIterator.Init(partitionData, isIncConsistentWithRt, lastLoadVersion,
                       startLoadSegment, false);

    PartitionDataPtr subPartitionData = partitionData->GetSubPartitionData();
    assert(subPartitionData);
    mSubIterator.Init(subPartitionData, isIncConsistentWithRt,
                      lastLoadVersion, startLoadSegment, true);

    mSubJoinAttributeReader = AttributeReaderFactory::CreateJoinAttributeReader(
            mSchema, SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME, partitionData);
    assert(mSubJoinAttributeReader);    
}

void SubDocPatchIterator::CreateIndependentPatchWorkItems(
    vector<AttributePatchWorkItem*>& workItems)
{
    mMainIterator.CreateIndependentPatchWorkItems(workItems);
    mSubIterator.CreateIndependentPatchWorkItems(workItems); 
}

void SubDocPatchIterator::Next(AttrFieldValue& value)
{
    if (!HasNext())
    {
        //TODO: need assert?
        value.Reset();
        return;
    }
    if (!mMainIterator.HasNext())
    {
        assert(mSubIterator.HasNext());
        mSubIterator.Next(value);
        return;
    }

    if (!mSubIterator.HasNext())
    {
        assert(mMainIterator.HasNext());
        mMainIterator.Next(value);
        return;
    }

    docid_t mainDocId = mMainIterator.GetCurrentDocId();
    docid_t subDocId = mSubIterator.GetCurrentDocId();
    assert(mainDocId != INVALID_DOCID);
    assert(subDocId != INVALID_DOCID);

    if (LessThen(mainDocId, subDocId))
    {
        mMainIterator.Next(value);
    }
    else
    {
        mSubIterator.Next(value);
    }
}

bool SubDocPatchIterator::LessThen(docid_t mainDocId, docid_t subDocId) const
{
    docid_t subJoinDocId = mSubJoinAttributeReader->GetJoinDocId(subDocId);
    return mainDocId < subJoinDocId;
}

IE_NAMESPACE_END(index);

