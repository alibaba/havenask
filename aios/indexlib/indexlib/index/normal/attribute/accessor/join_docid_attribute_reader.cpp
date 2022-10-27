#include <autil/StringUtil.h>
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_reader.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, JoinDocidAttributeReader);

JoinDocidAttributeReader::JoinDocidAttributeReader() 
{
}

JoinDocidAttributeReader::~JoinDocidAttributeReader() 
{
}

void JoinDocidAttributeReader::InitJoinBaseDocId(
        const PartitionDataPtr& partitionData)
{
    assert(mSegmentIds.size() == mSegmentReaders.size());
    for (size_t i = 0; i < mSegmentIds.size(); ++i)
    {
        const SegmentData& segData = partitionData->GetSegmentData(
                mSegmentIds[i]);
        mJoinedBaseDocIds.push_back(segData.GetBaseDocId());
    }

    SegmentIteratorPtr buildingIter =
        partitionData->CreateSegmentIterator()->CreateIterator(SIT_BUILDING);
    while (buildingIter->IsValid())
    {
        mJoinedBuildingBaseDocIds.push_back(buildingIter->GetBaseDocId());
        buildingIter->MoveToNext();
    }
    assert(!mBuildingAttributeReader ||
           mJoinedBuildingBaseDocIds.size() == mBuildingAttributeReader->Size());
}

AttributeReader* JoinDocidAttributeReader::Clone() const
{
    return new JoinDocidAttributeReader(*this);
}

bool JoinDocidAttributeReader::Read(docid_t docId, string& attrValue,
                                    autil::mem_pool::Pool* pool) const
{
    docid_t joinDocId = GetJoinDocId(docId);
    attrValue = StringUtil::toString<docid_t>(joinDocId);
    return true;
}

IE_NAMESPACE_END(index);

