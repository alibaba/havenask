#include "indexlib/index/normal/attribute/accessor/section_data_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_segment_reader.h"
#include "indexlib/config/section_attribute_config.h"

using namespace std;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);



IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SectionDataReader);

SectionDataReader::SectionDataReader() 
{
}

SectionDataReader::SectionDataReader(const SectionDataReader& reader)
    : StringAttributeReader((const StringAttributeReader&)reader)
{
}

SectionDataReader::~SectionDataReader() 
{
}

AttributeReader* SectionDataReader::Clone() const
{
    return new SectionDataReader(*this);
}

void SectionDataReader::InitBuildingAttributeReader(const SegmentIteratorPtr& buildingIter) 
{
    if (!buildingIter)
    {
        return;
    }
    while (buildingIter->IsValid())
    {
        index_base::InMemorySegmentPtr inMemorySegment = buildingIter->GetInMemSegment();
        if (!inMemorySegment)
        {
            buildingIter->MoveToNext();
            continue;
        }
        index::InMemorySegmentReaderPtr segmentReader = inMemorySegment->GetSegmentReader();
        if (!segmentReader)
        {
            buildingIter->MoveToNext();
            continue;
        }
        
        string indexName = SectionAttributeConfig::SectionAttributeNameToIndexName(
                mAttrConfig->GetAttrName());
        IndexSegmentReaderPtr indexSegReader =
            segmentReader->GetSingleIndexSegmentReader(indexName);
        assert(indexSegReader);
        AttributeSegmentReaderPtr attrSegReader = 
            indexSegReader->GetSectionAttributeSegmentReader();
        assert(attrSegReader);

        InMemSegmentReaderPtr inMemSegReader =
            DYNAMIC_POINTER_CAST(InMemSegmentReader, attrSegReader);
        if (!mBuildingAttributeReader)
        {
            mBuildingAttributeReader.reset(new BuildingAttributeReaderType);
        }
        mBuildingAttributeReader->AddSegmentReader(buildingIter->GetBaseDocId(), inMemSegReader);
        buildingIter->MoveToNext();
    }
}

IE_NAMESPACE_END(index);

