#include "indexlib/index/normal/attribute/accessor/multi_pack_attribute_reader.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MultiPackAttributeReader);

PackAttributeReaderPtr MultiPackAttributeReader::RET_EMPTY_PTR = PackAttributeReaderPtr();

MultiPackAttributeReader::MultiPackAttributeReader(
    const AttributeSchemaPtr& attrSchema,
    AttributeMetrics* attributeMetrics)
    : mAttrSchema(attrSchema)
    , mAttributeMetrics(attributeMetrics)
{
}

MultiPackAttributeReader::~MultiPackAttributeReader() 
{
}

void MultiPackAttributeReader::Open(const index_base::PartitionDataPtr& partitionData)
{
    assert(partitionData);
    if (mAttrSchema)
    {
        size_t packAttrCount = mAttrSchema->GetPackAttributeCount();
        mPackAttrReaders.resize(packAttrCount);
        
        auto packAttrConfigs = mAttrSchema->CreatePackAttrIterator();
        auto packIter = packAttrConfigs->Begin();
        for (; packIter != packAttrConfigs->End(); packIter++)
        {
            const PackAttributeConfigPtr& packConfig = *packIter;
            const string& packName = packConfig->GetAttrName();
            PackAttributeReaderPtr packAttrReader;
            packAttrReader.reset(new PackAttributeReader(mAttributeMetrics));
            packAttrReader->Open(packConfig, partitionData);
            mPackNameToIdxMap[packName] = packConfig->GetPackAttrId();
            mPackAttrReaders[packConfig->GetPackAttrId()] = packAttrReader;
        }
    }
}

const PackAttributeReaderPtr& MultiPackAttributeReader::GetPackAttributeReader(
    const string& packAttrName) const
{
    NameToIdxMap::const_iterator it = mPackNameToIdxMap.find(packAttrName);
    if (it == mPackNameToIdxMap.end())
    {
        return RET_EMPTY_PTR;
    }
    return GetPackAttributeReader(it->second);
}

const PackAttributeReaderPtr& MultiPackAttributeReader::GetPackAttributeReader(
    packattrid_t packAttrId) const
{
    if (packAttrId == INVALID_PACK_ATTRID || packAttrId >= mPackAttrReaders.size())
    {
        return RET_EMPTY_PTR;
    }
    return mPackAttrReaders[packAttrId];
}



IE_NAMESPACE_END(index);

