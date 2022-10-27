#include "indexlib/index/normal/inverted_index/accessor/index_accessory_reader.h"
#include "indexlib/index/normal/attribute/accessor/section_attribute_reader_impl.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/index_base/partition_data.h"

using namespace std;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, IndexAccessoryReader);

IndexAccessoryReader::IndexAccessoryReader(const IndexSchemaPtr& schema)
    : mIndexSchema(schema)
{
}

IndexAccessoryReader::IndexAccessoryReader(
        const IndexAccessoryReader &other)
    : mIndexSchema(other.mIndexSchema)
    , mSectionReaderMap(other.mSectionReaderMap)
{
    CloneSectionReaders(other.mSectionReaderVec, mSectionReaderVec);
}

IndexAccessoryReader::~IndexAccessoryReader() 
{
    mSectionReaderVec.clear();
}

IndexAccessoryReader* IndexAccessoryReader::Clone() const
{
    return new IndexAccessoryReader(*this);
}

const SectionAttributeReaderImplPtr IndexAccessoryReader::GetSectionReader(
        const string& indexName) const
{
    SectionReaderPosMap::const_iterator it = mSectionReaderMap.find(indexName);
    if (it != mSectionReaderMap.end())
    {
        return mSectionReaderVec[it->second];
    }
    return SectionAttributeReaderImplPtr();
}

bool IndexAccessoryReader::CheckSectionConfig(
        const IndexConfigPtr& indexConfig)
{
    IndexType indexType = indexConfig->GetIndexType();
    if (indexType == it_pack || indexType == it_expack)
    {
        PackageIndexConfigPtr packIndexConfig =
            DYNAMIC_POINTER_CAST(PackageIndexConfig, indexConfig);
        if (!packIndexConfig)
        {
            IE_LOG(ERROR, "indexConfig must be PackageIndexConfig"
                   " for pack/expack index");
            return false;
        }
        return packIndexConfig->HasSectionAttribute();
    }
    return false;
}

bool IndexAccessoryReader::UseSharedSectionReader(
        const PackageIndexConfigPtr& packIndexConfig)
{
    if (packIndexConfig->IsVirtual() ||
        packIndexConfig->GetShardingType() == IndexConfig::IST_IS_SHARDING)
    {
        return true;
    }
    return false;
}

void IndexAccessoryReader::AddSharedRelations(
        const PackageIndexConfigPtr& packIndexConfig,
        ShareSectionMap &shareMap)
{
    IndexConfig::IndexShardingType shardingType = packIndexConfig->GetShardingType();
    if (shardingType == IndexConfig::IST_IS_SHARDING)
    {
        return;
    }

    string sharedIndexName;
    if (packIndexConfig->IsVirtual())
    {
        sharedIndexName = packIndexConfig->GetNonTruncateIndexName();
        assert(!sharedIndexName.empty());
        shareMap[sharedIndexName].push_back(packIndexConfig->GetIndexName());
    }
    else
    {
        sharedIndexName = packIndexConfig->GetIndexName();
    }

    if (shardingType == IndexConfig::IST_NO_SHARDING)
    {
        return;
    }
    const vector<IndexConfigPtr>& shardingIndexConfigs =
        packIndexConfig->GetShardingIndexConfigs();

    for (size_t i = 0; i < shardingIndexConfigs.size(); ++i)
    {
        shareMap[sharedIndexName].push_back(shardingIndexConfigs[i]->GetIndexName());
    }
}


bool IndexAccessoryReader::InitSectionReader(
        const IndexConfigPtr& indexConfig,
        const PartitionDataPtr& partitionData,
        ShareSectionMap &shareMap)
{
    string indexName = indexConfig->GetIndexName();
    PackageIndexConfigPtr packIndexConfig =
        DYNAMIC_POINTER_CAST(PackageIndexConfig, indexConfig);
    assert(packIndexConfig.get() != NULL);

    AddSharedRelations(packIndexConfig, shareMap); 
    
    if (UseSharedSectionReader(packIndexConfig))
    {
        return true;
    }
    
    SectionAttributeReaderImplPtr sectionReader = OpenSectionReader(
            packIndexConfig, partitionData);
    mSectionReaderVec.push_back(sectionReader);
    bool ret = mSectionReaderMap.insert(
            make_pair(indexName, mSectionReaderVec.size()-1)).second;    

    assert(ret);
    return ret;
}

SectionAttributeReaderImplPtr IndexAccessoryReader::OpenSectionReader(
        const config::PackageIndexConfigPtr& indexConfig,
        const PartitionDataPtr& partitionData)
{
    SectionAttributeReaderImplPtr sectionReader(new SectionAttributeReaderImpl);
    sectionReader->Open(indexConfig, partitionData);
    return sectionReader;
}


void IndexAccessoryReader::CloneSectionReaders(
        const SectionReaderVec &srcReaders,
        SectionReaderVec &clonedReaders)
{
    clonedReaders.clear();
    for(size_t i = 0; i < srcReaders.size(); ++i)
    {
        SectionAttributeReaderImpl * cloneReader =
            dynamic_cast<SectionAttributeReaderImpl*>(srcReaders[i]->Clone()); 
        assert(cloneReader != NULL);
        clonedReaders.push_back(SectionAttributeReaderImplPtr(cloneReader));
    }
}

bool IndexAccessoryReader::Open(const PartitionDataPtr& partitionData)
{
    ShareSectionMap shareMap;
    auto indexConfigs = mIndexSchema->CreateIterator(true);
    auto iter = indexConfigs->Begin();
    for (; iter != indexConfigs->End(); iter++)
    {
        const IndexConfigPtr& indexConfig = *iter;
        if (!CheckSectionConfig(indexConfig))
        {
            continue;
        }
        if (!InitSectionReader(indexConfig, partitionData, shareMap))
        {
            return false;
        }
    }

    ShareSectionMap::const_iterator it = shareMap.begin();
    for (; it != shareMap.end(); ++it)
    {
        const string& sharedIndexName = it->first;
        const ShareSectionIndexNames& indexNames = it->second;
        SectionReaderPosMap::const_iterator sit = mSectionReaderMap.find(sharedIndexName);
        assert(sit != mSectionReaderMap.end());
        for (size_t i = 0; i < indexNames.size(); ++i)
        {
            mSectionReaderMap.insert(make_pair(indexNames[i], sit->second));
        }
    }
    return true;
}

IE_NAMESPACE_END(index);

