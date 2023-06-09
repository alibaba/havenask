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
#include "indexlib/index/normal/inverted_index/accessor/index_accessory_reader.h"

#include "indexlib/config/index_schema.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/index/normal/attribute/accessor/section_attribute_reader_impl.h"
#include "indexlib/index_base/partition_data.h"

using namespace std;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, LegacyIndexAccessoryReader);

LegacyIndexAccessoryReader::LegacyIndexAccessoryReader(const IndexSchemaPtr& schema) : mIndexSchema(schema) {}

LegacyIndexAccessoryReader::LegacyIndexAccessoryReader(const LegacyIndexAccessoryReader& other)
    : mIndexSchema(other.mIndexSchema)
    , mSectionReaderMap(other.mSectionReaderMap)
{
    CloneSectionReaders(other.mSectionReaderVec, mSectionReaderVec);
}

LegacyIndexAccessoryReader::~LegacyIndexAccessoryReader() { mSectionReaderVec.clear(); }

LegacyIndexAccessoryReader* LegacyIndexAccessoryReader::Clone() const { return new LegacyIndexAccessoryReader(*this); }

const LegacySectionAttributeReaderImplPtr LegacyIndexAccessoryReader::GetSectionReader(const string& indexName) const
{
    SectionReaderPosMap::const_iterator it = mSectionReaderMap.find(indexName);
    if (it != mSectionReaderMap.end()) {
        return mSectionReaderVec[it->second];
    }
    return LegacySectionAttributeReaderImplPtr();
}

bool LegacyIndexAccessoryReader::CheckSectionConfig(const IndexConfigPtr& indexConfig)
{
    InvertedIndexType indexType = indexConfig->GetInvertedIndexType();
    if (indexType == it_pack || indexType == it_expack) {
        PackageIndexConfigPtr packIndexConfig = DYNAMIC_POINTER_CAST(PackageIndexConfig, indexConfig);
        if (!packIndexConfig) {
            IE_LOG(ERROR, "indexConfig must be PackageIndexConfig"
                          " for pack/expack index");
            return false;
        }
        return packIndexConfig->HasSectionAttribute();
    }
    return false;
}

bool LegacyIndexAccessoryReader::UseSharedSectionReader(const PackageIndexConfigPtr& packIndexConfig)
{
    if (packIndexConfig->IsVirtual() || packIndexConfig->GetShardingType() == IndexConfig::IST_IS_SHARDING) {
        return true;
    }
    return false;
}

void LegacyIndexAccessoryReader::AddSharedRelations(const PackageIndexConfigPtr& packIndexConfig,
                                                    ShareSectionMap& shareMap)
{
    IndexConfig::IndexShardingType shardingType = packIndexConfig->GetShardingType();
    if (shardingType == IndexConfig::IST_IS_SHARDING) {
        return;
    }

    string sharedIndexName;
    if (packIndexConfig->IsVirtual()) {
        sharedIndexName = packIndexConfig->GetNonTruncateIndexName();
        assert(!sharedIndexName.empty());
        shareMap[sharedIndexName].push_back(packIndexConfig->GetIndexName());
    } else {
        sharedIndexName = packIndexConfig->GetIndexName();
    }

    if (shardingType == IndexConfig::IST_NO_SHARDING) {
        return;
    }
    const vector<IndexConfigPtr>& shardingIndexConfigs = packIndexConfig->GetShardingIndexConfigs();

    for (size_t i = 0; i < shardingIndexConfigs.size(); ++i) {
        shareMap[sharedIndexName].push_back(shardingIndexConfigs[i]->GetIndexName());
    }
}

bool LegacyIndexAccessoryReader::InitSectionReader(const IndexConfigPtr& indexConfig,
                                                   const PartitionDataPtr& partitionData, ShareSectionMap& shareMap)
{
    string indexName = indexConfig->GetIndexName();
    PackageIndexConfigPtr packIndexConfig = DYNAMIC_POINTER_CAST(PackageIndexConfig, indexConfig);
    assert(packIndexConfig.get() != NULL);

    AddSharedRelations(packIndexConfig, shareMap);

    if (UseSharedSectionReader(packIndexConfig)) {
        return true;
    }

    LegacySectionAttributeReaderImplPtr sectionReader = OpenSectionReader(packIndexConfig, partitionData);
    mSectionReaderVec.push_back(sectionReader);
    bool ret = mSectionReaderMap.insert(make_pair(indexName, mSectionReaderVec.size() - 1)).second;

    assert(ret);
    return ret;
}

LegacySectionAttributeReaderImplPtr
LegacyIndexAccessoryReader::OpenSectionReader(const config::PackageIndexConfigPtr& indexConfig,
                                              const PartitionDataPtr& partitionData)
{
    LegacySectionAttributeReaderImplPtr sectionReader(new LegacySectionAttributeReaderImpl);
    sectionReader->Open(indexConfig, partitionData);
    return sectionReader;
}

void LegacyIndexAccessoryReader::CloneSectionReaders(const SectionReaderVec& srcReaders,
                                                     SectionReaderVec& clonedReaders)
{
    clonedReaders.clear();
    for (size_t i = 0; i < srcReaders.size(); ++i) {
        LegacySectionAttributeReaderImpl* cloneReader =
            dynamic_cast<LegacySectionAttributeReaderImpl*>(srcReaders[i]->Clone());
        assert(cloneReader != NULL);
        clonedReaders.push_back(LegacySectionAttributeReaderImplPtr(cloneReader));
    }
}

bool LegacyIndexAccessoryReader::Open(const PartitionDataPtr& partitionData)
{
    ShareSectionMap shareMap;
    auto indexConfigs = mIndexSchema->CreateIterator(true);
    auto iter = indexConfigs->Begin();
    for (; iter != indexConfigs->End(); iter++) {
        const IndexConfigPtr& indexConfig = *iter;
        if (!CheckSectionConfig(indexConfig)) {
            continue;
        }
        if (!InitSectionReader(indexConfig, partitionData, shareMap)) {
            return false;
        }
    }

    ShareSectionMap::const_iterator it = shareMap.begin();
    for (; it != shareMap.end(); ++it) {
        const string& sharedIndexName = it->first;
        const ShareSectionIndexNames& indexNames = it->second;
        SectionReaderPosMap::const_iterator sit = mSectionReaderMap.find(sharedIndexName);
        assert(sit != mSectionReaderMap.end());
        for (size_t i = 0; i < indexNames.size(); ++i) {
            mSectionReaderMap.insert(make_pair(indexNames[i], sit->second));
        }
    }
    return true;
}
}} // namespace indexlib::index
