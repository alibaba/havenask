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
#include "indexlib/index/normal/attribute/accessor/multi_pack_attribute_reader.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, MultiPackAttributeReader);

PackAttributeReaderPtr MultiPackAttributeReader::RET_EMPTY_PTR = PackAttributeReaderPtr();

MultiPackAttributeReader::MultiPackAttributeReader(const AttributeSchemaPtr& attrSchema,
                                                   AttributeMetrics* attributeMetrics)
    : mAttrSchema(attrSchema)
    , mAttributeMetrics(attributeMetrics)
{
}

MultiPackAttributeReader::~MultiPackAttributeReader() {}

void MultiPackAttributeReader::Open(const index_base::PartitionDataPtr& partitionData,
                                    const MultiPackAttributeReader* hintReader)
{
    assert(partitionData);
    if (mAttrSchema) {
        size_t packAttrCount = mAttrSchema->GetPackAttributeCount();
        mPackAttrReaders.resize(packAttrCount);

        auto packAttrConfigs = mAttrSchema->CreatePackAttrIterator();
        auto packIter = packAttrConfigs->Begin();
        for (; packIter != packAttrConfigs->End(); packIter++) {
            const PackAttributeConfigPtr& packConfig = *packIter;
            const string& packName = packConfig->GetPackName();
            PackAttributeReaderPtr packAttrReader;
            PackAttributeReader* hintPackAttributeReader = nullptr;
            size_t packId = packConfig->GetPackAttrId();
            if (hintReader) {
                assert(hintReader->mPackAttrReaders.size() == mPackAttrReaders.size());
                hintPackAttributeReader = hintReader->mPackAttrReaders[packId].get();
            }
            packAttrReader.reset(new PackAttributeReader(mAttributeMetrics));
            packAttrReader->Open(packConfig, partitionData, hintPackAttributeReader);
            mPackNameToIdxMap[packName] = packId;
            mPackAttrReaders[packId] = packAttrReader;
        }
    }
}

const PackAttributeReaderPtr& MultiPackAttributeReader::GetPackAttributeReader(const string& packAttrName) const
{
    NameToIdxMap::const_iterator it = mPackNameToIdxMap.find(packAttrName);
    if (it == mPackNameToIdxMap.end()) {
        return RET_EMPTY_PTR;
    }
    return GetPackAttributeReader(it->second);
}

const PackAttributeReaderPtr& MultiPackAttributeReader::GetPackAttributeReader(packattrid_t packAttrId) const
{
    if (packAttrId == INVALID_PACK_ATTRID || packAttrId >= mPackAttrReaders.size()) {
        return RET_EMPTY_PTR;
    }
    return mPackAttrReaders[packAttrId];
}
}} // namespace indexlib::index
