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
#include "indexlib/index/normal/attribute/accessor/multi_field_attribute_reader.h"

#include "autil/LambdaWorkItem.h"
#include "autil/ThreadPool.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_factory.h"
#include "indexlib/index/normal/attribute/attribute_metrics.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, MultiFieldAttributeReader);

MultiFieldAttributeReader::MultiFieldAttributeReader(const AttributeSchemaPtr& attrSchema,
                                                     AttributeMetrics* attributeMetrics,
                                                     config::ReadPreference readPreference,
                                                     int32_t initReaderThreadCount)
    : mAttrSchema(attrSchema)
    , mAttributeMetrics(attributeMetrics)
    , mEnableAccessCountors(false)
    , mReadPreference(readPreference)
    , mInitReaderThreadCount(initReaderThreadCount)
{
}

MultiFieldAttributeReader::~MultiFieldAttributeReader() {}

void MultiFieldAttributeReader::Open(const PartitionDataPtr& partitionData, const MultiFieldAttributeReader* hintReader)
{
    InitAttributeReaders(partitionData, hintReader);
}

const AttributeReaderPtr& MultiFieldAttributeReader::GetAttributeReader(const string& field) const
{
    AttributeReaderMap::const_iterator it = mAttrReaderMap.find(field);
    if (it != mAttrReaderMap.end()) {
        if (likely(mAttributeMetrics != NULL && mEnableAccessCountors)) {
            mAttributeMetrics->IncAccessCounter(field);
        }
        return it->second;
    }
    static AttributeReaderPtr retNullPtr;
    return retNullPtr;
}

void MultiFieldAttributeReader::EnableAccessCountors(bool needReportTemperatureDetail)
{
    mEnableAccessCountors = true;
    if (needReportTemperatureDetail) {
        for (auto iter = mAttrReaderMap.begin(); iter != mAttrReaderMap.end(); iter++) {
            iter->second->EnableAccessCountors();
        }
    }
}

void MultiFieldAttributeReader::MultiThreadInitAttributeReaders(const PartitionDataPtr& partitionData,
                                                                int32_t threadNum,
                                                                const MultiFieldAttributeReader* hintReader)
{
    IE_LOG(INFO, "MultiThread InitAttributeReaders begin, threadNum[%d], mAttrReaderMap[%lu]", threadNum,
           mAttrReaderMap.size());
    if (!mAttrSchema) {
        return;
    }

    autil::ThreadPool threadPool(threadNum, 1024, true);
    if (!threadPool.start("indexIntAttrRdr")) {
        INDEXLIB_THROW(util::RuntimeException, "create thread pool failed");
    }

    uint32_t count = mAttrSchema->GetAttributeCount();
    autil::ThreadMutex attrReaderMapLock;
    uint32_t actualCount = 0;

    auto attrConfigs = mAttrSchema->CreateIterator();
    auto iter = attrConfigs->Begin();
    for (; iter != attrConfigs->End(); iter++) {
        const AttributeConfigPtr& attrConfig = *iter;
        if (attrConfig->GetPackAttributeConfig() != NULL) {
            continue;
        }
        ++actualCount;
        auto* workItem = new autil::LambdaWorkItem([this, &attrConfig, &partitionData, &attrReaderMapLock]() {
            const string& attrName = attrConfig->GetAttrName();
            AttributeReaderPtr attrReader = AttributeReaderFactory::CreateAttributeReader(
                attrConfig, partitionData, mReadPreference, mAttributeMetrics, nullptr);

            if (attrReader) {
                IE_LOG(INFO, "MultiThread InitAttribute [%s]", attrName.c_str());
                autil::ScopedLock lock(attrReaderMapLock);
                mAttrReaderMap.insert(make_pair(attrName, attrReader));
            }
        });
        if (ThreadPool::ERROR_NONE != threadPool.pushWorkItem(workItem)) {
            ThreadPool::dropItemIgnoreException(workItem);
        }
    }

    threadPool.stop();
    IE_LOG(INFO, "MultiThread InitAttributeReaders end, attribute total[%d], actual[%d], mAttrReaderMap[%lu]", count,
           actualCount, mAttrReaderMap.size());
}

void MultiFieldAttributeReader::InitAttributeReaders(const PartitionDataPtr& partitionData,
                                                     const MultiFieldAttributeReader* hintReader)
{
    if (mInitReaderThreadCount > 1) {
        return MultiThreadInitAttributeReaders(partitionData, mInitReaderThreadCount, hintReader);
    }
    if (mAttrSchema) {
        auto attrConfigs = mAttrSchema->CreateIterator();
        auto iter = attrConfigs->Begin();
        for (; iter != attrConfigs->End(); iter++) {
            const AttributeConfigPtr& attrConfig = *iter;
            if (attrConfig->GetPackAttributeConfig() != NULL) {
                continue;
            }
            InitAttributeReader(attrConfig, partitionData, hintReader);
        }
    }
}

void MultiFieldAttributeReader::InitAttributeReader(const AttributeConfigPtr& attrConfig,
                                                    const PartitionDataPtr& partitionData,
                                                    const MultiFieldAttributeReader* hintReader)
{
    const string& attrName = attrConfig->GetAttrName();
    AttributeReader* hintAttributeReader = nullptr;
    if (hintReader != nullptr) {
        auto iter = hintReader->mAttrReaderMap.find(attrName);
        if (iter != hintReader->mAttrReaderMap.end()) {
            hintAttributeReader = iter->second.get();
        }
    }
    AttributeReaderPtr attrReader = AttributeReaderFactory::CreateAttributeReader(
        attrConfig, partitionData, mReadPreference, mAttributeMetrics, hintAttributeReader);
    if (attrReader) {
        mAttrReaderMap.insert(make_pair(attrName, attrReader));
    }
}

void MultiFieldAttributeReader::AddAttributeReader(const string& attrName, const AttributeReaderPtr& attrReader)
{
    if (mAttrReaderMap.find(attrName) != mAttrReaderMap.end()) {
        INDEXLIB_FATAL_ERROR(Duplicate, "attribute reader for [%s] already exist!", attrName.c_str());
    }
    mAttrReaderMap.insert(make_pair(attrName, attrReader));
}
}} // namespace indexlib::index
