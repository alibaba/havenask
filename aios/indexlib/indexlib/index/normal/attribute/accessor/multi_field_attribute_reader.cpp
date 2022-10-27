#include <indexlib/util/thread_pool.h>
#include "indexlib/index/normal/attribute/accessor/multi_field_attribute_reader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_factory.h"
#include "indexlib/index/normal/attribute/accessor/lazy_load_attribute_reader_factory.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/lambda_work_item.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MultiFieldAttributeReader);

MultiFieldAttributeReader::MultiFieldAttributeReader(
        const AttributeSchemaPtr& attrSchema,
        AttributeMetrics* attributeMetrics,
        bool lazyLoad,
        int32_t initReaderThreadCount)
    : mAttrSchema(attrSchema)
    , mAttributeMetrics(attributeMetrics)
    , mEnableAccessCountors(false)
    , mLazyLoad(lazyLoad)
    , mInitReaderThreadCount(initReaderThreadCount)
{
}

MultiFieldAttributeReader::~MultiFieldAttributeReader() 
{
}

void MultiFieldAttributeReader::Open(
        const PartitionDataPtr& partitionData)
{
    InitAttributeReaders(partitionData);
}

const AttributeReaderPtr& MultiFieldAttributeReader::GetAttributeReader(
        const string& field) const
{
    AttributeReaderMap::const_iterator it = mAttrReaderMap.find(field);
    if (it != mAttrReaderMap.end())
    {
        if (likely(mAttributeMetrics != NULL && mEnableAccessCountors))
        {
            mAttributeMetrics->IncAccessCounter(field);
        }
        return it->second;
    }
    static AttributeReaderPtr retNullPtr;
    return retNullPtr;
}

void MultiFieldAttributeReader::MultiThreadInitAttributeReaders(
        const PartitionDataPtr& partitionData, int32_t threadNum)
{
    IE_LOG(INFO, "MultiThread InitAttributeReaders begin, threadNum[%d], mAttrReaderMap[%lu]",
           threadNum, mAttrReaderMap.size());
    if (!mAttrSchema)
    {
        return;
    }

    util::ThreadPool threadPool(threadNum, 1024);
    if (!threadPool.Start("indexIntAttrRdr")) {
        INDEXLIB_THROW(misc::RuntimeException, "create thread pool failed");
    }

    uint32_t count = mAttrSchema->GetAttributeCount();
    autil::ThreadMutex attrReaderMapLock;
    uint32_t actualCount = 0;
    
    auto attrConfigs = mAttrSchema->CreateIterator();
    auto iter = attrConfigs->Begin();
    for (; iter != attrConfigs->End(); iter++)
    {
        const AttributeConfigPtr &attrConfig = *iter;
        if (attrConfig->GetPackAttributeConfig() != NULL)
        {
            continue;
        }
        ++actualCount;
        auto* workItem = util::makeLambdaWorkItem(
                [this, &attrConfig, &partitionData, &attrReaderMapLock]() {
                    const string& attrName = attrConfig->GetAttrName();
                    AttributeReaderPtr attrReader = CreateAttributeReader(attrConfig, partitionData);
                    if (attrReader)
                    {
                        IE_LOG(INFO, "MultiThread InitAttribute [%s]", attrName.c_str());
                        autil::ScopedLock lock(attrReaderMapLock);
                        mAttrReaderMap.insert(make_pair(attrName, attrReader));
                    }
                });
        threadPool.PushWorkItem(workItem);
    }
    
    threadPool.Stop();
    IE_LOG(INFO, "MultiThread InitAttributeReaders end, attribute total[%d], actual[%d], mAttrReaderMap[%lu]",
           count, actualCount, mAttrReaderMap.size());
}

void MultiFieldAttributeReader::InitAttributeReaders(
        const PartitionDataPtr& partitionData)
{
    if (mInitReaderThreadCount > 1)
    {
        return MultiThreadInitAttributeReaders(partitionData, mInitReaderThreadCount);
    }
    if (mAttrSchema)
    {
        auto attrConfigs = mAttrSchema->CreateIterator();
        auto iter = attrConfigs->Begin();
        for (; iter != attrConfigs->End(); iter++)
        {
            const AttributeConfigPtr &attrConfig = *iter;
            if (attrConfig->GetPackAttributeConfig() != NULL)
            {
                continue;
            }
            InitAttributeReader(attrConfig, partitionData);
        }
    }
}

AttributeReaderPtr MultiFieldAttributeReader::CreateAttributeReader(
        const AttributeConfigPtr& attrConfig,
        const PartitionDataPtr& partitionData)
{
    AttributeReaderPtr attrReader;
    if (mLazyLoad)
    {
        attrReader = LazyLoadAttributeReaderFactory::CreateAttributeReader(
            attrConfig, partitionData, mAttributeMetrics);        
    }
    else
    {
        attrReader = AttributeReaderFactory::CreateAttributeReader(
            attrConfig, partitionData, mAttributeMetrics);
    }
    return attrReader;
}

void MultiFieldAttributeReader::InitAttributeReader(
        const AttributeConfigPtr& attrConfig,
        const PartitionDataPtr& partitionData)
{
    const string& attrName = attrConfig->GetAttrName();
    AttributeReaderPtr attrReader = CreateAttributeReader(attrConfig, partitionData);
    if (attrReader)
    {
        mAttrReaderMap.insert(make_pair(attrName, attrReader));
    }
}

void MultiFieldAttributeReader::AddAttributeReader(
        const string& attrName, const AttributeReaderPtr& attrReader)
{
    if (mAttrReaderMap.find(attrName) != mAttrReaderMap.end())
    {
        INDEXLIB_FATAL_ERROR(Duplicate, "attribute reader for [%s] already exist!",
                             attrName.c_str());
    }
    mAttrReaderMap.insert(make_pair(attrName, attrReader));
}

IE_NAMESPACE_END(index);

