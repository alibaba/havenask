#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"
#include "indexlib/index/normal/attribute/accessor/single_attribute_segment_reader_for_merge.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index/segment_directory_base.h"
#include "indexlib/util/class_typed_factory.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, OfflineAttributeSegmentReaderContainer);

AttributeSegmentReaderPtr OfflineAttributeSegmentReaderContainer::NULL_READER
    = AttributeSegmentReaderPtr();

OfflineAttributeSegmentReaderContainer::OfflineAttributeSegmentReaderContainer(
        const config::IndexPartitionSchemaPtr& schema,
        const SegmentDirectoryBasePtr& segDir)
    : mSchema(schema)
    , mSegDir(segDir)
{
}

OfflineAttributeSegmentReaderContainer::~OfflineAttributeSegmentReaderContainer() 
{
}

AttributeSegmentReaderPtr OfflineAttributeSegmentReaderContainer::CreateOfflineSegmentReader(
    const AttributeConfigPtr& attrConfig, const SegmentData& segData)
{
    auto fieldType = attrConfig->GetFieldType();
    if (attrConfig->IsMultiValue() || fieldType == ft_string)
    {
        assert(false);
        return AttributeSegmentReaderPtr();
    }
    else
    {
        auto attrReader = ClassTypedFactory<SingleAttributeSegmentReaderForMergeBase,
            SingleAttributeSegmentReaderForMerge, const AttributeConfigPtr&>::GetInstance()
                              ->Create(fieldType, attrConfig);
        attrReader->Open(mSegDir, segData.GetSegmentInfo(), segData.GetSegmentId());
        AttributeSegmentReaderPtr ret(attrReader);
        return ret;
    }
}

const AttributeSegmentReaderPtr& OfflineAttributeSegmentReaderContainer::GetAttributeSegmentReader(
    const std::string& attrName, segmentid_t segId)
{
    const auto& attrSchema = mSchema->GetAttributeSchema();
    const auto& attrConfig = attrSchema->GetAttributeConfig(attrName);
    if (!attrConfig)
    {
        IE_LOG(ERROR, "attribute config [%s] not found in schema", attrName.c_str());
        return NULL_READER;
    }
    const auto& partData = mSegDir->GetPartitionData();
    auto segData = partData->GetSegmentData(segId);

    auto attrMapIter = mReaderMap.find(attrName);
    if (attrMapIter == mReaderMap.end())
    {
        attrMapIter = mReaderMap.insert(attrMapIter, { attrName, SegmentReaderMap() });
    }
    auto& segReaderMap = attrMapIter->second;
    auto readerIter = segReaderMap.find(segId);
    if (readerIter == segReaderMap.end())
    {
        auto reader = CreateOfflineSegmentReader(attrConfig, segData);
        readerIter = segReaderMap.insert(readerIter, { segId, reader });
    }
    return readerIter->second;
}

IE_NAMESPACE_END(index);

