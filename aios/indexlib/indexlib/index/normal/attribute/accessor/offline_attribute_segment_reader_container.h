#ifndef __INDEXLIB_OFFLINE_ATTRIBUTE_SEGMENT_READER_CONTAINER_H
#define __INDEXLIB_OFFLINE_ATTRIBUTE_SEGMENT_READER_CONTAINER_H

#include <tr1/memory>
#include <unordered_map>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(index, SegmentDirectoryBase);
DECLARE_REFERENCE_CLASS(index, AttributeSegmentReader);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);

IE_NAMESPACE_BEGIN(index);


class OfflineAttributeSegmentReaderContainer
{
private:
    using SegmentReaderMap = std::unordered_map<segmentid_t, AttributeSegmentReaderPtr>;
    using AttrSegmentReaderMap = std::unordered_map<std::string, SegmentReaderMap>;

public:
    OfflineAttributeSegmentReaderContainer(
        const config::IndexPartitionSchemaPtr& schema,
        const SegmentDirectoryBasePtr& segDir);
    ~OfflineAttributeSegmentReaderContainer();

public:
    const AttributeSegmentReaderPtr& GetAttributeSegmentReader(
        const std::string& attrName, segmentid_t segId);

private:
    AttributeSegmentReaderPtr CreateOfflineSegmentReader(
        const config::AttributeConfigPtr& attrConfig, const index_base::SegmentData& segData);

private:
    static AttributeSegmentReaderPtr NULL_READER;

private:
    config::IndexPartitionSchemaPtr mSchema;
    SegmentDirectoryBasePtr mSegDir;
    AttrSegmentReaderMap mReaderMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OfflineAttributeSegmentReaderContainer);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_OFFLINE_ATTRIBUTE_SEGMENT_READER_CONTAINER_H
