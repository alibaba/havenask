#ifndef __INDEXLIB_VIRTUAL_ATTRIBUTE_DATA_APPENDER_H
#define __INDEXLIB_VIRTUAL_ATTRIBUTE_DATA_APPENDER_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, AttributeSchema);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(common, AttributeValueInitializer);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(index);

class VirtualAttributeDataAppender
{
public:
    VirtualAttributeDataAppender(
            const config::AttributeSchemaPtr& virtualAttrSchema);
    ~VirtualAttributeDataAppender();

public:
    void AppendData(const index_base::PartitionDataPtr& partitionData);

public:
    static void PrepareVirtualAttrData(
        const config::IndexPartitionSchemaPtr& rtSchema,
        const index_base::PartitionDataPtr& partData);

private:
    void AppendAttributeData(
            const index_base::PartitionDataPtr& partitionData,
            const config::AttributeConfigPtr& attrConfig);
    
    void AppendOneSegmentData(const config::AttributeConfigPtr& attrConfig, 
                              const index_base::SegmentData& segData, 
                              const common::AttributeValueInitializerPtr& initializer);

    bool CheckDataExist(const config::AttributeConfigPtr& attrConfig, 
                        const file_system::DirectoryPtr& attrDataDirectory);
                 
private:
    config::AttributeSchemaPtr mVirtualAttrSchema;
    autil::mem_pool::Pool mPool;

private:
    friend class VirtualAttributeDataAppenderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VirtualAttributeDataAppender);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_VIRTUAL_ATTRIBUTE_DATA_APPENDER_H
