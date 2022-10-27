#ifndef __INDEXLIB_ATTRIBUTE_FS_WRITER_PARAM_DECIDER_H
#define __INDEXLIB_ATTRIBUTE_FS_WRITER_PARAM_DECIDER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/fs_writer_param_decider.h"

IE_NAMESPACE_BEGIN(index);

class AttributeFSWriterParamDecider : public FSWriterParamDecider
{
public:
    AttributeFSWriterParamDecider(
        const config::AttributeConfigPtr& attrConfig,
        const config::IndexPartitionOptions& options)
        : mAttrConfig(attrConfig)
        , mOptions(options)
    {
        assert(mAttrConfig);
    }
    
    ~AttributeFSWriterParamDecider() {}
    
public:
    file_system::FSWriterParam MakeParam(
        const std::string& fileName) override;

private:
    config::AttributeConfigPtr mAttrConfig;
    config::IndexPartitionOptions mOptions;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeFSWriterParamDecider);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_FS_WRITER_PARAM_DECIDER_H
