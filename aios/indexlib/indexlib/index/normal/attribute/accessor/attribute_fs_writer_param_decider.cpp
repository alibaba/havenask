#include "indexlib/index/normal/attribute/accessor/attribute_fs_writer_param_decider.h"
#include "indexlib/index_define.h"

using namespace std;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributeFSWriterParamDecider);

FSWriterParam AttributeFSWriterParamDecider::MakeParam(
    const string& fileName)
{
    if (mOptions.IsOffline())
    {
        return FSWriterParam(false, false);
    }

    const OnlineConfig& onlineConfig = mOptions.GetOnlineConfig();
    if (!onlineConfig.onDiskFlushRealtimeIndex)
    {
        return FSWriterParam(false, false);
    }

    AttributeConfig::ConfigType attrConfType = mAttrConfig->GetConfigType();
    if (attrConfType != AttributeConfig::ct_normal
        && attrConfType != AttributeConfig::ct_virtual)
    {
        return FSWriterParam(false, false);
    }

    if (!mAttrConfig->IsAttributeUpdatable())
    {
        return FSWriterParam(false, false);
    }

    if (mAttrConfig->IsMultiValue() || mAttrConfig->GetFieldType() == ft_string)
    {
        return FSWriterParam(false, fileName == ATTRIBUTE_OFFSET_FILE_NAME);
    }
    return FSWriterParam(false, fileName == ATTRIBUTE_DATA_FILE_NAME);
}

IE_NAMESPACE_END(index);

