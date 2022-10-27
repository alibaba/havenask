#include <autil/StringUtil.h>
#include "indexlib/config/impl/offline_config_impl.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, OfflineConfigImpl);

OfflineConfigImpl::OfflineConfigImpl()
    : mNeedRebuildAdaptiveBitmap(false)
    , mNeedRebuildTruncateIndex(false)
{}

OfflineConfigImpl::OfflineConfigImpl(const OfflineConfigImpl& other)
    : mNeedRebuildAdaptiveBitmap(other.mNeedRebuildAdaptiveBitmap)
    , mNeedRebuildTruncateIndex(other.mNeedRebuildTruncateIndex)
    , mLoadConfigList(other.mLoadConfigList)
    , mModuleInfos(other.mModuleInfos)
{}

OfflineConfigImpl::~OfflineConfigImpl() {}

void OfflineConfigImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("modules", mModuleInfos, ModuleInfos());

}

void OfflineConfigImpl::Check() const
{
}

void OfflineConfigImpl::operator = (const OfflineConfigImpl& other)
{
    mNeedRebuildTruncateIndex = other.mNeedRebuildTruncateIndex;
    mNeedRebuildAdaptiveBitmap = other.mNeedRebuildAdaptiveBitmap;
    mLoadConfigList = other.mLoadConfigList;
    mModuleInfos = other.mModuleInfos;
}

void OfflineConfigImpl::SetLoadConfigList(const LoadConfigList& loadConfigList)
{
    mLoadConfigList = loadConfigList;
}

IE_NAMESPACE_END(config);

