#include "indexlib/config/online_config.h"
#include "indexlib/config/impl/online_config_impl.h"
#include "indexlib/index_define.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/file_system/load_config/cache_load_strategy.h"

using namespace std;
using namespace autil::legacy;

IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, OnlineConfig);

OnlineConfig::OnlineConfig()
{
    mImpl.reset(new OnlineConfigImpl);
}

OnlineConfig::OnlineConfig(const OnlineConfig& other)
    : OnlineConfigBase(other)
{ 
    mImpl.reset(new OnlineConfigImpl(*other.mImpl));
}

OnlineConfig::~OnlineConfig() {}

void OnlineConfig::Jsonize(Jsonizable::JsonWrapper& json)
{
    OnlineConfigBase::Jsonize(json);
    mImpl->Jsonize(json);
    if (json.GetMode() == FROM_JSON)
    {
        RewriteLoadConfig();
    }
}

void OnlineConfig::Check() const
{
    OnlineConfigBase::Check();
    mImpl->Check();
}

void OnlineConfig::operator=(const OnlineConfig& other)
{
    *(OnlineConfigBase*)this = (const OnlineConfigBase&)other;
    *mImpl = *(other.mImpl);
}


void OnlineConfig::SetEnableRedoSpeedup(bool enableFlag)
{
    return mImpl->SetEnableRedoSpeedup(enableFlag);
}

bool OnlineConfig::IsRedoSpeedupEnabled() const
{
    return mImpl->IsRedoSpeedupEnabled();
}

void OnlineConfig::SetVersionTsAlignment(int64_t ts)
{
    mImpl->SetVersionTsAlignment(ts);
}

int64_t OnlineConfig::GetVersionTsAlignment() const
{
    return mImpl->GetVersionTsAlignment();
}

bool OnlineConfig::IsValidateIndexEnabled() const
{
    return mImpl->IsValidateIndexEnabled();
}

bool OnlineConfig::IsReopenRetryOnIOExceptionEnabled() const
{
    return mImpl->IsReopenRetryOnIOExceptionEnabled();
}

DisableFieldsConfig& OnlineConfig::GetDisableFieldsConfig()
{
    return mImpl->GetDisableFieldsConfig();
}

const DisableFieldsConfig& OnlineConfig::GetDisableFieldsConfig() const
{
    return mImpl->GetDisableFieldsConfig();
}

void OnlineConfig::SetInitReaderThreadCount(int32_t initReaderThreadCount)
{
    mImpl->SetInitReaderThreadCount(initReaderThreadCount);
}

int32_t OnlineConfig::GetInitReaderThreadCount() const
{
    return mImpl->GetInitReaderThreadCount();
}

void OnlineConfig::RewriteLoadConfig()
{
    if (!mImpl->GetDisableFieldsConfig().rewriteLoadConfig)
    {
        return;
    }

    for (const auto& loadConfig : loadConfigList.GetLoadConfigs())
    {
        if (loadConfig.GetName() == DISABLE_FIELDS_LOAD_CONFIG_NAME)
        {
            IE_LOG(ERROR, "duplicate load config [%s]: [%s] exist in loadConfigList",
                   loadConfig.GetName().c_str(), ToJsonString(loadConfig).c_str());
        }
    }

    if (mImpl->GetDisableFieldsConfig().attributes.size() > 0 ||
        mImpl->GetDisableFieldsConfig().packAttributes.size() > 0 ||
        mImpl->GetDisableFieldsConfig().indexes.size() > 0)
    {
        LoadConfig loadConfig = CreateDisableLoadConfig(
                DISABLE_FIELDS_LOAD_CONFIG_NAME,
                mImpl->GetDisableFieldsConfig().indexes,
                mImpl->GetDisableFieldsConfig().attributes,
                mImpl->GetDisableFieldsConfig().packAttributes);
        loadConfigList.PushFront(loadConfig);
    }
}

LoadConfig OnlineConfig::CreateDisableLoadConfig(const string& name,
                                                 const set<string>& indexes,
                                                 const set<string>& attributes,
                                                 const set<string>& packAttrs)
{
    vector<string> indexVec(indexes.begin(), indexes.end());
    vector<string> attrVec(attributes.begin(), attributes.end());
    vector<string> packVec(packAttrs.begin(), packAttrs.end());
    return CreateDisableLoadConfig(name, indexVec, attrVec, packVec);
}

LoadConfig OnlineConfig::CreateDisableLoadConfig(const string& name,
                                                 const vector<string>& indexes,
                                                 const vector<string>& attributes,
                                                 const vector<string>& packAttrs)
{
#define SEGMENT_P "segment_[0-9]+(_level_[0-9]+)?(/sub_segment)?"
    LoadConfig loadConfig;
    loadConfig.SetLoadStrategyPtr(LoadStrategyPtr(new CacheLoadStrategy()));
    loadConfig.SetRemote(false);
    loadConfig.SetDeploy(false);
    loadConfig.SetName(name);
    vector<string> filePatterns;
    for (const string& attribute : attributes)
    {
        filePatterns.push_back(string(SEGMENT_P "/attribute/") + attribute + "($|/)");
    }
    for (const string& packAttribute : packAttrs)
    {
        filePatterns.push_back(string(SEGMENT_P "/attribute/") + packAttribute + "($|/)");
    }
    for (const string& index : indexes)
    {
        filePatterns.push_back(string(SEGMENT_P "/" INDEX_DIR_NAME "/") + index + "($|/)");
    }
    loadConfig.SetFilePatternString(filePatterns);
    loadConfig.Check();
#undef SEGMENT_P    
    return loadConfig;
}


IE_NAMESPACE_END(config);

