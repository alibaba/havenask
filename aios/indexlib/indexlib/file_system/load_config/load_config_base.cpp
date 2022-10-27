#include <unordered_map>
#include "indexlib/file_system/load_config/load_config_base.h"
#include "indexlib/index_define.h"
#include "indexlib/file_system/load_config/mmap_load_strategy.h"
#include "indexlib/file_system/load_config/cache_load_strategy.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/misc/exception.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(config, LoadConfigBase);
LoadConfigBase::LoadConfigBase()
    : mRemote(false)
    , mDeploy(true)
{
    mLoadStrategy.reset(new MmapLoadStrategy);
}

LoadConfigBase::~LoadConfigBase() 
{
}

bool LoadConfigBase::EqualWith(const LoadConfigBase& other) const
{
    assert(other.mLoadStrategy);
    assert(mLoadStrategy);

    if (!(mWarmupStrategy == other.mWarmupStrategy))
    {
        return false;
    }

    if (!mLoadStrategy->EqualWith(other.mLoadStrategy))
    {
        return false;
    }

    if (mFilePatterns != other.mFilePatterns)
    {
        return false;
    }
    return true;
}

void LoadConfigBase::SetFilePatternString(
        const FilePatternStringVector& filePatterns)
{
    mFilePatterns = filePatterns;
    CreateRegularExpressionVector(mFilePatterns, mRegexVector);    
}

void LoadConfigBase::Check() const
{
    if (!mDeploy && !mRemote && mName != DISABLE_FIELDS_LOAD_CONFIG_NAME)
    {
        if (mName != DISABLE_FIELDS_LOAD_CONFIG_NAME &&
            mName != NOT_READY_OR_DELETE_FIELDS_CONFIG_NAME) {
            INDEXLIB_FATAL_ERROR(BadParameter, "loadConfig [%s] [deploy] and [remote] can not be false simultaneously",
                    mName.c_str());
        }
    }
    mLoadStrategy->Check();
}

void LoadConfigBase::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("file_patterns", mFilePatterns);
    json.Jsonize("name", mName, mName);
    json.Jsonize("remote", mRemote, mRemote);
    json.Jsonize("deploy", mDeploy, mDeploy);
    if (json.GetMode() == FROM_JSON)
    {
        string warmupStr;
        json.Jsonize("warmup_strategy", warmupStr,
                     WarmupStrategy::ToTypeString(WarmupStrategy::WARMUP_NONE));
        mWarmupStrategy.SetWarmupType(WarmupStrategy::FromTypeString(warmupStr));
    }
    else
    {
        string warmupStr = WarmupStrategy::ToTypeString(
                mWarmupStrategy.GetWarmupType());
        json.Jsonize("warmup_strategy", warmupStr);
    }
    if (json.GetMode() == FROM_JSON)
    {
        CreateRegularExpressionVector(mFilePatterns, mRegexVector);
    }

    string loadStrategyName = GetLoadStrategyName();
    if (loadStrategyName == READ_MODE_GLOBAL_CACHE)
    {
        loadStrategyName = READ_MODE_CACHE;
    }
    json.Jsonize("load_strategy", loadStrategyName, READ_MODE_MMAP);
    if (json.GetMode() == FROM_JSON)
    {
        if (loadStrategyName == READ_MODE_MMAP)
        {
            MmapLoadStrategyPtr loadStrategy(new MmapLoadStrategy);
            json.Jsonize("load_strategy_param", *loadStrategy, *loadStrategy);
            mLoadStrategy = loadStrategy;
        }
        else if (loadStrategyName == READ_MODE_CACHE)
        {
            CacheLoadStrategyPtr loadStrategy(new CacheLoadStrategy());
            json.Jsonize("load_strategy_param", *loadStrategy, *loadStrategy);
            mLoadStrategy = loadStrategy;
        }
        else
        {
            INDEXLIB_THROW(misc::BadParameterException,
                           "Invalid parameter for load_strategy[%s]",
                           loadStrategyName.c_str());
        }
    }
    else // TO_JSON
    {
        if (mLoadStrategy)
        {
            json.Jsonize("load_strategy_param", *mLoadStrategy);
        }
    }
}

void LoadConfigBase::CreateRegularExpressionVector(
        const FilePatternStringVector& patternVector,
        RegularExpressionVector& regexVector)
{
    regexVector.clear();

    for (size_t i = 0; i < patternVector.size(); ++i)
    {
        RegularExpressionPtr regex(new RegularExpression);
        string pattern = BuiltInPatternToRegexPattern(patternVector[i]);
        if (!regex->Init(pattern))
        {
            INDEXLIB_THROW(misc::BadParameterException,
                           "Invalid parameter for regular expression pattern[%s], error[%s]",
                           patternVector[i].c_str(),
                           regex->GetLatestErrMessage().c_str());
        }
        regexVector.push_back(regex);
    }
}

bool LoadConfigBase::Match(const std::string& filePath) const
{
    for (size_t i = 0, size = mRegexVector.size(); i < size; ++i)
    {
        if (mRegexVector[i]->Match(filePath))
        {
            return true;
        }
    }
    return false;
}

const string& LoadConfigBase::BuiltInPatternToRegexPattern(const std::string& builtInPattern)
{
#define SWITCH_RT_PREFIX "(" FILE_SYSTEM_ROOT_LINK_NAME "(@[0-9]+)?/" RT_INDEX_PARTITION_DIR_NAME "/)?"
#define PATCH_INDEX_PREFIX "(" PARTITION_PATCH_DIR_PREFIX "_[0-9]+/)?"

#define SEGMENT_P "^" SWITCH_RT_PREFIX PATCH_INDEX_PREFIX "segment_[0-9]+(_level_[0-9]+)?"
#define SEGMENT_SUB_P SEGMENT_P "(/sub_segment)?"

    static const unordered_map<string, string> builtInPatternMap =
    {
        {"_PATCH_",     SEGMENT_SUB_P "/attribute/\\w+/(\\w+/)?[0-9]+_[0-9]+\\.patch"},
        {"_ATTRIBUTE_", SEGMENT_SUB_P "/attribute/"},
        {"_INDEX_",     SEGMENT_SUB_P "/index/"},
        {"_SUMMARY_",   SEGMENT_SUB_P "/summary/"},
        {"_SUMMARY_DATA_", SEGMENT_SUB_P "/summary/" SUMMARY_DATA_FILE_NAME "$"},
        {"_KV_KEY_",    SEGMENT_P "/index/\\w+/" KV_KEY_FILE_NAME},
        {"_KV_VALUE_",  SEGMENT_P "/index/\\w+/" KV_VALUE_FILE_NAME},
        {"_KKV_PKEY_",  SEGMENT_P "/index/\\w+/" PREFIX_KEY_FILE_NAME},
        {"_KKV_SKEY_",  SEGMENT_P "/index/\\w+/" SUFFIX_KEY_FILE_NAME},
        {"_KKV_VALUE_", SEGMENT_P "/index/\\w+/" KKV_VALUE_FILE_NAME},
    };

#undef SWITCH_RT_PREFIX
#undef PATCH_INDEX_PREFIX

    const auto& it = builtInPatternMap.find(builtInPattern);
    if (it != builtInPatternMap.end())
    {
        return it->second;
    }
    return builtInPattern;
}

IE_NAMESPACE_END(file_system);

