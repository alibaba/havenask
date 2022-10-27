#include <unordered_map>
#include "indexlib/index_define.h"
#include "indexlib/config/package_file_tag_config.h"
#include "indexlib/misc/exception.h"

using namespace std;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, PackageFileTagConfig);

PackageFileTagConfig::PackageFileTagConfig() 
{
}

PackageFileTagConfig::~PackageFileTagConfig() 
{
}

void PackageFileTagConfig::Init()
{
    for (const auto& filePattern : mFilePatterns)
    {
        RegularExpressionPtr regex(new RegularExpression);
        if (!regex->Init(BuiltInPatternToRegexPattern(filePattern)))
        {
            INDEXLIB_FATAL_ERROR(BadParameter,
                           "Invalid parameter for regular expression pattern[%s], error[%s]",
                           filePattern.c_str(),
                           regex->GetLatestErrMessage().c_str());
        }
        mRegexVec.push_back(regex);
    }
}

void PackageFileTagConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("file_patterns", mFilePatterns, mFilePatterns);
    json.Jsonize("tag", mTag, mTag);
    if (json.GetMode() == FROM_JSON)
    {
        Init();
    }
}

bool PackageFileTagConfig::Match(const string& filePath) const
{
    for (auto& regex : mRegexVec)
    {
        if (regex->Match(filePath))
        {
            return true;
        }
    }
    return false;
}

const string& PackageFileTagConfig::BuiltInPatternToRegexPattern(const string& builtInPattern)
{
#define SEGMENT_P "^segment_[0-9]+_level_[0-9]+"
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
#undef SEGMENT_P
#undef SEGMENT_SUB_P

    const auto& it = builtInPatternMap.find(builtInPattern);
    if (it != builtInPatternMap.end())
    {
        return it->second;
    }
    return builtInPattern;
}

IE_NAMESPACE_END(config);

