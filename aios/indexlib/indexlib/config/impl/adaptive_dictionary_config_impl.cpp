#include "indexlib/config/impl/adaptive_dictionary_config_impl.h"
#include "indexlib/misc/exception.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/config_define.h"

using namespace std;
using namespace autil::legacy;
IE_NAMESPACE_USE(misc);


static const string DOC_FREQUENCY_DICT_TYPE = string("DOC_FREQUENCY");
static const string PERCENT_DICT_TYPE = string("PERCENT");
static const string INDEX_SIZE_DICT_TYPE = string("INDEX_SIZE");
static const string UNKOWN_DICT_TYPE = string("UNKOWN_DICT_TYPE");

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, AdaptiveDictionaryConfigImpl);

AdaptiveDictionaryConfigImpl::AdaptiveDictionaryConfigImpl()
    : mThreshold(INVALID_ADAPTIVE_THRESHOLD)
{
}

AdaptiveDictionaryConfigImpl::AdaptiveDictionaryConfigImpl(
        const std::string& ruleName, 
        const std::string& dictType,
        int32_t threshold)
    : mRuleName(ruleName)
    , mThreshold(threshold)
{
    mDictType = FromTypeString(dictType);
    CheckThreshold();
}

AdaptiveDictionaryConfigImpl::~AdaptiveDictionaryConfigImpl() 
{
}

AdaptiveDictionaryConfig::DictType AdaptiveDictionaryConfigImpl::FromTypeString(
        const string& typeStr)
{
    if (typeStr == DOC_FREQUENCY_DICT_TYPE)
    {
        return AdaptiveDictionaryConfig::DictType::DF_ADAPTIVE;
    }

    if (typeStr == PERCENT_DICT_TYPE)
    {
        return AdaptiveDictionaryConfig::DictType::PERCENT_ADAPTIVE;
    }

    if (typeStr == INDEX_SIZE_DICT_TYPE)
    {
        return AdaptiveDictionaryConfig::DictType::SIZE_ADAPTIVE;
    }

    stringstream ss;
    ss << "unsupported dict_type [" << typeStr << "]" 
       << "of adaptive_dictionary [" << mRuleName << "]";
    INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    return AdaptiveDictionaryConfig::DictType::UNKOWN_TYPE;
}

string AdaptiveDictionaryConfigImpl::ToTypeString(AdaptiveDictionaryConfig::DictType type)
{
    if (type == AdaptiveDictionaryConfig::DictType::DF_ADAPTIVE)
    {
        return DOC_FREQUENCY_DICT_TYPE;
    }

    if (type == AdaptiveDictionaryConfig::DictType::PERCENT_ADAPTIVE)
    {
        return PERCENT_DICT_TYPE;
    }

    if (type == AdaptiveDictionaryConfig::DictType::SIZE_ADAPTIVE)
    {
        return INDEX_SIZE_DICT_TYPE;
    }
    stringstream ss;
    ss << "unknown enum dict type:" << type;
    INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    return UNKOWN_DICT_TYPE;
}

void AdaptiveDictionaryConfigImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize(ADAPTIVE_DICTIONARY_NAME, mRuleName);
    int32_t defaultThreshold = INVALID_ADAPTIVE_THRESHOLD;
    json.Jsonize(ADAPTIVE_DICTIONARY_THRESHOLD, mThreshold,
                 defaultThreshold);
    if (json.GetMode() == Jsonizable::TO_JSON)
    {
        string typeStr = ToTypeString(mDictType);
        json.Jsonize(ADAPTIVE_DICTIONARY_TYPE, typeStr);
    }
    else
    {
        string typeStr;
        json.Jsonize(ADAPTIVE_DICTIONARY_TYPE, typeStr);
        mDictType = FromTypeString(typeStr);
        CheckThreshold();
    }
}

void AdaptiveDictionaryConfigImpl::AssertEqual(
        const AdaptiveDictionaryConfigImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mRuleName, other.mRuleName,
                           "Adaptive dictionary name doesn't match");

    IE_CONFIG_ASSERT_EQUAL(mDictType, other.mDictType,
                           "Adaptive dict_type doesn't match");

    IE_CONFIG_ASSERT_EQUAL(mThreshold, other.mThreshold,
                           "Adaptive threshold doesn't match");
}

void AdaptiveDictionaryConfigImpl::CheckThreshold() const
{
    if (mDictType == AdaptiveDictionaryConfig::DictType::DF_ADAPTIVE)
    {
        if (mThreshold <= 0)
        {
            INDEXLIB_FATAL_ERROR(Schema, "adaptive threshold should"
                    " greater than 0 when dict_type is DOC_FREQUENCY");
        }
    }

    if (mDictType == AdaptiveDictionaryConfig::DictType::PERCENT_ADAPTIVE)
    {
        if (mThreshold > 100 || mThreshold < 0)
        {
            INDEXLIB_FATAL_ERROR(Schema, "adaptive threshold should"
                    " be [0~100] when dict_type is PERCENT");
        }
    }
}

IE_NAMESPACE_END(config);

