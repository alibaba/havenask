#include "indexlib/config/kkv_index_preference.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, KKVIndexPreference);

namespace {
struct KKVPreferenceParameter : public autil::legacy::Jsonizable {
public:
    void InitDefaultParam(IndexPreferenceType type)
    {
        {
            switch (type)
            {
            case ipt_perf:
            {
                hashParam = KKVIndexPreference::HashDictParam(
                        "dense", 50, true, false, false);
                skeyParam = KKVIndexPreference::SuffixKeyParam(false, "");
                valueParam = KKVIndexPreference::ValueParam(false, "");
                break;
            }
            case ipt_store:
            {
                hashParam = KKVIndexPreference::HashDictParam(
                        "separate_chain", 50, true, false, false);
                // TODO: support
                // skeyParam = KKVIndexPreference::SuffixKeyParam(true, "zstd");
                // valueParam = KKVIndexPreference::ValueParam(true, "zstd");
                skeyParam = KKVIndexPreference::SuffixKeyParam(false, "zstd");
                valueParam = KKVIndexPreference::ValueParam(false, "zstd");
                break;
            }
            default:
                assert(type == ipt_unknown);
                assert(false);
            }
        }
    }
    
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize(KKV_HASH_PARAM_NAME, hashParam, hashParam);
        json.Jsonize(KKV_SKEY_PARAM_NAME, skeyParam, skeyParam);
        json.Jsonize(KKV_VALUE_PARAM_NAME, valueParam, valueParam);
    }
    
    KKVIndexPreference::HashDictParam hashParam;
    KKVIndexPreference::SuffixKeyParam skeyParam;
    KKVIndexPreference::ValueParam valueParam;
};
};
    

KKVIndexPreference::KKVIndexPreference()
    : mValueInline(false)
{
    InitDefaultParams(mType);
}

KKVIndexPreference::~KKVIndexPreference() 
{
}

void KKVIndexPreference::Jsonize(Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON)
    {
        assert(mType != ipt_unknown);
        string typeStr = PreferenceTypeToStr(mType);
        json.Jsonize(PREFERENCE_TYPE_NAME, typeStr);
        KKVPreferenceParameter params;
        params.hashParam = mHashParam;
        params.skeyParam = mSkeyParam;
        params.valueParam = mValueParam;
        json.Jsonize(PREFERENCE_PARAMS_NAME, params);
        json.Jsonize(KKV_VALUE_INLINE, mValueInline);
    }
    else
    {
        string typeStr = PERF_PREFERENCE_TYPE;
        json.Jsonize(PREFERENCE_TYPE_NAME, typeStr, typeStr);
        IndexPreferenceType type = StrToPreferenceType(typeStr);
        if (type == ipt_unknown)
        {
            INDEXLIB_FATAL_ERROR(BadParameter,
                    "unknown paramter str[%s] for index_preference type",
                    typeStr.c_str());
        }
        mType = type;
        KKVPreferenceParameter params;
        params.InitDefaultParam(mType);
        json.Jsonize(PREFERENCE_PARAMS_NAME, params, params);
        mHashParam = params.hashParam;
        mSkeyParam = params.skeyParam;
        mValueParam = params.valueParam;
        json.Jsonize(KKV_VALUE_INLINE, mValueInline, mValueInline);
        Check();
    }
}

void KKVIndexPreference::Check() const
{
    mHashParam.Check();
    mSkeyParam.Check();
    mValueParam.Check();
}

void KKVIndexPreference::InitDefaultParams(IndexPreferenceType type)
{
    KKVPreferenceParameter defaultParam;
    defaultParam.InitDefaultParam(type);
    mHashParam = defaultParam.hashParam;
    mValueParam = defaultParam.valueParam;
    mSkeyParam = defaultParam.skeyParam;
}

IE_NAMESPACE_END(config);

