#include "indexlib/config/kv_index_preference.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, KVIndexPreference);

namespace {
struct KVPreferenceParameter : public autil::legacy::Jsonizable {
public:
    void InitDefaultParam(IndexPreferenceType type)
    {
        switch (type)
        {
        case ipt_perf:
        {
            hashParam = KVIndexPreference::HashDictParam("dense");
            valueParam = KVIndexPreference::ValueParam(false, "");
            break;
        }
        case ipt_store:
        {
            hashParam = KVIndexPreference::HashDictParam("cuckoo", 80);
            // TODO: valueParam = KVIndexPreference::ValueParam(true, "zstd");
            valueParam = KVIndexPreference::ValueParam(false, "zstd");
            break;
        }
        default:
            assert(type == ipt_unknown);
            assert(false);
        }
    }
    
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize(KV_HASH_PARAM_NAME, hashParam, hashParam);
        json.Jsonize(KV_VALUE_PARAM_NAME, valueParam, valueParam);
    }
    
    KVIndexPreference::HashDictParam hashParam;
    KVIndexPreference::ValueParam valueParam;
};
};

void KVIndexPreference::ValueParam::Jsonize(JsonWrapper& json)
{
    json.Jsonize("encode", mEncode, mEncode);
    json.Jsonize("file_compressor", mFileCompType, mFileCompType);
    json.Jsonize("file_compressor_buffer_size", mFileCompBuffSize, mFileCompBuffSize);
    json.Jsonize("fix_len_auto_inline", mFixLenAutoInline, mFixLenAutoInline);
}

void KVIndexPreference::ValueParam::Check() const
{
    if (mFileCompType.empty())
    {
        return;
    }

    if (mFileCompType != "snappy" && mFileCompType != "lz4" &&
        mFileCompType != "lz4hc" && mFileCompType != "zlib" &&
        mFileCompType != "zstd")
    {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "unsupported file_compressor [%s]!", mFileCompType.c_str());
    }

    size_t alignedSize = 1;
    while (alignedSize < mFileCompBuffSize)
    {
        alignedSize <<= 1;
    }
    if (alignedSize != mFileCompBuffSize)
    {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "mFileCompBuffSize [%lu] must be 2^n!", mFileCompBuffSize);
    }
}

KVIndexPreference::KVIndexPreference()
    : mType(ipt_perf)
{
    InitDefaultParams(mType);
}

KVIndexPreference::~KVIndexPreference() 
{
}

void KVIndexPreference::Jsonize(Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON)
    {
        assert(mType != ipt_unknown);
        string typeStr = PreferenceTypeToStr(mType);
        json.Jsonize(PREFERENCE_TYPE_NAME, typeStr);
        KVPreferenceParameter params;
        params.hashParam = mHashParam;
        params.valueParam = mValueParam;
        json.Jsonize(PREFERENCE_PARAMS_NAME, params);
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
        KVPreferenceParameter params;
        params.InitDefaultParam(mType);
        json.Jsonize(PREFERENCE_PARAMS_NAME, params, params);
        mHashParam = params.hashParam;
        mValueParam = params.valueParam;
        Check();
    }
}

void KVIndexPreference::Check() const
{
    mHashParam.Check();
    mValueParam.Check();
}

void KVIndexPreference::InitDefaultParams(IndexPreferenceType type)
{
    KVPreferenceParameter defaultParam;
    defaultParam.InitDefaultParam(type);
    mHashParam = defaultParam.hashParam;
    mValueParam = defaultParam.valueParam;
}

IndexPreferenceType KVIndexPreference::StrToPreferenceType(
        const string& typeStr) const
{
    if (typeStr == PERF_PREFERENCE_TYPE)
    {
        return ipt_perf;
    }
    if (typeStr == STORE_PREFERENCE_TYPE)
    {
        return ipt_store;
    }
    return ipt_unknown;
}

string KVIndexPreference::PreferenceTypeToStr(IndexPreferenceType type) const
{
    switch (type)
    {
    case ipt_perf:
        return PERF_PREFERENCE_TYPE;
    case ipt_store:
        return STORE_PREFERENCE_TYPE;
    default:
        assert(false);
    }
    return string("");
}

IE_NAMESPACE_END(config);

