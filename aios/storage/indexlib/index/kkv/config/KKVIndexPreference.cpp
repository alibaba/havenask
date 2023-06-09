/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/index/kkv/config/KKVIndexPreference.h"

#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil::legacy::json;

namespace indexlib::config {
AUTIL_LOG_SETUP(indexlib.config, KKVIndexPreference);

namespace {
struct KKVPreferenceParameter : public autil::legacy::Jsonizable {
public:
    void InitDefaultParam(IndexPreferenceType type)
    {
        switch (type) {
        case ipt_perf: {
            hashParam = KKVIndexPreference::HashDictParam("dense", 50, true, false, false);
            skeyParam = KKVIndexPreference::SuffixKeyParam(false, "");
            valueParam = KKVIndexPreference::ValueParam(false, "");
            break;
        }
        case ipt_store: {
            hashParam = KKVIndexPreference::HashDictParam("separate_chain", 50, true, false, false);
            skeyParam = KKVIndexPreference::SuffixKeyParam(false, "zstd");
            valueParam = KKVIndexPreference::ValueParam(false, "zstd");
            break;
        }
        default:
            assert(type == ipt_unknown);
            assert(false);
        }
    }

    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize(KKVIndexPreference::KKV_HASH_PARAM_NAME, hashParam, hashParam);
        json.Jsonize(KKVIndexPreference::KKV_SKEY_PARAM_NAME, skeyParam, skeyParam);
        json.Jsonize(KKVIndexPreference::KKV_VALUE_PARAM_NAME, valueParam, valueParam);
    }

    KKVIndexPreference::HashDictParam hashParam;
    KKVIndexPreference::SuffixKeyParam skeyParam;
    KKVIndexPreference::ValueParam valueParam;
};
}; // namespace

KKVIndexPreference::KKVIndexPreference() : _valueInline(false) { InitDefaultParams(_type); }

KKVIndexPreference::~KKVIndexPreference() {}

void KKVIndexPreference::Jsonize(Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        assert(_type != ipt_unknown);
        string typeStr = PreferenceTypeToStr(_type);
        json.Jsonize(PREFERENCE_TYPE_NAME, typeStr);
        KKVPreferenceParameter params;
        params.hashParam = _hashParam;
        params.skeyParam = _skeyParam;
        params.valueParam = _valueParam;
        json.Jsonize(PREFERENCE_PARAMS_NAME, params);
        json.Jsonize(KKV_VALUE_INLINE, _valueInline);
    } else {
        string typeStr = PERF_PREFERENCE_TYPE;
        json.Jsonize(PREFERENCE_TYPE_NAME, typeStr, typeStr);
        IndexPreferenceType type = StrToPreferenceType(typeStr);
        if (type == ipt_unknown) {
            INDEXLIB_FATAL_ERROR(BadParameter, "unknown paramter str[%s] for index_preference type", typeStr.c_str());
        }
        _type = type;
        KKVPreferenceParameter params;
        params.InitDefaultParam(_type);
        json.Jsonize(PREFERENCE_PARAMS_NAME, params, params);
        _hashParam = params.hashParam;
        _skeyParam = params.skeyParam;
        _valueParam = params.valueParam;
        json.Jsonize(KKV_VALUE_INLINE, _valueInline, _valueInline);
        Check();
    }
}

void KKVIndexPreference::Check() const
{
    _hashParam.Check();
    _skeyParam.Check();
    _valueParam.Check();
}

void KKVIndexPreference::InitDefaultParams(IndexPreferenceType type)
{
    KKVPreferenceParameter defaultParam;
    defaultParam.InitDefaultParam(type);
    _hashParam = defaultParam.hashParam;
    _valueParam = defaultParam.valueParam;
    _skeyParam = defaultParam.skeyParam;
}
} // namespace indexlib::config
