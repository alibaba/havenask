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
#include "indexlib/index/kv/config/KVIndexPreference.h"

#include "indexlib/util/KeyValueMap.h"
#include "indexlib/util/buffer_compressor/BufferCompressorCreator.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace autil::legacy::json;

namespace indexlib::config {
AUTIL_LOG_SETUP(indexlib.config, KVIndexPreference);

namespace {
struct KVPreferenceParameter : public autil::legacy::Jsonizable {
public:
    void InitDefaultParam(IndexPreferenceType type)
    {
        switch (type) {
        case ipt_perf: {
            hashParam = KVIndexPreference::HashDictParam("dense");
            valueParam = KVIndexPreference::ValueParam(false, "");
            break;
        }
        case ipt_store: {
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

private:
    inline static const std::string KV_HASH_PARAM_NAME = "hash_dict";
    inline static const std::string KV_VALUE_PARAM_NAME = "value";
};
}; // namespace

void KVIndexPreference::ValueParam::Jsonize(JsonWrapper& json)
{
    json.Jsonize("encode", _encode, _encode);
    json.Jsonize("value_impact", _valueImpact, _valueImpact);
    json.Jsonize("plain_format", _plainFormat, _plainFormat);
    json.Jsonize("file_compressor", _fileCompType, _fileCompType);
    json.Jsonize("file_compressor_buffer_size", _fileCompBuffSize, _fileCompBuffSize);
    json.Jsonize("file_compressor_parameter", _fileCompParams, _fileCompParams);
    json.Jsonize("fix_len_auto_inline", _fixLenAutoInline, _fixLenAutoInline);
}

void KVIndexPreference::ValueParam::Check() const
{
    if (_valueImpact && _plainFormat) {
        INDEXLIB_FATAL_ERROR(BadParameter, "both value_impact and plain_format are true, only support enable any one.");
    }

    if (_fileCompType.empty()) {
        return;
    }

    if (!util::BufferCompressorCreator::IsValidCompressorName(_fileCompType)) {
        INDEXLIB_FATAL_ERROR(BadParameter, "unsupported file_compressor [%s]!", _fileCompType.c_str());
    }

    size_t alignedSize = 1;
    while (alignedSize < _fileCompBuffSize) {
        alignedSize <<= 1;
    }
    if (alignedSize != _fileCompBuffSize) {
        INDEXLIB_FATAL_ERROR(BadParameter, "_fileCompBuffSize [%lu] must be 2^n!", _fileCompBuffSize);
    }
}

KVIndexPreference::KVIndexPreference() : _type(ipt_perf) { InitDefaultParams(_type); }

KVIndexPreference::~KVIndexPreference() {}

void KVIndexPreference::Jsonize(Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        assert(_type != ipt_unknown);
        string typeStr = PreferenceTypeToStr(_type);
        json.Jsonize(PREFERENCE_TYPE_NAME, typeStr);
        KVPreferenceParameter params;
        params.hashParam = _hashParam;
        params.valueParam = _valueParam;
        json.Jsonize(PREFERENCE_PARAMS_NAME, params);
    } else {
        string typeStr = PERF_PREFERENCE_TYPE;
        json.Jsonize(PREFERENCE_TYPE_NAME, typeStr, typeStr);
        IndexPreferenceType type = StrToPreferenceType(typeStr);
        if (type == ipt_unknown) {
            INDEXLIB_FATAL_ERROR(BadParameter, "unknown paramter str[%s] for index_preference type", typeStr.c_str());
        }
        _type = type;
        KVPreferenceParameter params;
        params.InitDefaultParam(_type);
        json.Jsonize(PREFERENCE_PARAMS_NAME, params, params);
        _hashParam = params.hashParam;
        _valueParam = params.valueParam;
        Check();
    }
}

void KVIndexPreference::Check() const
{
    _hashParam.Check();
    _valueParam.Check();
}

void KVIndexPreference::InitDefaultParams(IndexPreferenceType type)
{
    KVPreferenceParameter defaultParam;
    defaultParam.InitDefaultParam(type);
    _hashParam = defaultParam.hashParam;
    _valueParam = defaultParam.valueParam;
}

IndexPreferenceType KVIndexPreference::StrToPreferenceType(const string& typeStr) const
{
    if (typeStr == PERF_PREFERENCE_TYPE) {
        return ipt_perf;
    }
    if (typeStr == STORE_PREFERENCE_TYPE) {
        return ipt_store;
    }
    return ipt_unknown;
}

string KVIndexPreference::PreferenceTypeToStr(IndexPreferenceType type) const
{
    switch (type) {
    case ipt_perf:
        return PERF_PREFERENCE_TYPE;
    case ipt_store:
        return STORE_PREFERENCE_TYPE;
    default:
        assert(false);
    }
    return string("");
}
} // namespace indexlib::config
