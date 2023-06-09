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
#pragma once
#include "indexlib/index/kv/config/KVIndexPreference.h"

namespace indexlib::config {

class KKVIndexPreference : public KVIndexPreference
{
public:
    using HashDictParam = KVIndexPreference::HashDictParam;
    using ValueParam = KVIndexPreference::ValueParam;
    using SuffixKeyParam = ValueParam;

public:
    KKVIndexPreference();
    ~KKVIndexPreference();

public:
    const SuffixKeyParam& GetSkeyParam() const { return _skeyParam; }
    void SetSkeyParam(const SuffixKeyParam& param) { _skeyParam = param; }
    bool IsValueInline() const { return _valueInline; }

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const override;

public:
    void TEST_SetValueInline(bool v) { _valueInline = v; }

private:
    void InitDefaultParams(IndexPreferenceType type);

private:
    SuffixKeyParam _skeyParam;
    bool _valueInline;

public:
    static constexpr const char KKV_HASH_PARAM_NAME[] = "hash_dict";
    static constexpr const char KKV_SKEY_PARAM_NAME[] = "suffix_key";
    static constexpr const char KKV_VALUE_PARAM_NAME[] = "value";
    static constexpr const char KKV_VALUE_INLINE[] = "value_inline";

private:
    AUTIL_LOG_DECLARE();
};
} // namespace indexlib::config
