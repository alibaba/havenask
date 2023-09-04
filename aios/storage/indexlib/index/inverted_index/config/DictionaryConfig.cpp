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
#include "indexlib/index/inverted_index/config/DictionaryConfig.h"

#include "indexlib/config/ConfigDefine.h"

using std::string;

namespace indexlib::config {
AUTIL_LOG_SETUP(indexlib.config, DictionaryConfig);

struct DictionaryConfig::Impl {
    string dictName;
    string content;
    Impl() {}
    Impl(const string& name, const string& content) : dictName(name), content(content) {}
};

DictionaryConfig::DictionaryConfig() : _impl(std::make_unique<Impl>()) {}

DictionaryConfig::DictionaryConfig(const string& dictName, const string& content)
    : _impl(std::make_unique<Impl>(dictName, content))
{
}

DictionaryConfig::~DictionaryConfig() {}

void DictionaryConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize(DICTIONARY_NAME, _impl->dictName, _impl->dictName);
    json.Jsonize(DICTIONARY_CONTENT, _impl->content, _impl->content);
}
Status DictionaryConfig::CheckEqual(const DictionaryConfig& other) const
{
    CHECK_CONFIG_EQUAL(_impl->dictName, other.GetDictName(), "Dictionary name doesn't match");
    CHECK_CONFIG_EQUAL(_impl->content, other.GetContent(), "Dictionary content doesn't match");
    return Status::OK();
}

DictionaryConfig::DictionaryConfig(const DictionaryConfig& other)
    : _impl(std::make_unique<Impl>(other.GetDictName(), other.GetContent()))
{
}

const string& DictionaryConfig::GetDictName() const { return _impl->dictName; }

const string& DictionaryConfig::GetContent() const { return _impl->content; }
} // namespace indexlib::config
