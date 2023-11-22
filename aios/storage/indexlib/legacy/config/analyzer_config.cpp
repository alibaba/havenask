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
#include "indexlib/config/analyzer_config.h"

#include "indexlib/config/impl/analyzer_config_impl.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, AnalyzerConfig);

AnalyzerConfig::AnalyzerConfig(const string& name) : mImpl(new AnalyzerConfigImpl(name)) {}

AnalyzerConfig::~AnalyzerConfig() {}

string AnalyzerConfig::GetTokenizerConfig(const string& key) const { return mImpl->GetTokenizerConfig(key); }

void AnalyzerConfig::SetNormalizeOption(const string& key, const string& value)
{
    mImpl->SetNormalizeOption(key, value);
}

string AnalyzerConfig::GetNormalizeOption(const string& key) const { return mImpl->GetNormalizeOption(key); }

void AnalyzerConfig::SetTokenizerConfig(const string& key, const string& value)
{
    mImpl->SetTokenizerConfig(key, value);
}

void AnalyzerConfig::AddStopWord(const std::string& word) { mImpl->AddStopWord(word); }

void AnalyzerConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) { mImpl->Jsonize(json); }

void AnalyzerConfig::AssertEqual(const AnalyzerConfig& other) const { mImpl->AssertEqual(*(other.mImpl.get())); }

const string& AnalyzerConfig::GetAnalyzerName() const { return mImpl->GetAnalyzerName(); }

const vector<string>& AnalyzerConfig::GetStopWords() const { return mImpl->GetStopWords(); }
}} // namespace indexlib::config
