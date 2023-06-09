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
#ifndef __INDEXLIB_ANALYZER_CONFIG_H
#define __INDEXLIB_ANALYZER_CONFIG_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, AnalyzerConfigImpl);

namespace indexlib { namespace config {

class AnalyzerConfig : public autil::legacy::Jsonizable
{
public:
    AnalyzerConfig(const std::string& name);
    ~AnalyzerConfig();

public:
    const std::string& GetAnalyzerName() const;

    std::string GetTokenizerConfig(const std::string& key) const;
    void SetTokenizerConfig(const std::string& key, const std::string& value);

    std::string GetNormalizeOption(const std::string& key) const;
    void SetNormalizeOption(const std::string& key, const std::string& value);

    void AddStopWord(const std::string& word);
    const std::vector<std::string>& GetStopWords() const;

    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    virtual void AssertEqual(const AnalyzerConfig& other) const;

protected:
    AnalyzerConfigImplPtr mImpl;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AnalyzerConfig);
}} // namespace indexlib::config

#endif //__INDEXLIB_ANALYZER_CONFIG_H
