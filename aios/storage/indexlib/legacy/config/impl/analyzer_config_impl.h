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

#include <memory>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/legacy/indexlib.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace config {

class AnalyzerConfigImpl : public autil::legacy::Jsonizable
{
public:
    AnalyzerConfigImpl(const std::string& name);
    ~AnalyzerConfigImpl();

public:
    const std::string& GetAnalyzerName() const { return mAnalyzerName; }

    std::string GetTokenizerConfig(const std::string& key) const;
    void SetTokenizerConfig(const std::string& key, const std::string& value);

    std::string GetNormalizeOption(const std::string& key) const;
    void SetNormalizeOption(const std::string& key, const std::string& value);

    void AddStopWord(const std::string& word);
    const std::vector<std::string>& GetStopWords() const { return mStopWords; }

    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    virtual void AssertEqual(const AnalyzerConfigImpl& other) const;

protected:
    std::string mAnalyzerName;
    std::map<std::string, std::string> mTokenizerConfigs;
    std::vector<std::string> mStopWords;
    std::map<std::string, std::string> mNormalizeOptions;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AnalyzerConfigImpl> AnalyzerConfigImplPtr;
}} // namespace indexlib::config
