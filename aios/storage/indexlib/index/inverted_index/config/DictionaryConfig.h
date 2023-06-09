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
#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"

namespace indexlib::config {

class DictionaryConfig : public autil::legacy::Jsonizable
{
public:
    DictionaryConfig();
    DictionaryConfig(const std::string& dictName, const std::string& content);
    DictionaryConfig(const DictionaryConfig& other);
    ~DictionaryConfig();

public:
    const std::string& GetDictName() const;

    const std::string& GetContent() const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    Status CheckEqual(const DictionaryConfig& other) const;

private:
    inline static const std::string DICTIONARY_NAME = "dictionary_name";
    inline static const std::string DICTIONARY_CONTENT = "content";

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::config
