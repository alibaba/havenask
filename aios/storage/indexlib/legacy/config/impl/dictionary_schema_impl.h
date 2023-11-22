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
#include "indexlib/config/dictionary_schema.h"
#include "indexlib/index/inverted_index/config/DictionaryConfig.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace config {

class DictionarySchemaImpl : public autil::legacy::Jsonizable
{
public:
    DictionarySchemaImpl();
    ~DictionarySchemaImpl();

public:
    void AddDictionaryConfig(const std::shared_ptr<DictionaryConfig>& dictConfig);
    std::shared_ptr<DictionaryConfig> GetDictionaryConfig(const std::string& dictName) const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const DictionarySchemaImpl& other) const;
    void AssertCompatible(const DictionarySchemaImpl& other) const;

    inline DictionarySchema::Iterator Begin() const { return mDictConfigs.begin(); }
    inline DictionarySchema::Iterator End() const { return mDictConfigs.end(); }

private:
    std::map<std::string, std::shared_ptr<DictionaryConfig>> mDictConfigs;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<DictionarySchemaImpl> DictionarySchemaImplPtr;
}} // namespace indexlib::config
