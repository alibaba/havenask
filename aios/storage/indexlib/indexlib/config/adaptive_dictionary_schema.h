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
#ifndef __INDEXLIB_ADAPTIVE_DICTIONARY_SCHEMA_H
#define __INDEXLIB_ADAPTIVE_DICTIONARY_SCHEMA_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/config/AdaptiveDictionaryConfig.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {
class AdaptiveDictionarySchemaImpl;
DEFINE_SHARED_PTR(AdaptiveDictionarySchemaImpl);

class AdaptiveDictionarySchema : public autil::legacy::Jsonizable
{
public:
    typedef std::map<std::string, size_t>::const_iterator Iterator;

public:
    AdaptiveDictionarySchema();
    ~AdaptiveDictionarySchema() {}

public:
    std::shared_ptr<AdaptiveDictionaryConfig> GetAdaptiveDictionaryConfig(const std::string& ruleName) const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const AdaptiveDictionarySchema& other) const;
    void AssertCompatible(const AdaptiveDictionarySchema& other) const;
    // for test
    void AddAdaptiveDictionaryConfig(const std::shared_ptr<AdaptiveDictionaryConfig>& config);

private:
    AdaptiveDictionarySchemaImplPtr mImpl;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AdaptiveDictionarySchema);
}} // namespace indexlib::config

#endif //__INDEXLIB_ADAPTIVE_DICTIONARY_SCHEMA_H
