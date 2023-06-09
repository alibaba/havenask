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
#include "indexlib/base/Constant.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/document/IDocumentParser.h"

namespace indexlibv2::config {
class KKVIndexConfig;
}

namespace indexlibv2::document {
class KVIndexDocument;

class KKVKeysExtractor
{
public:
    KKVKeysExtractor(const std::shared_ptr<config::KKVIndexConfig>& indexConfig);
    ~KKVKeysExtractor();

public:
    void HashPrefixKey(const std::string& pkey, uint64_t& pkeyHash);
    void HashSuffixKey(const std::string& suffixKey, uint64_t& skeyHash);

private:
    FieldType _prefixKeyFieldType;
    bool _prefixKeyUseNumberHash;
    FieldType _suffixKeyFieldType;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::document
