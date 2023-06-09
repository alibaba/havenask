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

#include "indexlib/document/kv/KVIndexDocumentParserBase.h"

namespace indexlibv2::config {
class FieldConfig;
class KVIndexConfig;
} // namespace indexlibv2::config

namespace indexlibv2::document {
class KVKeyExtractor;

class KVIndexDocumentParser final : public KVIndexDocumentParserBase
{
public:
    explicit KVIndexDocumentParser(const std::shared_ptr<indexlib::util::AccumulativeCounter>& counter = {});
    ~KVIndexDocumentParser();

public:
    Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig) override;

private:
    bool MaybeIgnore(const RawDocument& rawDoc) const override;
    const std::shared_ptr<config::TTLSettings>& GetTTLSettings() const override;
    bool ParseKey(const RawDocument& rawDoc, KVDocument* doc) const override;

private:
    std::shared_ptr<config::KVIndexConfig> _indexConfig;
    std::shared_ptr<config::FieldConfig> _keyField;
    std::unique_ptr<KVKeyExtractor> _keyExtractor;
};

} // namespace indexlibv2::document
