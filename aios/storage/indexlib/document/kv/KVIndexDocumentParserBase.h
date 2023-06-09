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

#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/document/kv/KVDocument.h"
#include "indexlib/document/kv/ValueConvertor.h"
#include "indexlib/index/kv/config/TTLSettings.h"
#include "indexlib/index/kv/config/ValueConfig.h"

namespace autil::mem_pool {
class Pool;
}

namespace indexlib::util {
class AccumulativeCounter;
}

namespace indexlibv2::document {
class RawDocument;
class KVDocument;

class KVIndexDocumentParserBase
{
public:
    KVIndexDocumentParserBase(const std::shared_ptr<indexlib::util::AccumulativeCounter>& counter = {});
    virtual ~KVIndexDocumentParserBase();

public:
    virtual Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig) = 0;

public:
    StatusOr<std::unique_ptr<KVDocument>> Parse(const RawDocument& rawDoc, autil::mem_pool::Pool* pool) const;

protected:
    Status InitValueConvertor(const std::shared_ptr<config::ValueConfig>& valueConfig, bool forcePackValue,
                              bool parseDelete = false);
    StatusOr<std::unique_ptr<ValueConvertor>>
    CreateValueConvertor(const std::shared_ptr<config::ValueConfig>& valueConfig, bool forcePackValue,
                         bool parseDelete = false) const;
    virtual ValueConvertor::ConvertResult ParseValue(const RawDocument& rawDoc, autil::mem_pool::Pool* pool) const;

private:
    Status Parse(const RawDocument& rawDoc, KVDocument* doc, autil::mem_pool::Pool* pool) const;
    virtual bool MaybeIgnore(const RawDocument& rawDoc) const = 0;
    virtual const std::shared_ptr<config::TTLSettings>& GetTTLSettings() const = 0;
    virtual bool ParseKey(const RawDocument& rawDoc, KVDocument* doc) const = 0;
    virtual bool NeedParseValue(DocOperateType opType) const;
    void ParseTTL(const RawDocument& rawDoc, KVDocument* doc) const;
    void SetDocInfo(const RawDocument& rawDoc, KVDocument* doc) const;

private:
    std::shared_ptr<indexlib::util::AccumulativeCounter> _attributeConvertErrorCounter;
    std::unique_ptr<ValueConvertor> _valueConvertor;
};

} // namespace indexlibv2::document
