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
#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Constant.h"
#include "indexlib/document/IDocumentParser.h"

namespace indexlib::util {
class AccumulativeCounter;
}

namespace indexlibv2::config {
class IIndexConfig;
}

namespace indexlibv2::document {

class KVDocument;
class KVDocumentBatch;
class KVIndexDocumentParserBase;

class KVDocumentParser : public IDocumentParser
{
public:
    KVDocumentParser();
    ~KVDocumentParser();

public:
    Status Init(const std::shared_ptr<config::ITabletSchema>& schema,
                const std::shared_ptr<DocumentInitParam>& initParam) override;

    std::unique_ptr<IDocumentBatch> Parse(const std::string& docString, const std::string& docFormat) const override;

    std::pair<Status, std::unique_ptr<IDocumentBatch>> Parse(ExtendDocument& extendDoc) const override;

    std::pair<Status, std::unique_ptr<IDocumentBatch>> Parse(const autil::StringView& serializedData) const override;

    std::pair<Status, std::unique_ptr<IDocumentBatch>> BatchParse(IMessageIterator* inputIterator) const override;

    std::pair<Status, std::unique_ptr<IDocumentBatch>>
    ParseRawDoc(const std::shared_ptr<RawDocument>& rawDoc) const override;

private:
    virtual std::unique_ptr<KVIndexDocumentParserBase>
    CreateIndexFieldsParser(const std::shared_ptr<config::IIndexConfig>& indexConfig) const;

    virtual std::unique_ptr<KVDocumentBatch> CreateDocumentBatch() const;

private:
    std::shared_ptr<indexlib::util::AccumulativeCounter>
    InitCounter(const std::shared_ptr<DocumentInitParam>& initParam) const;

protected:
    schemaid_t _schemaId = DEFAULT_SCHEMAID;
    std::shared_ptr<indexlib::util::AccumulativeCounter> _counter;
    std::vector<std::pair<size_t, std::unique_ptr<KVIndexDocumentParserBase>>> _indexDocParsers;

private:
    inline static const std::string ATTRIBUTE_CONVERT_ERROR_COUNTER_NAME = "bs.processor.attributeConvertError";

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::document
