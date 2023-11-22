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

#include "build_service/common_define.h"
#include "build_service/reader/SwiftRawDocumentReader.h"
#include "build_service/util/Log.h"

namespace indexlibv2::index {
class PackAttributeFormatter;
class AttributeConvertor;
} // namespace indexlibv2::index

namespace indexlib::document {
class AttributeDocument;
class IndexDocument;
class SerializedSummaryDocument;
class SerializedSourceDocument;
} // namespace indexlib::document

namespace indexlibv2::document {
class IDocumentParser;
class RawDocument;
class NormalDocument;
class KVDocument;
} // namespace indexlibv2::document

namespace build_service { namespace reader {

class SwiftProcessedDocReaderV2 : public SwiftRawDocumentReader
{
public:
    SwiftProcessedDocReaderV2(const util::SwiftClientCreatorPtr& swiftClientCreator);
    ~SwiftProcessedDocReaderV2();

private:
    SwiftProcessedDocReaderV2(const SwiftProcessedDocReaderV2&);
    SwiftProcessedDocReaderV2& operator=(const SwiftProcessedDocReaderV2&);

public:
    bool init(const ReaderInitParam& params) override;
    ErrorCode getNextRawDoc(document::RawDocument& rawDoc, Checkpoint* checkpoint, DocInfo& docInfo) override;

private:
    void ExtractDocMeta(document::RawDocument& rawDoc, const std::shared_ptr<indexlibv2::document::IDocument>& doc);

    void ExtractRawDocument(document::RawDocument& rawDoc,
                            const std::shared_ptr<indexlibv2::document::RawDocument>& doc);

    void ExtractIndexNormalDocument(document::RawDocument& rawDoc,
                                    const std::shared_ptr<indexlibv2::document::NormalDocument>& doc);
    void ExtractIndexKVDocument(document::RawDocument& rawDoc,
                                const std::shared_ptr<indexlibv2::document::KVDocument>& kvDoc);

    void ExtractIndexDocument(document::RawDocument& rawDoc,
                              const std::shared_ptr<indexlib::document::IndexDocument>& doc);

    void ExtractAttributeDocument(document::RawDocument& rawDoc,
                                  const std::shared_ptr<indexlib::document::AttributeDocument>& doc,
                                  autil::mem_pool::Pool* pool);

    void ExtractSourceDocument(document::RawDocument& rawDoc,
                               const std::shared_ptr<indexlib::document::SerializedSourceDocument>& doc,
                               autil::mem_pool::Pool* pool);

    void ExtractSummaryDocument(document::RawDocument& rawDoc,
                                const std::shared_ptr<indexlib::document::SerializedSummaryDocument>& doc,
                                autil::mem_pool::Pool* pool);

public:
    // for test
    void ExtractFieldInfos(document::RawDocument& rawDoc,
                           const std::shared_ptr<indexlibv2::document::IDocument>& indexDoc);

    bool initAttributeExtractResource(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema);

private:
    std::unique_ptr<indexlibv2::document::IDocumentFactory>
    createDocumentFactory(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema) const;
    void ExtractTagInfoMap(document::RawDocument& rawDoc, const std::map<std::string, std::string>& tagInfoMap);

private:
    std::unique_ptr<indexlibv2::document::IDocumentParser> _docParser;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _schema;
    std::vector<std::shared_ptr<indexlibv2::index::PackAttributeFormatter>> _packFormatters;
    std::vector<std::shared_ptr<indexlibv2::index::AttributeConvertor>> _attrConvertors;
    bool _printToken;
    bool _printAttribute;
    bool _printSummary;
    bool _printKVValue;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftProcessedDocReaderV2);

}} // namespace build_service::reader
