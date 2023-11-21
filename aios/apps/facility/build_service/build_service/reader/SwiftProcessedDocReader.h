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
#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/document/document.h"
#include "indexlib/document/document_factory_wrapper.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/kv_document/kv_document.h"

namespace build_service { namespace reader {

class SwiftProcessedDocReader : public SwiftRawDocumentReader
{
public:
    SwiftProcessedDocReader(const util::SwiftClientCreatorPtr& swiftClientCreator);
    ~SwiftProcessedDocReader();

private:
    SwiftProcessedDocReader(const SwiftProcessedDocReader&);
    SwiftProcessedDocReader& operator=(const SwiftProcessedDocReader&);

public:
    bool init(const ReaderInitParam& params) override;
    ErrorCode getNextRawDoc(document::RawDocument& rawDoc, Checkpoint* checkpoint, DocInfo& docInfo) override;

private:
    void ExtractDocMeta(document::RawDocument& rawDoc, const indexlib::document::DocumentPtr& doc);

    void ExtractRawDocument(document::RawDocument& rawDoc, const document::RawDocumentPtr& doc);

    void ExtractIndexNormalDocument(document::RawDocument& rawDoc, const indexlib::document::NormalDocumentPtr& doc);
    void ExtractIndexKVDocument(document::RawDocument& rawDoc, const indexlib::document::KVDocumentPtr& kvDoc);

    void ExtractIndexDocument(document::RawDocument& rawDoc, const indexlib::document::IndexDocumentPtr& doc);

    void ExtractAttributeDocument(document::RawDocument& rawDoc, const indexlib::document::AttributeDocumentPtr& doc,
                                  autil::mem_pool::Pool* pool);

    void ExtractSourceDocument(document::RawDocument& rawDoc,
                               const indexlib::document::SerializedSourceDocumentPtr& doc, autil::mem_pool::Pool* pool);

    void ExtractSummaryDocument(document::RawDocument& rawDoc,
                                const indexlib::document::SerializedSummaryDocumentPtr& doc,
                                autil::mem_pool::Pool* pool);

public:
    // for test
    void ExtractFieldInfos(document::RawDocument& rawDoc, const indexlib::document::DocumentPtr& indexDoc);

    void initAttributeExtractResource(const indexlib::config::IndexPartitionSchemaPtr& schema);

private:
    indexlib::document::DocumentFactoryWrapperPtr _docFactoryWrapper;
    indexlib::document::DocumentParserPtr _docParser;
    indexlib::config::IndexPartitionSchemaPtr _schema;
    std::vector<indexlib::common::PackAttributeFormatterPtr> _packFormatters;
    std::vector<indexlib::common::PackAttributeFormatterPtr> _subPackFormatters;
    std::vector<indexlib::common::AttributeConvertorPtr> _attrConvertors;
    std::vector<indexlib::common::AttributeConvertorPtr> _subAttrConvertors;
    bool _printToken;
    bool _printAttribute;
    bool _printSummary;
    bool _printKVValue;

private:
    friend class SwiftProcessedDocReaderTest;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftProcessedDocReader);

}} // namespace build_service::reader
