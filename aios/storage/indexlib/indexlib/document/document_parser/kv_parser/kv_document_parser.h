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
#ifndef __INDEXLIB_KV_DOCUMENT_PARSER_H
#define __INDEXLIB_KV_DOCUMENT_PARSER_H

#include <memory>

#include "autil/ConstString.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/document/document_parser.h"
#include "indexlib/document/source_timestamp_parser.h"
#include "indexlib/document/kv_document/kv_document.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(document, MultiRegionExtendDocFieldsConvertor);
DECLARE_REFERENCE_CLASS(document, MultiRegionPackAttributeAppender);
DECLARE_REFERENCE_CLASS(document, MultiRegionKVKeyExtractor);
DECLARE_REFERENCE_CLASS(util, AccumulativeCounter);
DECLARE_REFERENCE_CLASS(config, IndexSchema);

namespace indexlib { namespace document {

class KvDocumentParser : public DocumentParser
{
public:
    KvDocumentParser(const config::IndexPartitionSchemaPtr& schema);
    ~KvDocumentParser();

public:
    bool DoInit() override;
    DocumentPtr Parse(const IndexlibExtendDocumentPtr& extendDoc) override;
    DocumentPtr Parse(const autil::StringView& serializedData) override;

public:
    bool Parse(const IndexlibExtendDocumentPtr& extendDoc, document::KVDocument* kvDoc);

protected:
    void InitStorePkey();
    virtual bool InitKeyExtractor();
    virtual bool SetPrimaryKeyField(const IndexlibExtendDocumentPtr& document,
                                    const config::IndexSchemaPtr& indexSchema, regionid_t regionId,
                                    DocOperateType opType, document::KVIndexDocument* kvIndexDoc);

private:
    bool CreateDocument(const IndexlibExtendDocumentPtr& document, document::KVDocument* kvDoc);
    bool InitFromDocumentParam();
    fieldid_t AttrValueFieldId() const;

protected:
    MultiRegionExtendDocFieldsConvertorPtr mFieldConvertPtr;
    MultiRegionPackAttributeAppenderPtr mPackAttrAppender;
    bool mDenyEmptyPrimaryKey;

private:
    document::MultiRegionKVKeyExtractorPtr mKvKeyExtractor;
    fieldid_t mValueAttrFieldId;
    util::AccumulativeCounterPtr mAttributeConvertErrorCounter;
    bool mNeedStorePKeyValue = false;
    std::unique_ptr<SourceTimestampParser> mSourceTimestampParser = nullptr;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KvDocumentParser);
}} // namespace indexlib::document

#endif //__INDEXLIB_KV_DOCUMENT_PARSER_H
