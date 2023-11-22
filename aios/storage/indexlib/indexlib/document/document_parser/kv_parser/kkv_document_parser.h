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

#include "indexlib/common_define.h"
#include "indexlib/document/document_parser/kv_parser/kv_document_parser.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(document, MultiRegionKKVKeysExtractor);

namespace indexlib { namespace document {

class KkvDocumentParser : public KvDocumentParser
{
public:
    KkvDocumentParser(const config::IndexPartitionSchemaPtr& schema);
    ~KkvDocumentParser();

public:
    bool DoInit() override;
    bool InitKeyExtractor() override;
    bool SetPrimaryKeyField(const IndexlibExtendDocumentPtr& document, const config::IndexSchemaPtr& indexSchema,
                            regionid_t regionId, DocOperateType opType, document::KVIndexDocument* kvIndexDoc) override;

private:
    document::MultiRegionKKVKeysExtractorPtr mKkvKeyExtractor;
    bool mDenyEmptySuffixKey;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KkvDocumentParser);
}} // namespace indexlib::document
