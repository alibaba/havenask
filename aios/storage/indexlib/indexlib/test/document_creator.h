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
#ifndef __INDEXLIB_DOCUMENT_CREATOR_H
#define __INDEXLIB_DOCUMENT_CREATOR_H

#include <memory>

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/kv_document/kv_document.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/raw_document.h"

namespace indexlib { namespace test {

class DocumentCreator
{
public:
    DocumentCreator();
    ~DocumentCreator();

public:
    static document::NormalDocumentPtr CreateNormalDocument(const config::IndexPartitionSchemaPtr& schema,
                                                            const std::string& docString,
                                                            bool needRewriteSectionAttribute = true,
                                                            autil::mem_pool::Pool* pool = NULL);

    static std::vector<document::NormalDocumentPtr> CreateNormalDocuments(const config::IndexPartitionSchemaPtr& schema,
                                                                          const std::string& docString,
                                                                          bool needRewriteSectionAttribute = true);

    static document::KVDocumentPtr CreateKVDocument(const config::IndexPartitionSchemaPtr& schema,
                                                    const std::string& docString, const std::string& defaultRegionName,
                                                    bool useMultiIndex = false);

    static std::vector<document::KVDocumentPtr> CreateKVDocuments(const config::IndexPartitionSchemaPtr& schema,
                                                                  const std::string& docString,
                                                                  const std::string& defaultRegionName = "",
                                                                  bool useMultiIndex = false);

    static std::string makeFbKvMessage(const std::map<std::string, std::string>& fields, const std::string& region = "",
                                       DocOperateType opType = ADD_DOC);
    static std::string GetDocumentTypeStr(DocOperateType opType);
    static void SetPrimaryKey(config::IndexPartitionSchemaPtr schema, RawDocumentPtr rawDoc, document::DocumentPtr doc);

private:
    static void SetKVPrimaryKey(config::IndexPartitionSchemaPtr schema, RawDocumentPtr rawDoc,
                                document::KVIndexDocument* doc);
    static void SetDocumentValue(config::IndexPartitionSchemaPtr schema, RawDocumentPtr rawDoc,
                                 document::DocumentPtr doc, autil::mem_pool::Pool* pool = NULL);
    static void SetKVDocumentValue(config::IndexPartitionSchemaPtr schema, RawDocumentPtr rawDoc,
                                   document::KVIndexDocument* doc, bool useMultiIndex = false);

    static document::NormalDocumentPtr CreateNormalDocument(const config::IndexPartitionSchemaPtr& schema,
                                                            RawDocumentPtr rawDoc, autil::mem_pool::Pool* pool);

    static document::KVDocumentPtr CreateKVDocument(const config::IndexPartitionSchemaPtr& schema,
                                                    RawDocumentPtr rawDoc, const std::string& defaultRegionName,
                                                    bool useMultiIndex);

    static regionid_t ExtractRegionIdFromRawDocument(const config::IndexPartitionSchemaPtr& schema,
                                                     const RawDocumentPtr& rawDocument,
                                                     const std::string& defaultRegionName);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentCreator);
}} // namespace indexlib::test

#endif //__INDEXLIB_DOCUMENT_CREATOR_H
