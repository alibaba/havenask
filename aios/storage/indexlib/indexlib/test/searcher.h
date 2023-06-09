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
#ifndef __INDEXLIB_SEARCHER_H
#define __INDEXLIB_SEARCHER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/query.h"
#include "indexlib/test/raw_document.h"
#include "indexlib/test/result.h"
#include "indexlib/test/source_query.h"

DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib { namespace test {

class Searcher
{
public:
    Searcher();
    ~Searcher();

public:
    bool Init(const partition::IndexPartitionReaderPtr& reader, config::IndexPartitionSchema* schema);

    ResultPtr Search(QueryPtr query, TableSearchCacheType searchCacheType, std::string& errorInfo);
    ResultPtr Search(QueryPtr query, TableSearchCacheType searchCacheType);

private:
    void FillResult(QueryPtr query, docid_t docId, ResultPtr result, bool isDeleted);
    void FillRawDocBySource(SourceQueryPtr query, docid_t docId, RawDocumentPtr rawDoc);
    void FillPosition(QueryPtr query, RawDocumentPtr rawDoc);

    bool GetSingleAttributeValue(const config::AttributeSchemaPtr& attrSchema, const std::string& attrName,
                                 docid_t docId, std::string& attrValue);

private:
    partition::IndexPartitionReaderPtr mReader;
    config::IndexPartitionSchema* mSchema;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(Searcher);
}} // namespace indexlib::test

#endif //__INDEXLIB_SEARCHER_H
