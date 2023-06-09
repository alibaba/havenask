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
#ifndef __INDEXLIB_SOURCE_READER_H
#define __INDEXLIB_SOURCE_READER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/source_schema.h"
#include "indexlib/document/index_document/normal_document/source_document.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/indexlib.h"
namespace indexlib { namespace index {

class SourceReader
{
public:
    SourceReader(const config::SourceSchemaPtr& sourceSchema) : mSourceSchema(sourceSchema) {}
    virtual ~SourceReader() {}

public:
    virtual bool Open(const index_base::PartitionDataPtr& partitionData, const SourceReader* hintReader = nullptr) = 0;
    virtual bool GetDocument(docid_t docId, document::SourceDocument* sourceDocument) const = 0;

protected:
    config::SourceSchemaPtr mSourceSchema;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SourceReader);
}} // namespace indexlib::index

#endif //__INDEXLIB_SOURCE_READER_H
