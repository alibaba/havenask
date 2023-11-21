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

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/config/source_schema.h"
#include "indexlib/document/index_document/normal_document/source_document.h"
#include "indexlib/index/data_structure/var_len_data_reader.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class SourceSegmentReader
{
public:
    SourceSegmentReader(const config::SourceSchemaPtr& sourceSchema);
    ~SourceSegmentReader();

public:
    bool Open(const index_base::SegmentData& segData, const index_base::SegmentInfo& segmentInfo);
    bool GetDocument(docid_t localDocId, document::SourceDocument* sourceDocument) const;

private:
    config::SourceSchemaPtr mSourceSchema;
    std::vector<VarLenDataReaderPtr> mGroupReaders;
    VarLenDataReaderPtr mMetaReader;
    static constexpr size_t DEFAULT_POOL_SIZE = 512 * 1024;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SourceSegmentReader);
}} // namespace indexlib::index
