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
#ifndef __INDEXLIB_SUB_DOC_BUILT_SEGMENTS_DOCUMENT_DEDUPER_H
#define __INDEXLIB_SUB_DOC_BUILT_SEGMENTS_DOCUMENT_DEDUPER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/document_deduper/built_segments_document_deduper.h"
#include "indexlib/partition/document_deduper/document_deduper.h"
#include "indexlib/partition/modifier/sub_doc_modifier.h"

namespace indexlib { namespace partition {

class SubDocBuiltSegmentsDocumentDeduper : public DocumentDeduper
{
public:
    SubDocBuiltSegmentsDocumentDeduper(const config::IndexPartitionSchemaPtr& schema);

    ~SubDocBuiltSegmentsDocumentDeduper() {}

public:
    void Init(const partition::PartitionModifierPtr& modifier);

    void Dedup() override;

private:
    BuiltSegmentsDocumentDeduperPtr mMainSegmentsDeduper;
    BuiltSegmentsDocumentDeduperPtr mSubSegmentsDeduper;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SubDocBuiltSegmentsDocumentDeduper);
}} // namespace indexlib::partition

#endif //__INDEXLIB_SUB_DOC_BUILT_SEGMENTS_DOCUMENT_DEDUPER_H
