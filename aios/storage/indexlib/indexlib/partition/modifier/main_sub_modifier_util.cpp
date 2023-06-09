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
#include "indexlib/partition/modifier/main_sub_modifier_util.h"

#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_reader.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"

namespace indexlib { namespace partition {

void MainSubModifierUtil::GetSubDocIdRange(const index::JoinDocidAttributeReader* reader, docid_t mainDocId,
                                           docid_t* subDocIdBegin, docid_t* subDocIdEnd)
{
    assert(subDocIdBegin);
    assert(subDocIdEnd);

    *subDocIdBegin = 0;
    if (mainDocId > 0) {
        *subDocIdBegin = reader->GetJoinDocId(mainDocId - 1);
    }
    *subDocIdEnd = reader->GetJoinDocId(mainDocId);

    if (*subDocIdEnd < 0 || *subDocIdBegin < 0 || (*subDocIdBegin > *subDocIdEnd)) {
        INDEXLIB_THROW(util::ExceptionSelector<util::IndexCollapsed>::ExceptionType,
                       "failed to get join docid [%d, %d), mainDocId [%d]", *subDocIdBegin, *subDocIdEnd, mainDocId);
    }
}

}} // namespace indexlib::partition
