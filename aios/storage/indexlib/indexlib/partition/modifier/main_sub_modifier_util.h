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

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, JoinDocidAttributeReader);
DECLARE_REFERENCE_CLASS(index, PrimaryKeyIndexReader);
DECLARE_REFERENCE_CLASS(document, NormalDocument);

namespace indexlib { namespace partition {

class MainSubModifierUtil
{
public:
    static void GetSubDocIdRange(const index::JoinDocidAttributeReader* reader, docid_t mainDocId,
                                 docid_t* subDocIdBegin, docid_t* subDocIdEnd);

private:
    MainSubModifierUtil() {}
    ~MainSubModifierUtil() {}
};

}} // namespace indexlib::partition
