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
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(common, AttributeConvertor);

namespace indexlib { namespace index {

class TTLDecoder
{
public:
    TTLDecoder(const config::IndexPartitionSchemaPtr& schema);
    ~TTLDecoder();

public:
    void SetDocumentTTL(const document::DocumentPtr& document) const;

private:
    fieldid_t mTTLFieldId;
    common::AttributeConvertorPtr mConverter;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TTLDecoder);
}} // namespace indexlib::index
