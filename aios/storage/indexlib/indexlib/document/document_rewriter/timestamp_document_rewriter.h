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
#ifndef __INDEXLIB_TIMESTAMP_DOCUMENT_REWRITER_H
#define __INDEXLIB_TIMESTAMP_DOCUMENT_REWRITER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/document_rewriter/document_rewriter.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class TimestampDocumentRewriter : public document::DocumentRewriter
{
public:
    TimestampDocumentRewriter(fieldid_t timestampFieldId);
    ~TimestampDocumentRewriter();

public:
    void Rewrite(const document::DocumentPtr& doc) override;

private:
    fieldid_t mTimestampFieldId;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TimestampDocumentRewriter);
}} // namespace indexlib::document

#endif //__INDEXLIB_TIMESTAMP_DOCUMENT_REWRITER_H
