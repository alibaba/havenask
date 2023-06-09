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
#ifndef __INDEXLIB_DELETE_TO_ADD_DOCUMENT_REWRITER_H
#define __INDEXLIB_DELETE_TO_ADD_DOCUMENT_REWRITER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/document/document_rewriter/document_rewriter.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class DeleteToAddDocumentRewriter : public document::DocumentRewriter
{
public:
    DeleteToAddDocumentRewriter() {}
    ~DeleteToAddDocumentRewriter() {}

public:
    void Rewrite(const document::DocumentPtr& doc) override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DeleteToAddDocumentRewriter);
}} // namespace indexlib::document

#endif //__INDEXLIB_KKV_DOCUMENT_REWRITER_H
