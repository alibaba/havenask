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
#ifndef ISEARCH_BS_KKVSORTDOCCONVERTOR_H
#define ISEARCH_BS_KKVSORTDOCCONVERTOR_H

#include "autil/ConstString.h"
#include "build_service/builder/SortDocumentConverter.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/document/kv_document/kv_document.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace builder {

class KKVSortDocConvertor : public SortDocumentConverter
{
public:
    KKVSortDocConvertor() {}
    ~KKVSortDocConvertor() {}

private:
    KKVSortDocConvertor(const KKVSortDocConvertor&);
    KKVSortDocConvertor& operator=(const KKVSortDocConvertor&);

public:
    bool convert(const indexlib::document::DocumentPtr& doc, SortDocument& sortDoc) override
    {
        auto kvDoc = DYNAMIC_POINTER_CAST(indexlib::document::KVDocument, doc);
        if (!kvDoc) {
            return false;
        }
        if (kvDoc->GetDocumentCount() != 1u) {
            return false;
        }

        sortDoc._kvDoc.reset(kvDoc->CloneKVDocument());
        return true;
    }

    indexlib::document::DocumentPtr getDocument(const SortDocument& sortDoc) override
    {
        indexlib::document::KVDocumentPtr doc = sortDoc._kvDoc;
        return doc;
    }

    void swap(SortDocumentConverter& other) override { SortDocumentConverter::swap(other); }

    void clear() override { _pool->reset(); }

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(KKVSortDocConvertor);

}} // namespace build_service::builder

#endif // ISEARCH_BS_KKVSORTKEYGENERATOR_H
