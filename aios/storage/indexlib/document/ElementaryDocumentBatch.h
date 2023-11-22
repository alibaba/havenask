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

#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/util/Exception.h"
namespace indexlibv2::document {

class ElementaryDocumentBatch : public IDocumentBatch
{
public:
    ElementaryDocumentBatch() {}
    virtual ~ElementaryDocumentBatch() {}

public:
    void AddDocument(DocumentPtr doc) override
    {
        assert(false);
        INDEXLIB_THROW(indexlib::util::UnSupportedException, "AddDocument is not supported in ElementaryDocumentBatch");
    }

    std::shared_ptr<IDocument> operator[](int64_t docIdx) const override
    {
        assert(false);
        INDEXLIB_THROW(indexlib::util::UnSupportedException,
                       "single document access is not supported in ElementaryDocumentBatch");
        return nullptr;
    }

    std::shared_ptr<IDocument> TEST_GetDocument(int64_t docIdx) const override
    {
        assert(false);
        INDEXLIB_THROW(indexlib::util::UnSupportedException,
                       "single document access is not supported in ElementaryDocumentBatch");
        return nullptr;
    }
};

} // namespace indexlibv2::document
