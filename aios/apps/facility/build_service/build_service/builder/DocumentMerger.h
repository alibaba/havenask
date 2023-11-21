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

#include "build_service/builder/NormalSortDocConvertor.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

namespace build_service { namespace builder {

class DocumentMerger
{
public:
    DocumentMerger(std::vector<SortDocument>& documentVec, NormalSortDocConvertor* converter, bool hasSub,
                   bool hasInvertedIndexUpdate)
        : _documentVec(documentVec)
        , _converter(converter)
        , _hasSub(hasSub)
        , _hasInvertedIndexUpdate(hasInvertedIndexUpdate)
    {
    }
    ~DocumentMerger() {};

private:
    DocumentMerger(const DocumentMerger&);
    DocumentMerger& operator=(const DocumentMerger&);

private:
    typedef indexlib::document::NormalDocumentPtr Doc;
    typedef indexlib::document::AttributeDocumentPtr AttrDoc;

public:
    void clear()
    {
        _lastUpdateDoc.reset();
        _posVec.clear();
    }

    const std::vector<uint32_t>& getResult()
    {
        flushUpdateDoc();
        return _posVec;
    }
    void merge(uint32_t pos)
    {
        if (_posVec.empty() || _hasSub || _hasInvertedIndexUpdate) {
            pushOneDoc(pos);
            return;
        }

        SortDocument& lastDoc = _documentVec[_posVec.back()];
        SortDocument& nowDoc = _documentVec[pos];
        if (nowDoc._docType != UPDATE_FIELD || lastDoc._docType != UPDATE_FIELD) {
            flushUpdateDoc();
            pushOneDoc(pos);
        } else {
            if (!_lastUpdateDoc) {
                _lastUpdateDoc = lastDoc.deserailize<indexlib::document::NormalDocumentPtr>();
            }
            Doc doc = nowDoc.deserailize<indexlib::document::NormalDocumentPtr>();
            mergeUpdateDoc(_lastUpdateDoc, doc, _lastUpdateDoc->GetPool());
        }
    }

private:
    void pushOneDoc(uint32_t pos)
    {
        _lastUpdateDoc.reset();
        _posVec.push_back(pos);
    }

    void flushUpdateDoc()
    {
        if (!_lastUpdateDoc) {
            return;
        }
        SortDocument sortDoc;
        if (_converter->convert(_lastUpdateDoc, sortDoc)) {
            _posVec.back() = _documentVec.size();
            _documentVec.push_back(sortDoc);
        } else {
            assert(false);
        }
    }

    static void mergeUpdateDoc(const Doc& to, const Doc& from, autil::mem_pool::Pool* pool)
    {
        const AttrDoc& toAttrDoc = to->GetAttributeDocument();
        const AttrDoc& fromAttrDoc = from->GetAttributeDocument();
        indexlib::document::AttributeDocument::Iterator it = fromAttrDoc->CreateIterator();
        while (it.HasNext()) {
            fieldid_t fieldId = 0;
            const autil::StringView& value = it.Next(fieldId);
            autil::StringView newValue = autil::MakeCString(value, pool);
            toAttrDoc->SetField(fieldId, newValue);
        }
    }

private:
    std::vector<SortDocument>& _documentVec;
    NormalSortDocConvertor* _converter;
    std::vector<uint32_t> _posVec;
    Doc _lastUpdateDoc;
    bool _hasSub;
    bool _hasInvertedIndexUpdate;

private:
    BS_LOG_DECLARE();
};

BS_LOG_SETUP(builder, DocumentMerger);

}} // namespace build_service::builder
