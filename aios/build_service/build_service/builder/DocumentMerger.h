#ifndef ISEARCH_BS_DOCUMENTMERGER_H
#define ISEARCH_BS_DOCUMENTMERGER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/builder/NormalSortDocConvertor.h"
#include <indexlib/document/index_document/normal_document/normal_document.h>

namespace build_service {
namespace builder {

class DocumentMerger
{
public:
    DocumentMerger(std::vector<SortDocument> &documentVec,
                   NormalSortDocConvertor *converter,
                   bool hasSub)
        : _documentVec(documentVec)
        , _converter(converter)
        , _hasSub(hasSub)
    {}
    ~DocumentMerger() {};
private:
    DocumentMerger(const DocumentMerger &);
    DocumentMerger& operator=(const DocumentMerger &);
private:
    typedef IE_NAMESPACE(document)::NormalDocumentPtr Doc;
    typedef IE_NAMESPACE(document)::AttributeDocumentPtr AttrDoc;
public:
    void clear() {
        _lastUpdateDoc.reset();
        _posVec.clear();
    }

    const std::vector<uint32_t> &getResult() {
        flushUpdateDoc();
        return _posVec;
    }
    void merge(uint32_t pos) {
        if (_posVec.empty() || _hasSub) {
            pushOneDoc(pos);
            return ;
        }

        SortDocument &lastDoc = _documentVec[_posVec.back()];
        SortDocument &nowDoc = _documentVec[pos];
        if (nowDoc._docType != UPDATE_FIELD || lastDoc._docType != UPDATE_FIELD) {
            flushUpdateDoc();
            pushOneDoc(pos);
        } else {
            if (!_lastUpdateDoc) {
                _lastUpdateDoc = lastDoc.deserailize();
            }
            Doc doc = nowDoc.deserailize();
            mergeUpdateDoc(_lastUpdateDoc, doc, _lastUpdateDoc->GetPool());
        }
    }

private:
    void pushOneDoc(uint32_t pos) {
        _lastUpdateDoc.reset();
        _posVec.push_back(pos);
    }

    void flushUpdateDoc() {
        if (!_lastUpdateDoc) {
            return ;
        }
        SortDocument sortDoc;
        if (_converter->convert(_lastUpdateDoc, sortDoc)) {
            _posVec.back() = _documentVec.size();
            _documentVec.push_back(sortDoc);
        } else {
            assert(false);
        }
    }

    static void mergeUpdateDoc(const Doc &to, const Doc &from,
                               autil::mem_pool::Pool *pool)
    {
        const AttrDoc &toAttrDoc = to->GetAttributeDocument();
        const AttrDoc &fromAttrDoc = from->GetAttributeDocument();
        IE_NAMESPACE(document)::AttributeDocument::Iterator it =
            fromAttrDoc->CreateIterator();
        while (it.HasNext()) {
            fieldid_t fieldId = 0;
            const autil::ConstString& value = it.Next(fieldId);
            autil::ConstString newValue(value, pool);
            toAttrDoc->SetField(fieldId, newValue);
        }
    }

private:
    std::vector<SortDocument> &_documentVec;
    NormalSortDocConvertor *_converter;
    std::vector<uint32_t> _posVec;
    Doc _lastUpdateDoc;
    bool _hasSub;
private:
    BS_LOG_DECLARE();
};

BS_LOG_SETUP(builder, DocumentMerger);

}
}

#endif //ISEARCH_BS_DOCUMENTMERGER_H
