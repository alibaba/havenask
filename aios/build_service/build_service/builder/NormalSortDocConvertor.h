#ifndef ISEARCH_BS_NORMALSORTDOCCONVERTOR_H
#define ISEARCH_BS_NORMALSORTDOCCONVERTOR_H

#include <indexlib/index_base/index_meta/partition_meta.h>
#include <indexlib/config/index_partition_schema.h>
#include <indexlib/document/index_document/normal_document/normal_document.h>
#include <indexlib/document/document.h>
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/builder/SortDocumentConverter.h"
#include "build_service/builder/BinaryStringUtil.h"

namespace build_service {
namespace builder {

class NormalSortDocConvertor : public SortDocumentConverter
{
public:
    NormalSortDocConvertor();
    ~NormalSortDocConvertor();
private:
    NormalSortDocConvertor(const NormalSortDocConvertor &);
    NormalSortDocConvertor& operator=(const NormalSortDocConvertor &);
    
public:
    bool init(const IE_NAMESPACE(index_base)::SortDescriptions &sortDesc,
              const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema);
    inline bool convert(const IE_NAMESPACE(document)::DocumentPtr &document,
                        SortDocument& sortDoc)
    {
        IE_NAMESPACE(document)::NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(
                IE_NAMESPACE(document)::NormalDocument, document);
        if (!doc || !doc->GetIndexDocument() || !doc->GetAttributeDocument()) {
            return false;
        }
        sortDoc._docType = doc->GetDocOperateType();
        sortDoc._pk = autil::ConstString(doc->GetIndexDocument()->GetPrimaryKey(), _pool.get());
        IE_NAMESPACE(document)::AttributeDocumentPtr attrDoc = doc->GetAttributeDocument();
        std::string sortKey;
        for (size_t i = 0; i < _sortDesc.size(); i++) {
            const autil::ConstString &field = attrDoc->GetField(_fieldIds[i]);
            NormalSortDocConvertor::MakeEmitKeyFuncType emitFunc = _emitKeyFuncs[i];
            assert(emitFunc != NULL);
            std::string key;
            emitFunc(field, key);
            sortKey += key;
        }
        sortDoc._sortKey = autil::ConstString(sortKey, _pool.get());
        _dataBuffer->write(doc);
        sortDoc._docStr = autil::ConstString(_dataBuffer->getData(),
                                             _dataBuffer->getDataLen(), _pool.get());
        _dataBuffer->clear();
        return true;
    }

    inline IE_NAMESPACE(document)::DocumentPtr getDocument(const SortDocument &sortDoc) {
        IE_NAMESPACE(document)::DocumentPtr doc;
        doc = sortDoc.deserailize();
        return doc;
    }

    void swap(SortDocumentConverter &other) override {
        SortDocumentConverter::swap(other);
    }

    void clear() override
    {
        _pool->reset();
    }

private:
    typedef void (*MakeEmitKeyFuncType)(
            const autil::ConstString &sortValue,
            std::string &emitKey);
    typedef std::vector<MakeEmitKeyFuncType> MakeEmitKeyFuncVec;

    MakeEmitKeyFuncType initEmitKeyFunc(
        const IE_NAMESPACE(index_base)::SortDescription &sortDesc);
    template<typename T>
    static void makeDescEmitKey(const autil::ConstString &sortValue,
                                std::string &emitKey);

    template<typename T>
    static void makeAscEmitKey(const autil::ConstString &sortValue,
                               std::string &emitKey);

    template <typename T>
    static void makeDescEmitKey(T sortFieldValue, std::string &emitKey) {
        BinaryStringUtil::toInvertString(sortFieldValue, emitKey);
    }

    template <typename T>
    static void makeAscEmitKey(T sortFieldValue, std::string &emitKey) {
        BinaryStringUtil::toString(sortFieldValue, emitKey);
    }


private:
    IE_NAMESPACE(index_base)::SortDescriptions _sortDesc;
    IE_NAMESPACE(config)::AttributeSchemaPtr _attrSchema;
    MakeEmitKeyFuncVec _emitKeyFuncs;
    std::vector<fieldid_t> _fieldIds;
    autil::DataBuffer *_dataBuffer;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(NormalSortDocConvertor);

}
}

#endif //ISEARCH_BS_NORMALSORTDOCCONVERTOR_H
