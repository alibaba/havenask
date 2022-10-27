#ifndef ISEARCH_BS_SORTDOCUMENTCONTAINER_H
#define ISEARCH_BS_SORTDOCUMENTCONTAINER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/builder/SortDocumentConverter.h"
#include "build_service/builder/NormalSortDocConvertor.h"
#include "build_service/builder/SortDocumentSorter.h"
#include "build_service/builder/NormalSortDocSorter.h"
#include "build_service/common/Locator.h"
#include <autil/mem_pool/Pool.h>

namespace build_service {
namespace builder {

class SortDocumentContainer
{
public:
    typedef std::vector<SortDocument> DocumentVector;

public:
    SortDocumentContainer();
    ~SortDocumentContainer();
private:
    SortDocumentContainer(const SortDocumentContainer &);
    SortDocumentContainer& operator=(const SortDocumentContainer &);
public:
    bool init(const IE_NAMESPACE(index_base)::SortDescriptions &sortDesc,
              const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &attrSchemaPtr,
              const common::Locator& locator);
public:
    inline void pushDocument(const IE_NAMESPACE(document)::DocumentPtr &doc)
    {
        updateLocator(doc->GetLocator().GetLocator());
        doc->SetLocator(IE_NAMESPACE(document)::Locator()); // set locator null, reduce memory
        SortDocument sortDoc;

        if (!((NormalSortDocConvertor*)(_converter.get()))->convert(doc, sortDoc)) {
            return;
        }

        if (!_hasPrimaryKey) {
            _documentVec.push_back(sortDoc);
            _sortQueueMemUse += sortDoc.size();
            return;
        }

        _documentVec.push_back(sortDoc);
        _sortQueueMemUse += sortDoc.size();
        uint32_t currentCursor = _documentVec.size() - 1;
        ((NormalSortDocSorter*)(_sortDocumentSorter.get()))->push(sortDoc, currentCursor);
    }
    void swap(SortDocumentContainer &other) {
        std::swap(_hasPrimaryKey, other._hasPrimaryKey);
        std::swap(_hasSub, other._hasSub);
        std::swap(_processCursor, other._processCursor);
        std::swap(_sortQueueMemUse, other._sortQueueMemUse);
        std::swap(_documentVec, other._documentVec);
        std::swap(_orderVec, other._orderVec);
        _converter.swap(other._converter);
        std::swap(_firstLocator, other._firstLocator);
        std::swap(_lastLocator, other._lastLocator);
        std::swap(_tableType, other._tableType);
        resetSortResource();
        other.resetSortResource();
    }
    void sortDocument();
    void clear();
    inline size_t allDocCount() const {
        return _documentVec.size();
    }
    inline size_t sortedDocCount() const {
        return _orderVec.size();
    }
    inline size_t memUse() const {
        return (_sortQueueMemUse + _sortDocumentSorter->getMemUse()) / 1024 / 1024;
    }
    inline size_t nonDocumentMemUse() const {
        return _sortDocumentSorter->getMemUse() / 1024 / 1024;
    }
    inline bool empty() const {
        return _documentVec.empty();
    }

    IE_NAMESPACE(document)::DocumentPtr operator[] (size_t idx);

    inline void incProcessCursor() {
        _processCursor++;
        assert(_processCursor <= _orderVec.size());
    }
    inline size_t getUnprocessedCount() const {
        return _orderVec.size() - _processCursor;
    }
private:
    void updateLocator(const autil::ConstString &locatorStr);
    void resetSortResource();
private:
    static const size_t RESERVED_QUEUE_SIZE;
private:
    bool _hasPrimaryKey;
    bool _hasSub;
    size_t _processCursor;
    size_t _sortQueueMemUse;
    DocumentVector _documentVec;
    std::vector<uint32_t> _orderVec;
    SortDocumentConverterPtr _converter;
    common::Locator _firstLocator;
    common::Locator _lastLocator;
    std::unique_ptr<SortDocumentSorter> _sortDocumentSorter;
    heavenask::indexlib::TableType _tableType;
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_SORTDOCUMENTCONTAINER_H
