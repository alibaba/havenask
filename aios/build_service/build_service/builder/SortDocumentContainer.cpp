#include "build_service/builder/SortDocumentContainer.h"
#include <autil/DataBuffer.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);

namespace build_service {
namespace builder {
BS_LOG_SETUP(builder, SortDocumentContainer);

class DocumentComparatorWrapper {
public:
    typedef SortDocumentContainer::DocumentVector DocumentVector;
public:
    DocumentComparatorWrapper(const DocumentVector &documentVec)
        : _documentVec(documentVec)
    {
    }
    ~DocumentComparatorWrapper() {}
public:
    inline bool operator() (uint32_t left, uint32_t right)
    {
        const SortDocument &leftDoc = _documentVec[left];
        const SortDocument &rightDoc = _documentVec[right];
        assert(leftDoc._sortKey.size() == rightDoc._sortKey.size());
        int r = memcmp(leftDoc._sortKey.data(), rightDoc._sortKey.data(),
                       rightDoc._sortKey.size());
        if (r < 0) {
            return true;
        } else if (r > 0) {
            return false;
        } else {
            return left < right;
        }
    }
private:
    const DocumentVector &_documentVec;
    BS_LOG_DECLARE();
};

BS_LOG_SETUP(builder, DocumentComparatorWrapper);
const size_t SortDocumentContainer::RESERVED_QUEUE_SIZE = 128 * 1024 * 1024;

SortDocumentContainer::SortDocumentContainer()
    : _hasPrimaryKey(false)
    , _hasSub(false)
    , _processCursor(0)
    , _sortQueueMemUse(0)
{
    _documentVec.reserve(RESERVED_QUEUE_SIZE);
    _orderVec.reserve(RESERVED_QUEUE_SIZE);
}

SortDocumentContainer::~SortDocumentContainer() {
}

bool SortDocumentContainer::init(const SortDescriptions &sortDesc,
                                 const IndexPartitionSchemaPtr &schema,
                                 const common::Locator& firstLocator)
{
    _firstLocator = firstLocator;
    _lastLocator = firstLocator;
    _tableType = schema->GetTableType();
    _hasPrimaryKey = false;
    const IndexSchemaPtr &indexSchemaPtr = schema->GetIndexSchema();
    if (indexSchemaPtr) {
        _hasPrimaryKey = indexSchemaPtr->HasPrimaryKeyIndex();
    }
    _hasSub = schema->GetSubIndexPartitionSchema();
    NormalSortDocConvertor* convertor = new NormalSortDocConvertor();
    _converter.reset(convertor);
    if (!convertor->init(sortDesc, schema)) {
        return false;
    }
    resetSortResource();
    return true;
}

void SortDocumentContainer::sortDocument() {
    if (empty()) {
        return;
    }
    if (!_hasPrimaryKey) {
        for (size_t i = 0; i < _documentVec.size(); ++i) {
            _orderVec.push_back(i);
        }
        std::sort(_orderVec.begin(), _orderVec.end(),
                  DocumentComparatorWrapper(_documentVec));
        return;
    }
    _sortDocumentSorter->sort(_orderVec);
    resetSortResource();
}

void SortDocumentContainer::clear() {
    _processCursor = 0;
    _sortQueueMemUse = 0;
    _documentVec.clear();
    _orderVec.clear();
    _converter->clear();
    _firstLocator = _lastLocator;
    _lastLocator = _lastLocator;
}

void SortDocumentContainer::resetSortResource() {
    _sortDocumentSorter.reset(NULL);
    _sortDocumentSorter.reset(
            new NormalSortDocSorter(
                    _documentVec, (NormalSortDocConvertor*)_converter.get(), _hasSub));
}

DocumentPtr SortDocumentContainer::operator[] (size_t idx) {
    assert(idx < sortedDocCount());
    const SortDocument &sortDoc = _documentVec[_orderVec[idx]];
    DocumentPtr doc = ((NormalSortDocConvertor*)(_converter.get()))->getDocument(sortDoc);
    common::Locator locator = _firstLocator;
    if (idx + 1 == sortedDocCount()) {
        locator = _lastLocator;
    }
    doc->SetLocator(locator.toString());
    return doc;
}

void SortDocumentContainer::updateLocator(const ConstString &locatorStr) {
    common::Locator locator;
    if (!locator.fromString(locatorStr)) {
        string errorMsg = "deserialize locator[" +
                          string(locatorStr.data(), locatorStr.size()) +
                          "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return;
    }
    _lastLocator = locator;
}


}
}
