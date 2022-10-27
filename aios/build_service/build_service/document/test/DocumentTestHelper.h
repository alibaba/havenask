#ifndef ISEARCH_BS_DOCUMENTTESTHELPER_H
#define ISEARCH_BS_DOCUMENTTESTHELPER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/document/RawDocument.h"
#include "build_service/document/ProcessedDocument.h"
#include <autil/mem_pool/Pool.h>

namespace build_service {
namespace document {

struct FakeDocument {
public:
    FakeDocument(const std::string &pkStr = "",
                 DocOperateType opType = ADD_DOC,
                 bool hasSummary = true,
                 bool hasAttribute = true,
                 docid_t docId = 0,
                 uint32_t sortField = 0,
                 uint32_t subDocCount = 0,
                 int64_t locator = 0)
        : _pkStr(pkStr)
        , _opType(opType)
        , _hasSummary(hasSummary)
        , _docId(docId)
        , _subDocCount(subDocCount)
        , _locator(locator)
    {
        std::pair<std::string, std::string> attribute("field#uint32",
                autil::StringUtil::toString(sortField));
        _attributes.push_back(attribute);
    }
    ~FakeDocument() {
    }
public:
    // format of attribute
    // fieldName#fieldType=fieldValue;fieldName#fieldType=fieldValue
    // fieldName;fieldName#fieldType=fieldValue
    static FakeDocument constructWithAttributes(const std::string &pkStr, 
            const std::string &attributes, DocOperateType opType = ADD_DOC);
public:
    std::string _pkStr;
    DocOperateType _opType;
    bool _hasSummary;
    docid_t _docId;
    uint32_t _subDocCount;
    int64_t _locator;
    std::vector<std::pair<std::string, std::string> > _attributes;
};
struct FakeProcessedDoc {
public:
    FakeProcessedDoc(const FakeDocument &fakeDoc,
                     const common::Locator &locator = common::Locator(),
                     const std::string &clusterMeta = "")
        : _fakeDoc(fakeDoc)
        , _locator(locator)
        , _clusterMeta(clusterMeta)
    {}
    FakeProcessedDoc(const FakeDocument &fakeDoc,
                     int64_t locator,
                     const std::string &clusterMeta)
        : _fakeDoc(fakeDoc)
        , _locator(locator)
        , _clusterMeta(clusterMeta)
    {}
    ~FakeProcessedDoc() {
    }
public:
    FakeDocument _fakeDoc;
    common::Locator _locator;
    std::string _clusterMeta;
};

class DocumentTestHelper
{
private:
    DocumentTestHelper();
    ~DocumentTestHelper();
    DocumentTestHelper(const DocumentTestHelper &);
    DocumentTestHelper& operator=(const DocumentTestHelper &);
public:
    static RawDocumentPtr createRawDocStr(uint32_t seed, const std::string cmd = "add");

    static IE_NAMESPACE(document)::DocumentPtr createDocument(
            const FakeDocument &fakeDoc);

    static void checkDocument(const FakeDocument &fakeDoc,
                              const IE_NAMESPACE(document)::DocumentPtr &document);

    static ProcessedDocumentPtr createProcessedDocument(
            const FakeProcessedDoc &fakeProcessedDoc);

    static void checkDocument(uint32_t seed, const ProcessedDocumentVecPtr &doc);
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DocumentTestHelper);

}
}

#endif //ISEARCH_BS_DOCUMENTTESTHELPER_H
