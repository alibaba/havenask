#ifndef ISEARCH_BS_DOCUMENTTESTHELPER_H
#define ISEARCH_BS_DOCUMENTTESTHELPER_H

#include "autil/mem_pool/Pool.h"
#include "build_service/common_define.h"
#include "build_service/document/ProcessedDocument.h"
#include "build_service/util/Log.h"
#include "indexlib/document/raw_document/default_raw_document.h"

namespace build_service { namespace document {

struct FakeDocument {
public:
    FakeDocument(const std::string& pkStr = "", DocOperateType opType = ADD_DOC, bool hasSummary = true,
                 bool hasAttribute = true, docid_t docId = 0, uint32_t sortField = 0, uint32_t subDocCount = 0,
                 int64_t locator = 0, bool isNull = false)
        : _pkStr(pkStr)
        , _opType(opType)
        , _hasSummary(hasSummary)
        , _docId(docId)
        , _subDocCount(subDocCount)
        , _locator(locator)
    {
        if (!isNull) {
            std::pair<std::string, std::string> attribute("field#uint32", autil::StringUtil::toString(sortField));
            _attributes.push_back(attribute);
        } else {
            std::pair<std::string, std::string> attribute("field#uint32", DEFAULT_NULL_FIELD_STRING);
            _attributes.push_back(attribute);
        }
    }

    ~FakeDocument() {}

public:
    // format of attribute
    // fieldName#fieldType=fieldValue;fieldName#fieldType=fieldValue
    // fieldName;fieldName#fieldType=fieldValue
    static FakeDocument constructWithAttributes(const std::string& pkStr, const std::string& attributes,
                                                DocOperateType opType = ADD_DOC);

public:
    std::string _pkStr;
    DocOperateType _opType;
    bool _hasSummary;
    docid_t _docId;
    uint32_t _subDocCount;
    int64_t _locator;
    std::vector<std::pair<std::string, std::string>> _attributes;
};
struct FakeProcessedDoc {
public:
    FakeProcessedDoc(const FakeDocument& fakeDoc, const common::Locator& locator = common::Locator(),
                     const std::string& clusterMeta = "")
        : _fakeDoc(fakeDoc)
        , _clusterMeta(clusterMeta)
    {
        _locator = locator;
    }
    FakeProcessedDoc(const FakeDocument& fakeDoc, int64_t locator, const std::string& clusterMeta)
        : _fakeDoc(fakeDoc)
        , _locator(0, locator)
        , _clusterMeta(clusterMeta)
    {
    }
    ~FakeProcessedDoc() {}

public:
    FakeDocument _fakeDoc;
    common::Locator _locator;
    std::string _clusterMeta;
};

class FakeRawDocument : public indexlib::document::DefaultRawDocument, public indexlib::document::Document
{
public:
    uint32_t GetDocBinaryVersion() const override
    {
        assert(0);
        return {};
    }
    void SetDocId(docid_t docId) override { assert(0); }
    void setDocOperateType(DocOperateType type) override
    {
        indexlib::document::DefaultRawDocument::setDocOperateType(type);
        indexlib::document::Document::SetDocOperateType(type);
    }
    void SetDocOperateType(DocOperateType type) override
    {
        indexlib::document::DefaultRawDocument::setDocOperateType(type);
        indexlib::document::Document::SetDocOperateType(type);
    }
    void SetLocator(const indexlibv2::framework::Locator& locator) override
    {
        indexlib::document::DefaultRawDocument::SetLocator(locator);
        indexlib::document::Document::SetLocator(locator);
    }
    void setDocTimestamp(int64_t timestamp) override
    {
        indexlib::document::DefaultRawDocument::setDocTimestamp(timestamp);
        indexlib::document::Document::SetTimestamp(timestamp);
    }
    void SetDocInfo(const indexlibv2::document::IDocument::DocInfo& docInfo) override
    {
        indexlib::document::DefaultRawDocument::SetDocInfo(docInfo);
        indexlib::document::Document::SetDocInfo(docInfo);
    }

private:
    void DoSerialize(autil::DataBuffer& dataBuffer, uint32_t serializedVersion) const override { assert(0); }
    void DoDeserialize(autil::DataBuffer& dataBuffer, uint32_t serializedVersion) override { assert(0); }
};

class DocumentTestHelper
{
private:
    DocumentTestHelper();
    ~DocumentTestHelper();
    DocumentTestHelper(const DocumentTestHelper&);
    DocumentTestHelper& operator=(const DocumentTestHelper&);

public:
    static RawDocumentPtr createRawDocStr(uint32_t seed, const std::string cmd = "add");

    static indexlib::document::DocumentPtr createDocument(const FakeDocument& fakeDoc);

    static void checkDocument(const FakeDocument& fakeDoc,
                              const std::shared_ptr<indexlibv2::document::IDocument>& document);

    static ProcessedDocumentPtr createProcessedDocument(const FakeProcessedDoc& fakeProcessedDoc);

    static void checkDocument(uint32_t seed, const ProcessedDocumentVecPtr& doc, bool checkSource);
    static void checkDocument(uint32_t seed, const ProcessedDocumentVecPtr& doc);

    static void checkDocumentSource(const ProcessedDocumentVecPtr& docs);

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DocumentTestHelper);

}} // namespace build_service::document

#endif // ISEARCH_BS_DOCUMENTTESTHELPER_H
