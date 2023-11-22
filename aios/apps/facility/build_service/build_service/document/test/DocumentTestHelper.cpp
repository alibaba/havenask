#include "build_service/document/test/DocumentTestHelper.h"

#include <cstddef>
#include <ext/alloc_traits.h>

#include "autil/Span.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "build_service/document/DocumentDefine.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/customized_config.h"
#include "indexlib/config/field_config.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/index_document/normal_document/serialized_summary_document.h"
#include "indexlib/document/locator.h"
#include "indexlib/document/normal/AttributeDocument.h"
#include "indexlib/document/normal/IndexDocument.h"
#include "indexlib/document/normal/SerializedSummaryDocument.h"
#include "indexlib/document/raw_document/raw_document_define.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/misc/common.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace build_service::document;

using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::common;

namespace build_service { namespace document {

BS_LOG_SETUP(document, DocumentTestHelper);

FakeDocument FakeDocument::constructWithAttributes(const string& pkStr, const string& attributes, DocOperateType opType)
{
    FakeDocument fakeDoc;
    fakeDoc._attributes.clear();
    fakeDoc._pkStr = pkStr;
    fakeDoc._opType = opType;
    vector<string> strVec = StringUtil::split(attributes, ";", true);
    fakeDoc._attributes.reserve(strVec.size());
    for (size_t i = 0; i < strVec.size(); i++) {
        const string& str = strVec[i];
        vector<string> oneField = StringUtil::split(str, "=", true);
        if (oneField.size() == 2) {
            fakeDoc._attributes.push_back(make_pair(oneField[0], oneField[1]));
        } else {
            fakeDoc._attributes.push_back(make_pair(string(), string()));
        }
    }
    return fakeDoc;
}

DocumentTestHelper::DocumentTestHelper() {}

DocumentTestHelper::~DocumentTestHelper() {}

RawDocumentPtr DocumentTestHelper::createRawDocStr(uint32_t seed, const std::string cmd)
{
    string seedStr = StringUtil::toString(seed);
    RawDocumentPtr rawDoc(new indexlib::document::DefaultRawDocument());
    rawDoc->setField("CMD", cmd);
    rawDoc->setField("title", "title_" + seedStr);
    rawDoc->setField("price", seedStr);
    rawDoc->setField("price2", seedStr);
    rawDoc->setField("id", seedStr);
    rawDoc->SetLocator(common::Locator(0, seed));
    return rawDoc;
}

DocumentPtr DocumentTestHelper::createDocument(const FakeDocument& fakeDoc)
{
    NormalDocumentPtr docPtr(new NormalDocument);
    IndexDocumentPtr indexDoc(new IndexDocument(docPtr->GetPool()));
    indexDoc->SetPrimaryKey(fakeDoc._pkStr);
    docPtr->SetIndexDocument(indexDoc);
    if (fakeDoc._hasSummary) {
        SerializedSummaryDocumentPtr summaryDoc(new SerializedSummaryDocument());
        docPtr->SetSummaryDocument(summaryDoc);
    }

    common::Locator locator(0, fakeDoc._locator);
    docPtr->SetLocator(locator);

    AttributeDocumentPtr attrDoc(new AttributeDocument());
    for (size_t i = 0; i < fakeDoc._attributes.size(); i++) {
        const string& fieldDesc = fakeDoc._attributes[i].first;
        const string& value = fakeDoc._attributes[i].second;
        if (fieldDesc.empty()) {
            continue;
        }
        vector<string> strVec = StringUtil::split(fieldDesc, "#", true);
        if (strVec.size() != 2) {
            continue;
        }

        FieldConfigPtr fieldConfig(new FieldConfig);
        fieldConfig->SetFieldId((fieldid_t)i);
        fieldConfig->SetFieldName(strVec[0]);
        fieldConfig->SetFieldType(FieldConfig::StrToFieldType(strVec[1]));

        if (value == fieldConfig->GetNullFieldLiteralString()) {
            attrDoc->SetNullField((fieldid_t)i);
        } else {
            auto attrConfig = std::make_shared<indexlib::config::AttributeConfig>();
            attrConfig->Init(fieldConfig);
            std::shared_ptr<indexlibv2::index::AttributeConvertor> convertor(
                indexlibv2::index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
            StringView sortField(value);
            StringView encodeStr = convertor->Encode(sortField, docPtr->GetPool());
            attrDoc->SetField((fieldid_t)i, encodeStr);
        }
    }
    if (fakeDoc._attributes.size() > 0) {
        docPtr->SetAttributeDocument(attrDoc);
    }

    docPtr->SetDocId(fakeDoc._docId);
    docPtr->SetDocOperateType(fakeDoc._opType);
    uint32_t subDocCount = fakeDoc._subDocCount;
    FakeDocument fakeSubDoc = fakeDoc;
    fakeSubDoc._subDocCount = 0;
    for (uint32_t i = 0; i < subDocCount; i++) {
        NormalDocumentPtr subDoc = DYNAMIC_POINTER_CAST(NormalDocument, createDocument(fakeSubDoc));
        docPtr->AddSubDocument(subDoc);
    }
    return docPtr;
}

ProcessedDocumentPtr DocumentTestHelper::createProcessedDocument(const FakeProcessedDoc& fakeProcessedDoc)
{
    ProcessedDocumentPtr docPtr(new ProcessedDocument);
    docPtr->TEST_setDocument(createDocument(fakeProcessedDoc._fakeDoc));
    docPtr->setLocator(fakeProcessedDoc._locator);
    vector<string> clusterMetaStrs = StringTokenizer::tokenize(StringView(fakeProcessedDoc._clusterMeta), ',');
    ProcessedDocument::DocClusterMetaVec metaVec;
    metaVec.resize(clusterMetaStrs.size());
    for (size_t i = 0; i < clusterMetaStrs.size(); i++) {
        vector<string> metaVecStr = StringTokenizer::tokenize(StringView(clusterMetaStrs[i]), ':');
        metaVec[i].clusterName = metaVecStr[0];
        hashid_t hashId = 0;
        if (metaVecStr.size() == 2) {
            hashId = StringUtil::fromString<hashid_t>(metaVecStr[1].c_str());
        }
        metaVec[i].hashId = hashId;
    }
    docPtr->setDocClusterMetaVec(metaVec);
    return docPtr;
}

void DocumentTestHelper::checkDocument(uint32_t seed, const ProcessedDocumentVecPtr& docs)
{
    common::Locator locator(0, seed);
    // TODO: add cluster meta check.
    EXPECT_EQ(size_t(2), docs->size());

    EXPECT_EQ(locator, (*docs)[0]->getLocator());
    auto doc1 = (*docs)[0]->getDocument();
    checkDocument(FakeDocument(StringUtil::toString(seed), ADD_DOC, true, true), doc1);

    EXPECT_EQ(locator, (*docs)[1]->getLocator());
    auto doc2 = (*docs)[1]->getDocument();
    checkDocument(FakeDocument(StringUtil::toString(seed), ADD_DOC, false, true), doc2);
}

void DocumentTestHelper::checkDocument(const FakeDocument& fakeDoc,
                                       const std::shared_ptr<indexlibv2::document::IDocument>& doc)
{
    NormalDocumentPtr document = DYNAMIC_POINTER_CAST(NormalDocument, doc);
    ASSERT_TRUE(document.get());

    EXPECT_TRUE(document->GetIndexDocument().get());
    EXPECT_EQ(fakeDoc._pkStr, document->GetIndexDocument()->GetPrimaryKey());
    EXPECT_EQ(fakeDoc._opType, document->GetDocOperateType());
    EXPECT_EQ(fakeDoc._hasSummary, document->GetSummaryDocument() != NULL);
    EXPECT_EQ(fakeDoc._attributes.size() > 0, document->GetAttributeDocument() != NULL);
}

}} // namespace build_service::document
