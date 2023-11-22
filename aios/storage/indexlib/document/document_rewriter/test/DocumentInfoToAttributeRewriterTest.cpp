#include "indexlib/document/document_rewriter/DocumentInfoToAttributeRewriter.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/document/DocumentBatch.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "unittest/unittest.h"

using namespace std;
using indexlib::document::AttributeDocument;
using indexlib::document::NormalDocument;
using indexlibv2::config::FieldConfig;
using indexlibv2::index::AttributeConfig;

namespace indexlibv2 { namespace document {
class DocumentInfoToAttributeRewriterTest : public TESTBASE
{
};

TEST_F(DocumentInfoToAttributeRewriterTest, TestSimpleProcess)
{
    std::shared_ptr<FieldConfig> tsFieldConfig(
        new FieldConfig(DocumentInfoToAttributeRewriter::VIRTUAL_TIMESTAMP_FIELD_NAME,
                        DocumentInfoToAttributeRewriter::VIRTUAL_TIMESTAMP_FIELD_TYPE,
                        /*multiValue = */ false));
    std::shared_ptr<AttributeConfig> tsAttrConfig(new AttributeConfig);
    ASSERT_TRUE(tsAttrConfig->Init(tsFieldConfig).IsOK());
    tsAttrConfig->SetAttrId(0);

    std::shared_ptr<FieldConfig> docInfoFieldConfig(
        new FieldConfig(DocumentInfoToAttributeRewriter::VIRTUAL_DOC_INFO_FIELD_NAME,
                        DocumentInfoToAttributeRewriter::VIRTUAL_DOC_INFO_FIELD_TYPE,
                        /*multiValue = */ false));
    std::shared_ptr<AttributeConfig> docInfoAttrConfig(new AttributeConfig);
    ASSERT_TRUE(docInfoAttrConfig->Init(docInfoFieldConfig).IsOK());
    docInfoAttrConfig->SetAttrId(0);

    fieldid_t tsFieldId = 1;
    fieldid_t docInfoFieldId = 2;
    tsFieldConfig->SetFieldId(tsFieldId);
    docInfoFieldConfig->SetFieldId(docInfoFieldId);
    std::shared_ptr<IDocumentRewriter> rewriter(new DocumentInfoToAttributeRewriter(tsAttrConfig, docInfoAttrConfig));
    auto normalDocument = new indexlibv2::document::NormalDocument;
    normalDocument->SetDocOperateType(ADD_DOC);
    std::shared_ptr<AttributeDocument> attrDoc(new AttributeDocument);
    std::shared_ptr<IDocument> doc(normalDocument);
    normalDocument->SetAttributeDocument(attrDoc);
    normalDocument->SetDocInfo({200, 100, 15, 42});
    std::shared_ptr<IDocumentBatch> documentBatch(new DocumentBatch);
    documentBatch->AddDocument(doc);
    ASSERT_TRUE(rewriter->Rewrite(documentBatch.get()).IsOK());

    auto tsConvertor = indexlibv2::index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(tsAttrConfig);
    auto valueMeta1 = tsConvertor->Decode(attrDoc->GetField(tsFieldId));
    int64_t ts = *(int64_t*)valueMeta1.data.data();
    ASSERT_EQ(100, ts);
    delete tsConvertor;

    auto docInfoConvertor =
        indexlibv2::index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(docInfoAttrConfig);
    auto valueMeta2 = docInfoConvertor->Decode(attrDoc->GetField(docInfoFieldId));
    uint64_t docInfoValue = *(uint64_t*)valueMeta2.data.data();
    auto docInfo = DocumentInfoToAttributeRewriter::DecodeDocInfo(docInfoValue, ts);
    ASSERT_TRUE(docInfo.has_value());
    ASSERT_EQ(200, docInfo.value().hashId);
    ASSERT_EQ(15, docInfo.value().concurrentIdx);
    ASSERT_EQ(42, docInfo.value().sourceIdx);
    ASSERT_EQ(ts, docInfo.value().timestamp);
    delete docInfoConvertor;
}

}} // namespace indexlibv2::document
