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

    std::shared_ptr<FieldConfig> hashIdFieldConfig(
        new FieldConfig(DocumentInfoToAttributeRewriter::VIRTUAL_HASH_ID_FIELD_NAME,
                        DocumentInfoToAttributeRewriter::VIRTUAL_HASH_ID_FIELD_TYPE,
                        /*multiValue = */ false));
    std::shared_ptr<AttributeConfig> hashIdAttrConfig(new AttributeConfig);
    ASSERT_TRUE(hashIdAttrConfig->Init(hashIdFieldConfig).IsOK());
    hashIdAttrConfig->SetAttrId(0);

    std::shared_ptr<FieldConfig> concurrentIdxFieldConfig(
        new FieldConfig(DocumentInfoToAttributeRewriter::VIRTUAL_CONCURRENT_IDX_FIELD_NAME,
                        DocumentInfoToAttributeRewriter::VIRTUAL_CONCURRENT_IDX_FIELD_TYPE,
                        /*multiValue = */ false));
    std::shared_ptr<AttributeConfig> concurrentIdxAttrConfig(new AttributeConfig);
    ASSERT_TRUE(concurrentIdxAttrConfig->Init(concurrentIdxFieldConfig).IsOK());
    concurrentIdxAttrConfig->SetAttrId(0);

    fieldid_t tsFieldId = 1;
    fieldid_t hashIdFieldId = 2;
    fieldid_t concurrentIdxFieldId = 3;
    tsFieldConfig->SetFieldId(tsFieldId);
    hashIdFieldConfig->SetFieldId(hashIdFieldId);
    concurrentIdxFieldConfig->SetFieldId(concurrentIdxFieldId);
    std::shared_ptr<IDocumentRewriter> rewriter(
        new DocumentInfoToAttributeRewriter(tsAttrConfig, hashIdAttrConfig, concurrentIdxAttrConfig));
    auto normalDocument = new indexlibv2::document::NormalDocument;
    normalDocument->SetDocOperateType(ADD_DOC);
    std::shared_ptr<AttributeDocument> attrDoc(new AttributeDocument);
    std::shared_ptr<IDocument> doc(normalDocument);
    normalDocument->SetAttributeDocument(attrDoc);
    normalDocument->SetDocInfo({200, 100, 15});
    std::shared_ptr<IDocumentBatch> documentBatch(new DocumentBatch);
    documentBatch->AddDocument(doc);
    ASSERT_TRUE(rewriter->Rewrite(documentBatch.get()).IsOK());
    {
        auto tsConvertor =
            indexlibv2::index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(tsAttrConfig);
        auto valueMeta = tsConvertor->Decode(attrDoc->GetField(tsFieldId));
        int64_t ts = *(int64_t*)valueMeta.data.data();
        ASSERT_EQ(100, ts);
        delete tsConvertor;
    }
    {
        auto hashIdConvertor =
            indexlibv2::index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(hashIdAttrConfig);
        auto valueMeta = hashIdConvertor->Decode(attrDoc->GetField(hashIdFieldId));
        uint16_t hashId = *(uint16_t*)valueMeta.data.data();
        ASSERT_EQ(200, hashId);
        delete hashIdConvertor;
    }
    {
        auto concurrentIdxConvertor =
            indexlibv2::index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(concurrentIdxAttrConfig);
        auto valueMeta = concurrentIdxConvertor->Decode(attrDoc->GetField(concurrentIdxFieldId));
        uint32_t concurrentIdx = *(uint32_t*)valueMeta.data.data();
        ASSERT_EQ(15, concurrentIdx);
        delete concurrentIdxConvertor;
    }
}

}} // namespace indexlibv2::document
