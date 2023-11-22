#include "indexlib/document/normal/ClassifiedDocument.h"

#include "autil/legacy/jsonizable.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/document/normal/SourceDocument.h"
#include "indexlib/document/normal/SourceFormatter.h"
#include "indexlib/document/raw_document/DefaultRawDocument.h"
#include "indexlib/index/source/config/SourceIndexConfig.h"
#include "unittest/unittest.h"

namespace indexlibv2::document {

class ClassifiedDocumentTest : public TESTBASE
{
public:
    ClassifiedDocumentTest() = default;
    ~ClassifiedDocumentTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void ClassifiedDocumentTest::setUp() {}

void ClassifiedDocumentTest::tearDown() {}

TEST_F(ClassifiedDocumentTest, TestCreateSourceDocument)
{
    auto rawDoc = std::make_shared<DefaultRawDocument>();
    rawDoc->setField("f1", "v1");
    rawDoc->setField("f2", "v2");
    rawDoc->setField("f3", "v3");
    rawDoc->setField("f4", "v4");
    rawDoc->setField("f5", "v5");

    std::vector<std::vector<std::string>> fieldGroups = {{"f5", "f2", "f3", "f1"}, {"f2", "f9999", "f4"}};
    ClassifiedDocument doc;
    std::shared_ptr<RawDocument::Snapshot> snapshot(rawDoc->GetSnapshot());
    doc.createSourceDocument(fieldGroups, snapshot.get());

    auto srcDoc = doc.getSourceDocument();
    ASSERT_TRUE(srcDoc);
    ASSERT_EQ(autil::StringView("v5"), srcDoc->GetField(0, "f5"));
    ASSERT_EQ(autil::StringView::empty_instance(), srcDoc->GetField(0, "f4"));
    ASSERT_EQ(autil::StringView("v4"), srcDoc->GetField(1, "f4"));
    ASSERT_TRUE(srcDoc->HasField(1, "f9999"));
    ASSERT_TRUE(srcDoc->IsNonExistField(1, "f9999"));
    ASSERT_FALSE(srcDoc->HasField(0, "f9999"));
}

TEST_F(ClassifiedDocumentTest, TestCreateEmptySourceDocument)
{
    auto rawDoc = std::make_shared<DefaultRawDocument>();

    rawDoc->setField("f1", "v1");
    rawDoc->setField("f2", "v2");
    rawDoc->setField("f3", "v3");
    rawDoc->setField("f4", "v4");
    rawDoc->setField("f5", "v5");

    std::vector<std::vector<std::string>> fieldGroups = {{"f9"}};
    ClassifiedDocument doc;
    std::shared_ptr<RawDocument::Snapshot> snapshot(rawDoc->GetSnapshot());
    doc.createSourceDocument(fieldGroups, snapshot.get());

    auto srcDoc = doc.getSourceDocument();
    ASSERT_TRUE(srcDoc);
    ASSERT_EQ(1u, srcDoc->_data.size());
    ASSERT_EQ(1u, srcDoc->_data[0]->fieldsName.size());
    ASSERT_EQ(autil::StringView::empty_instance(), srcDoc->GetField(0, "f9"));
    ASSERT_TRUE(srcDoc->HasField(0, "f9"));
    ASSERT_TRUE(srcDoc->IsNonExistField(0, "f9"));
}

TEST_F(ClassifiedDocumentTest, TestGetSerialziedSourceDocument)
{
    std::string configStr = R"(
    {
        "group_configs": [
            {
                "field_mode": "specified_field",
                "fields": ["f5", "f2", "f3", "f1"]
            },
            {
                "field_mode": "specified_field",
                "fields": ["f2", "f9999", "f4"]
            }
        ]
    })";
    config::MutableJson mutableJson;
    config::IndexConfigDeserializeResource resource({}, mutableJson);
    auto config = std::make_shared<config::SourceIndexConfig>();
    autil::legacy::Any any = autil::legacy::json::ParseJson(configStr);
    config->Deserialize(any, 0, resource);

    std::vector<std::vector<std::string>> fieldGroups = {{"f5", "f2", "f3", "f1"}, {"f2", "f9999", "f4"}};
    auto rawDoc = std::make_shared<DefaultRawDocument>();
    rawDoc->setField("f1", "v1");
    rawDoc->setField("f2", "v2");
    rawDoc->setField("f3", "v3");
    rawDoc->setField("f4", "v4");
    rawDoc->setField("f5", "v5");
    ClassifiedDocument doc;
    autil::mem_pool::Pool pool;
    ASSERT_FALSE(doc.getSerializedSourceDocument(config, &pool));

    std::shared_ptr<RawDocument::Snapshot> snapshot(rawDoc->GetSnapshot());
    doc.createSourceDocument(fieldGroups, snapshot.get());
    auto serDoc = doc.getSerializedSourceDocument(config, &pool);
    ASSERT_TRUE(serDoc);

    SourceFormatter formatter;
    indexlib::document::SourceDocument sourceDoc(nullptr);
    formatter.Init(config);
    ASSERT_TRUE(formatter.DeserializeSourceDocument(serDoc.get(), &sourceDoc).IsOK());
    ASSERT_EQ(autil::StringView("v5"), sourceDoc.GetField(0, "f5"));
    ASSERT_EQ(autil::StringView::empty_instance(), sourceDoc.GetField(0, "f4"));
    ASSERT_EQ(autil::StringView("v4"), sourceDoc.GetField(1, "f4"));
    ASSERT_TRUE(sourceDoc.HasField(1, "f9999"));
    ASSERT_TRUE(sourceDoc.IsNonExistField(1, "f9999"));
    ASSERT_FALSE(sourceDoc.HasField(0, "f9999"));
}
} // namespace indexlibv2::document
