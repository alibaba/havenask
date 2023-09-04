#include "autil/ConstString.h"
#include "indexlib/common_define.h"
#include "indexlib/config/source_schema.h"
#include "indexlib/document/extend_document/classified_document.h"
#include "indexlib/document/index_document/normal_document/source_document.h"
#include "indexlib/document/index_document/normal_document/source_document_formatter.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace indexlib::config;

namespace indexlib { namespace document {

class ClassifiedDocumentTest : public INDEXLIB_TESTBASE
{
public:
    ClassifiedDocumentTest();
    ~ClassifiedDocumentTest();

    DECLARE_CLASS_NAME(ClassifiedDocumentTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCreateSourceDocument();
    void TestGetSerialziedSourceDocument();
    void TestCreateEmptySourceDocument();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ClassifiedDocumentTest, TestCreateSourceDocument);
INDEXLIB_UNIT_TEST_CASE(ClassifiedDocumentTest, TestCreateEmptySourceDocument);
INDEXLIB_UNIT_TEST_CASE(ClassifiedDocumentTest, TestGetSerialziedSourceDocument);
IE_LOG_SETUP(document, ClassifiedDocumentTest);

ClassifiedDocumentTest::ClassifiedDocumentTest() {}

ClassifiedDocumentTest::~ClassifiedDocumentTest() {}

void ClassifiedDocumentTest::CaseSetUp() {}

void ClassifiedDocumentTest::CaseTearDown() {}

void ClassifiedDocumentTest::TestCreateSourceDocument()
{
    RawDocumentPtr rawDoc(new DefaultRawDocument());
    rawDoc->setField("f1", "v1");
    rawDoc->setField("f2", "v2");
    rawDoc->setField("f3", "v3");
    rawDoc->setField("f4", "v4");
    rawDoc->setField("f5", "v5");

    SourceSchema::FieldGroups grps;
    grps.resize(2);
    grps[0] = {"f5", "f2", "f3", "f1"};
    grps[1] = {"f2", "f9999", "f4"};

    ClassifiedDocument doc;
    doc.createSourceDocument(grps, rawDoc);

    SourceDocumentPtr srcDoc = doc._srcDocument;
    ASSERT_TRUE(srcDoc);
    ASSERT_EQ(StringView("v5"), srcDoc->GetField(0, "f5"));
    ASSERT_EQ(StringView::empty_instance(), srcDoc->GetField(0, "f4"));
    ASSERT_EQ(StringView("v4"), srcDoc->GetField(1, "f4"));
    ASSERT_TRUE(srcDoc->HasField(1, "f9999"));
    ASSERT_TRUE(srcDoc->IsNonExistField(1, "f9999"));
    ASSERT_FALSE(srcDoc->HasField(0, "f9999"));
}

void ClassifiedDocumentTest::TestCreateEmptySourceDocument()
{
    RawDocumentPtr rawDoc(new DefaultRawDocument());
    rawDoc->setField("f1", "v1");
    rawDoc->setField("f2", "v2");
    rawDoc->setField("f3", "v3");
    rawDoc->setField("f4", "v4");
    rawDoc->setField("f5", "v5");

    SourceSchema::FieldGroups grps {{"f9"}};
    ClassifiedDocument doc;
    doc.createSourceDocument(grps, rawDoc);

    SourceDocumentPtr srcDoc = doc._srcDocument;
    ASSERT_TRUE(srcDoc);
    ASSERT_EQ(1u, srcDoc->_data.size());
    ASSERT_EQ(1u, srcDoc->_data[0]->fieldsName.size());
    ASSERT_EQ(StringView::empty_instance(), srcDoc->GetField(0, "f9"));
    ASSERT_TRUE(srcDoc->HasField(0, "f9"));
    ASSERT_TRUE(srcDoc->IsNonExistField(0, "f9"));
}

void ClassifiedDocumentTest::TestGetSerialziedSourceDocument()
{
    string configStr = R"(
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
    SourceSchema::FieldGroups grps;
    grps.resize(2);
    grps[0] = {"f5", "f2", "f3", "f1"};
    grps[1] = {"f2", "f9999", "f4"};

    SourceSchemaPtr sourceSchema(new SourceSchema);
    Any any = ParseJson(configStr);
    FromJson(*(sourceSchema.get()), any);
    ASSERT_NO_THROW(sourceSchema->Check());

    RawDocumentPtr rawDoc(new DefaultRawDocument());
    rawDoc->setField("f1", "v1");
    rawDoc->setField("f2", "v2");
    rawDoc->setField("f3", "v3");
    rawDoc->setField("f4", "v4");
    rawDoc->setField("f5", "v5");

    mem_pool::Pool* pool = new mem_pool::Pool(1024);
    ClassifiedDocument doc;
    ASSERT_FALSE(createSerializedSourceDocument(doc.getSourceDocument(), sourceSchema, pool));
    doc.createSourceDocument(grps, rawDoc);
    ASSERT_FALSE(createSerializedSourceDocument(doc.getSourceDocument(), sourceSchema, NULL));

    SerializedSourceDocumentPtr serDoc = createSerializedSourceDocument(doc.getSourceDocument(), sourceSchema, pool);
    doc.clear();
    ASSERT_TRUE(serDoc);
    SourceDocumentFormatterPtr formatter(new SourceDocumentFormatter());
    SourceDocument* srcDoc = new SourceDocument(pool);
    formatter->Init(sourceSchema);
    formatter->DeserializeSourceDocument(serDoc, srcDoc);
    ASSERT_EQ(StringView("v5"), srcDoc->GetField(0, "f5"));
    ASSERT_EQ(StringView::empty_instance(), srcDoc->GetField(0, "f4"));
    ASSERT_EQ(StringView("v4"), srcDoc->GetField(1, "f4"));
    ASSERT_TRUE(srcDoc->HasField(1, "f9999"));
    ASSERT_TRUE(srcDoc->IsNonExistField(1, "f9999"));
    ASSERT_FALSE(srcDoc->HasField(0, "f9999"));
    delete srcDoc;
    delete pool;
}
}} // namespace indexlib::document
