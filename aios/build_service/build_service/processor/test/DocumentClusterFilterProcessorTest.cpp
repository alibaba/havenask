#include "build_service/test/unittest.h"
#include "build_service/processor/DocumentClusterFilterProcessor.h"
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>
#include "build_service/document/ExtendDocument.h"
#include <indexlib/config/index_partition_schema_maker.h>
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;
using namespace testing;
using namespace autil;
using namespace build_service::document;
IE_NAMESPACE_USE(config);

namespace build_service {
namespace processor {

namespace
{

ExtendDocumentPtr createDocument(const string& distFieldName, const string &distFieldValue)
{
    RawDocumentPtr rawDoc(new IE_NAMESPACE(document)::DefaultRawDocument());
    rawDoc->setDocOperateType(ADD_DOC);
    ExtendDocumentPtr document(new ExtendDocument);
    rawDoc->setField(distFieldName, distFieldValue);
    document->setRawDocument(rawDoc);
    return document;
}

}

class DocumentClusterFilterProcessorTest: public BUILD_SERVICE_TESTBASE {
public:
    DocumentClusterFilterProcessorTest() {}
    ~DocumentClusterFilterProcessorTest() {}
public:
    void setUp() {}
    void tearDown() {}
};


TEST_F(DocumentClusterFilterProcessorTest, testSimple) {
    // >, <, >=, <=, ==, !=, in, notin
    // gt, lt, ge...., in, notin, range
    {
        KeyValueMap kvMap;
        kvMap[DocumentClusterFilterProcessor::FILTER_RULE] = "test1 > 3";
        DocumentClusterFilterProcessor processor;
        DocProcessorInitParam param;
        param.parameters = kvMap;
        processor.init(param);

        auto doc = createDocument("test1", "10");
        EXPECT_TRUE(processor.process(doc));
        EXPECT_EQ(SKIP_DOC, doc->getRawDocument()->getDocOperateType());
        doc = createDocument("test1", "3");
        EXPECT_TRUE(processor.process(doc));
        EXPECT_EQ(ADD_DOC, doc->getRawDocument()->getDocOperateType());
        doc = createDocument("test1", "3xxx");
        EXPECT_FALSE(processor.process(doc));
        doc = createDocument("test1", "5.0");
        EXPECT_FALSE(processor.process(doc));

        doc = createDocument("test", "3");
        EXPECT_TRUE(processor.process(doc));
        EXPECT_EQ(ADD_DOC, doc->getRawDocument()->getDocOperateType());
    }
    {
        KeyValueMap kvMap;
        kvMap[DocumentClusterFilterProcessor::FILTER_RULE] = "test1 != 3";
        DocumentClusterFilterProcessor processor;
        DocProcessorInitParam param;
        param.parameters = kvMap;
        processor.init(param);

        auto doc = createDocument("test1", "3");
        EXPECT_TRUE(processor.process(doc));
        EXPECT_EQ(ADD_DOC, doc->getRawDocument()->getDocOperateType());

        doc = createDocument("test1", "4");
        EXPECT_TRUE(processor.process(doc));
        EXPECT_EQ(SKIP_DOC, doc->getRawDocument()->getDocOperateType());
    }

    {
        KeyValueMap kvMap;
        kvMap[DocumentClusterFilterProcessor::FILTER_RULE] = "test1 != 3.0";
        DocumentClusterFilterProcessor processor;
        DocProcessorInitParam param;
        param.parameters = kvMap;
        EXPECT_FALSE(processor.init(param));

        kvMap[DocumentClusterFilterProcessor::FILTER_RULE] = "test1!= 2";
        param.parameters = kvMap;
        EXPECT_FALSE(processor.init(param));

        kvMap[DocumentClusterFilterProcessor::FILTER_RULE] = "test1 && 2";
        param.parameters = kvMap;
        EXPECT_FALSE(processor.init(param));

    }
}

TEST_F(DocumentClusterFilterProcessorTest, testDifferTypeDoc)
{
    KeyValueMap kvMap;
    kvMap[DocumentClusterFilterProcessor::FILTER_RULE] = "test1 > 3";
    DocumentClusterFilterProcessor processor;
    DocProcessorInitParam param;
    param.parameters = kvMap;
    processor.init(param);
    auto doc = createDocument("test1", "10");
    doc->getRawDocument()->setDocOperateType(DELETE_DOC);
    EXPECT_TRUE(processor.process(doc));
    EXPECT_EQ(SKIP_DOC, doc->getRawDocument()->getDocOperateType());

    doc = createDocument("test1", "3");
    doc->getRawDocument()->setDocOperateType(DELETE_DOC);
    EXPECT_TRUE(processor.process(doc));
    EXPECT_EQ(DELETE_DOC, doc->getRawDocument()->getDocOperateType());
}


}
}
