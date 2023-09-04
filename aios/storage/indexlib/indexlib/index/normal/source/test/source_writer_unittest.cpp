#include <random>

#include "autil/Thread.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/document/extend_document/classified_document.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/index/normal/source/source_writer.h"
#include "indexlib/index/normal/source/source_writer_impl.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/SimplePool.h"

using autil::StringView;
using autil::legacy::Any;
using std::string;
using std::vector;

using indexlib::config::SourceGroupConfigPtr;
using indexlib::config::SourceSchema;
using indexlib::config::SourceSchemaPtr;
using indexlib::document::SerializedSourceDocumentPtr;
using indexlib::util::BuildResourceMetrics;

namespace indexlib { namespace index {

class SourceWriterTest : public INDEXLIB_TESTBASE
{
public:
    SourceWriterTest();
    ~SourceWriterTest();
    static const size_t OP_COUNT = 10000;
    DECLARE_CLASS_NAME(SourceWriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestConcurrentReadWrite();

private:
    void ReadFunc(SourceWriterPtr& writer);
    void WriteFunc(SourceWriterPtr& writer, autil::mem_pool::Pool* pool);

private:
    SourceSchemaPtr _sourceSchema;
    SourceSchema::FieldGroups _grps;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SourceWriterTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SourceWriterTest, TestConcurrentReadWrite);
IE_LOG_SETUP(index, SourceWriterTest);

SourceWriterTest::SourceWriterTest() {}

SourceWriterTest::~SourceWriterTest() {}

void SourceWriterTest::CaseSetUp()
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
                "fields": ["f2", "nid", "f4"]
            },
            {
                "field_mode": "specified_field",
                "fields": ["f1", "f2"],
                "parameter" : {
                    "compress_type": "uniq|equal",
                    "doc_compressor": "zlib",
                    "file_compressor" : "snappy",
                    "file_compress_buffer_size" : 8192
                }
            }
        ]
    })";
    _grps = {{"f5", "f2", "f3", "f1"}, {"f2", "nid", "f4"}, {"f1", "f2"}};
    _sourceSchema.reset(new SourceSchema);
    Any any = autil::legacy::json::ParseJson(configStr);
    autil::legacy::FromJson(*(_sourceSchema.get()), any);
}

void SourceWriterTest::CaseTearDown() {}

void SourceWriterTest::TestSimpleProcess()
{
    SourceWriterPtr writer(new SourceWriterImpl);
    BuildResourceMetrics buildResourceMetrics;
    buildResourceMetrics.Init();

    writer->Init(_sourceSchema, &buildResourceMetrics);
    autil::mem_pool::Pool* pool = new autil::mem_pool::Pool(1024);
    {
        document::RawDocumentPtr rawDoc(new document::DefaultRawDocument());
        rawDoc->setField("f1", "v1");
        rawDoc->setField("f2", "v2");
        rawDoc->setField("f5", "v5");
        rawDoc->setField("nid", "1");
        document::ClassifiedDocument doc;
        doc.createSourceDocument(_grps, rawDoc);
        document::SerializedSourceDocumentPtr serDoc =
            createSerializedSourceDocument(doc.getSourceDocument(), _sourceSchema, pool);
        writer->AddDocument(serDoc);
    }
    {
        document::RawDocumentPtr rawDoc(new document::DefaultRawDocument());
        rawDoc->setField("f1", "value1");
        rawDoc->setField("f3", "value3");
        rawDoc->setField("f5", "value5");
        rawDoc->setField("nid", "2");
        document::ClassifiedDocument doc;
        doc.createSourceDocument(_grps, rawDoc);
        document::SerializedSourceDocumentPtr serDoc =
            createSerializedSourceDocument(doc.getSourceDocument(), _sourceSchema, pool);
        writer->AddDocument(serDoc);
    }

    file_system::DirectoryPtr directory = GET_PARTITION_DIRECTORY();
    util::SimplePool dumpPool;
    // test in memory reader
    InMemSourceSegmentReaderPtr reader = writer->CreateInMemSegmentReader();
    {
        document::SourceDocument sourceDoc(NULL);
        reader->GetDocument(0, &sourceDoc);
        ASSERT_EQ(StringView("v1"), sourceDoc.GetField(0, "f1"));
        ASSERT_EQ(StringView("v2"), sourceDoc.GetField(0, "f2"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(0, "f3"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(0, "f4"));
        ASSERT_EQ(StringView("v5"), sourceDoc.GetField(0, "f5"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(0, "nid"));

        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(1, "f1"));
        ASSERT_EQ(StringView("v2"), sourceDoc.GetField(1, "f2"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(1, "f3"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(1, "f4"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(1, "f5"));
        ASSERT_EQ(StringView("1"), sourceDoc.GetField(1, "nid"));

        ASSERT_EQ(StringView("v1"), sourceDoc.GetField(2, "f1"));
        ASSERT_EQ(StringView("v2"), sourceDoc.GetField(2, "f2"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(2, "f3"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(2, "f4"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(2, "f5"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(2, "nid"));
    }

    {
        document::SourceDocument sourceDoc(NULL);
        reader->GetDocument(1, &sourceDoc);
        ASSERT_EQ(StringView("value1"), sourceDoc.GetField(0, "f1"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(0, "f2"));
        ASSERT_EQ(StringView("value3"), sourceDoc.GetField(0, "f3"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(0, "f4"));
        ASSERT_EQ(StringView("value5"), sourceDoc.GetField(0, "f5"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(0, "nid"));

        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(1, "f1"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(1, "f2"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(1, "f3"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(1, "f4"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(1, "f5"));
        ASSERT_EQ(StringView("2"), sourceDoc.GetField(1, "nid"));

        ASSERT_EQ(StringView("value1"), sourceDoc.GetField(2, "f1"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(2, "f2"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(2, "f3"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(2, "f4"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(2, "f5"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(2, "nid"));
    }

    {
        // test merge
        document::SourceDocumentPtr sourceDoc(new document::SourceDocument(pool));
        reader->GetDocument(0, sourceDoc.get());
        document::SourceDocumentPtr updateDoc(new document::SourceDocument(pool));
        reader->GetDocument(1, updateDoc.get());
        updateDoc->AppendNonExistField(2, "f2");
        ASSERT_TRUE(sourceDoc->Merge(updateDoc));
        document::ClassifiedDocument doc;
        doc.setSourceDocument(sourceDoc);
        document::SerializedSourceDocumentPtr serDoc =
            createSerializedSourceDocument(doc.getSourceDocument(), _sourceSchema, pool);
        writer->AddDocument(serDoc);
    }

    {
        InMemSourceSegmentReaderPtr reader = writer->CreateInMemSegmentReader();
        document::SourceDocument sourceDoc(NULL);
        reader->GetDocument(2, &sourceDoc);
        ASSERT_EQ(StringView("value1"), sourceDoc.GetField(0, "f1"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(0, "f2"));
        ASSERT_EQ(StringView("value3"), sourceDoc.GetField(0, "f3"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(0, "f4"));
        ASSERT_EQ(StringView("value5"), sourceDoc.GetField(0, "f5"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(0, "nid"));

        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(1, "f1"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(1, "f2"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(1, "f3"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(1, "f4"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(1, "f5"));
        ASSERT_EQ(StringView("2"), sourceDoc.GetField(1, "nid"));

        ASSERT_EQ(StringView("value1"), sourceDoc.GetField(2, "f1"));
        ASSERT_EQ(StringView("v2"), sourceDoc.GetField(2, "f2"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(2, "f3"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(2, "f4"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(2, "f5"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(2, "nid"));
    }

    writer->Dump(directory, &dumpPool);
    ASSERT_TRUE(directory->IsExist("group_0/data"));
    ASSERT_TRUE(directory->IsExist("group_0/offset"));
    ASSERT_TRUE(directory->IsExist("group_1/data"));
    ASSERT_TRUE(directory->IsExist("group_1/offset"));
    ASSERT_TRUE(directory->IsExist("group_2/data"));
    ASSERT_TRUE(directory->IsExist("group_2/offset"));
    ASSERT_TRUE(directory->IsExist("meta/data"));
    ASSERT_TRUE(directory->IsExist("meta/offset"));
    delete pool;
}

void SourceWriterTest::ReadFunc(SourceWriterPtr& writer)
{
    size_t times = OP_COUNT;
    while (times--) {
        document::SourceDocument sourceDoc(NULL);
        docid_t docId = rand() % OP_COUNT;
        InMemSourceSegmentReaderPtr reader = writer->CreateInMemSegmentReader();
        if (!reader->GetDocument(docId, &sourceDoc)) {
            // std::cout << docId << " failed " << std::endl;
            continue;
        }
        ASSERT_EQ(StringView("v1"), sourceDoc.GetField(0, "f1"));
        ASSERT_EQ(StringView("v2"), sourceDoc.GetField(0, "f2"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(0, "f3"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(0, "f4"));
        ASSERT_EQ(StringView("v5"), sourceDoc.GetField(0, "f5"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(0, "nid"));
    }
}

void SourceWriterTest::WriteFunc(SourceWriterPtr& writer, autil::mem_pool::Pool* pool)
{
    size_t times = OP_COUNT;
    // docid_t docId = 0;
    while (times--) {
        document::RawDocumentPtr rawDoc(new document::DefaultRawDocument());
        rawDoc->setField("f1", "v1");
        string v2 = "v2";
        for (size_t i = 0; i < 5; ++i) {
            v2 += v2;
        }
        rawDoc->setField("f2", "v2");
        rawDoc->setField("f5", "v5");
        rawDoc->setField("nid", "1");
        // std::cout << "write " << docId++ << std::endl;
        document::ClassifiedDocument doc;
        doc.createSourceDocument(_grps, rawDoc);
        document::SerializedSourceDocumentPtr serDoc =
            createSerializedSourceDocument(doc.getSourceDocument(), _sourceSchema, pool);
        writer->AddDocument(serDoc);
    }
}

void SourceWriterTest::TestConcurrentReadWrite()
{
    // expect no exception throw
    SourceWriterPtr writer(new SourceWriterImpl);
    BuildResourceMetrics buildResourceMetrics;
    buildResourceMetrics.Init();

    writer->Init(_sourceSchema, &buildResourceMetrics);
    autil::mem_pool::Pool pool(1024);
    autil::ThreadPtr writeThread =
        autil::Thread::createThread(std::bind(&SourceWriterTest::WriteFunc, this, writer, &pool));
    autil::ThreadPtr readThread = autil::Thread::createThread(std::bind(&SourceWriterTest::ReadFunc, this, writer));
    writeThread->join();
    readThread->join();
}
}} // namespace indexlib::index
