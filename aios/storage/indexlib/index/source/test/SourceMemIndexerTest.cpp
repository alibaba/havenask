#include "indexlib/index/source/SourceMemIndexer.h"

#include "autil/Thread.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/config/MutableJson.h"
#include "indexlib/document/extractor/plain/DocumentInfoExtractorFactory.h"
#include "indexlib/document/normal/ClassifiedDocument.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/document/normal/SerializedSourceDocument.h"
#include "indexlib/document/normal/SourceDocument.h"
#include "indexlib/document/normal/SourceFormatter.h"
#include "indexlib/document/normal/SummaryFormatter.h"
#include "indexlib/document/raw_document/DefaultRawDocument.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/index/MemIndexerParameter.h"
#include "indexlib/index/source/config/SourceIndexConfig.h"
#include "indexlib/util/SimplePool.h"
#include "unittest/unittest.h"
using autil::StringView;
using indexlibv2::document::SourceFormatter;
namespace indexlibv2::index {

class SourceMemIndexerTest : public TESTBASE
{
public:
    SourceMemIndexerTest() = default;
    ~SourceMemIndexerTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    void WriteFunc(std::shared_ptr<SourceMemIndexer>& writer, autil::mem_pool::Pool* pool);
    void ReadFunc(std::shared_ptr<SourceMemIndexer>& writer);

private:
    static const size_t OP_COUNT = 10000;
    std::shared_ptr<config::SourceIndexConfig> _sourceSchema;
    std::vector<std::vector<std::string>> _grps;
};

void SourceMemIndexerTest::setUp()
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
    _sourceSchema.reset(new config::SourceIndexConfig);
    auto any = autil::legacy::json::ParseJson(configStr);
    config::MutableJson mtJson;
    config::IndexConfigDeserializeResource resource({}, mtJson);
    _sourceSchema->Deserialize(any, 0, resource);
}

void SourceMemIndexerTest::tearDown() {}

TEST_F(SourceMemIndexerTest, testSimpleProcess)
{
    std::shared_ptr<SourceMemIndexer> writer(new SourceMemIndexer(index::MemIndexerParameter {}));
    plain::DocumentInfoExtractorFactory factory;
    ASSERT_TRUE(writer->Init(_sourceSchema, &factory).IsOK());
    autil::mem_pool::Pool* pool = new autil::mem_pool::Pool(1024);
    {
        std::shared_ptr<document::RawDocument> rawDoc(new document::DefaultRawDocument());
        rawDoc->setField("f1", "v1");
        rawDoc->setField("f2", "v2");
        rawDoc->setField("f5", "v5");
        rawDoc->setField("nid", "1");
        document::ClassifiedDocument doc;
        std::shared_ptr<document::RawDocument::Snapshot> snapshot(rawDoc->GetSnapshot());
        doc.createSourceDocument(_grps, snapshot.get());
        std::shared_ptr<indexlib::document::SerializedSourceDocument> serDoc =
            doc.getSerializedSourceDocument(_sourceSchema, pool);

        std::shared_ptr<indexlibv2::document::NormalDocument> normalDoc(new indexlibv2::document::NormalDocument);
        normalDoc->SetSourceDocument(serDoc);
        ASSERT_TRUE(writer->AddDocument(normalDoc.get()).IsOK());
    }
    {
        std::shared_ptr<document::RawDocument> rawDoc(new document::DefaultRawDocument());
        rawDoc->setField("f1", "value1");
        rawDoc->setField("f3", "value3");
        rawDoc->setField("f5", "value5");
        rawDoc->setField("nid", "2");
        document::ClassifiedDocument doc;
        std::shared_ptr<document::RawDocument::Snapshot> snapshot(rawDoc->GetSnapshot());
        doc.createSourceDocument(_grps, snapshot.get());
        std::shared_ptr<indexlib::document::SerializedSourceDocument> serDoc =
            doc.getSerializedSourceDocument(_sourceSchema, pool);
        std::shared_ptr<indexlibv2::document::NormalDocument> normalDoc(new indexlibv2::document::NormalDocument);
        normalDoc->SetSourceDocument(serDoc);
        ASSERT_TRUE(writer->AddDocument(normalDoc.get()).IsOK());
    }

    indexlib::file_system::FileSystemOptions options;

    auto fs = indexlib::file_system::FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(), options).GetOrThrow();
    auto directory = indexlib::file_system::Directory::Get(fs);

    indexlib::util::SimplePool dumpPool;
    // test in memory reader
    auto reader = writer;
    {
        indexlib::document::SerializedSourceDocument serSourceDoc;
        SourceFormatter formatter;
        formatter.Init(_sourceSchema);
        indexlib::document::SourceDocument sourceDoc(NULL);
        reader->GetDocument(0, {0, 1, 2}, &serSourceDoc);
        ASSERT_TRUE(formatter.DeserializeSourceDocument(&serSourceDoc, &sourceDoc).IsOK());
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
        indexlib::document::SerializedSourceDocument serSourceDoc;
        SourceFormatter formatter;
        formatter.Init(_sourceSchema);
        indexlib::document::SourceDocument sourceDoc(NULL);
        reader->GetDocument(0, {1}, &serSourceDoc);
        ASSERT_TRUE(formatter.DeserializeSourceDocument(&serSourceDoc, &sourceDoc).IsOK());
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(0, "f1"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(0, "f2"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(0, "f3"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(0, "f4"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(0, "f5"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(0, "nid"));

        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(1, "f1"));
        ASSERT_EQ(StringView("v2"), sourceDoc.GetField(1, "f2"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(1, "f3"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(1, "f4"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(1, "f5"));
        ASSERT_EQ(StringView("1"), sourceDoc.GetField(1, "nid"));

        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(2, "f1"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(2, "f2"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(2, "f3"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(2, "f4"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(2, "f5"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(2, "nid"));
    }

    {
        indexlib::document::SerializedSourceDocument serSourceDoc;
        SourceFormatter formatter;
        formatter.Init(_sourceSchema);
        indexlib::document::SourceDocument sourceDoc(NULL);
        reader->GetDocument(1, {0, 1, 2}, &serSourceDoc);
        ASSERT_TRUE(formatter.DeserializeSourceDocument(&serSourceDoc, &sourceDoc).IsOK());
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
        indexlib::document::SerializedSourceDocument serSourceDoc1, serSourceDoc2;
        SourceFormatter formatter1, formatter2;
        formatter1.Init(_sourceSchema);
        formatter2.Init(_sourceSchema);
        std::shared_ptr<indexlib::document::SourceDocument> sourceDoc(new indexlib::document::SourceDocument(pool));
        reader->GetDocument(0, {0, 1, 2}, &serSourceDoc1);
        ASSERT_TRUE(formatter1.DeserializeSourceDocument(&serSourceDoc1, sourceDoc.get()).IsOK());
        std::shared_ptr<indexlib::document::SourceDocument> updateDoc(new indexlib::document::SourceDocument(pool));
        reader->GetDocument(1, {0, 1, 2}, &serSourceDoc2);
        ASSERT_TRUE(formatter2.DeserializeSourceDocument(&serSourceDoc2, updateDoc.get()).IsOK());
        updateDoc->AppendNonExistField(2, "f2");
        ASSERT_TRUE(sourceDoc->Merge(updateDoc));
        document::ClassifiedDocument doc;
        doc.setSourceDocument(sourceDoc);
        std::shared_ptr<indexlib::document::SerializedSourceDocument> serDoc =
            doc.getSerializedSourceDocument(_sourceSchema, pool);
        std::shared_ptr<indexlibv2::document::NormalDocument> normalDoc(new indexlibv2::document::NormalDocument);
        normalDoc->SetSourceDocument(serDoc);
        ASSERT_TRUE(writer->AddDocument(normalDoc.get()).IsOK());
    }

    {
        auto reader = writer;
        indexlib::document::SerializedSourceDocument serSourceDoc;
        SourceFormatter formatter;
        formatter.Init(_sourceSchema);
        indexlib::document::SourceDocument sourceDoc(NULL);
        reader->GetDocument(2, {0, 1, 2}, &serSourceDoc);
        ASSERT_TRUE(formatter.DeserializeSourceDocument(&serSourceDoc, &sourceDoc).IsOK());
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

    ASSERT_TRUE(writer->Dump(&dumpPool, directory, nullptr).IsOK());
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

void SourceMemIndexerTest::WriteFunc(std::shared_ptr<SourceMemIndexer>& writer, autil::mem_pool::Pool* pool)
{
    size_t times = OP_COUNT;
    // docid_t docId = 0;
    while (times--) {
        std::shared_ptr<document::RawDocument> rawDoc(new document::DefaultRawDocument());
        rawDoc->setField("f1", "v1");
        std::string v2 = "v2";
        for (size_t i = 0; i < 5; ++i) {
            v2 += v2;
        }
        rawDoc->setField("f2", "v2");
        rawDoc->setField("f5", "v5");
        rawDoc->setField("nid", "1");
        // std::cout << "write " << docId++ << std::endl;
        document::ClassifiedDocument doc;
        std::shared_ptr<document::RawDocument::Snapshot> snapshot(rawDoc->GetSnapshot());
        doc.createSourceDocument(_grps, snapshot.get());

        std::shared_ptr<indexlib::document::SerializedSourceDocument> serDoc =
            doc.getSerializedSourceDocument(_sourceSchema, pool);
        std::shared_ptr<indexlibv2::document::NormalDocument> normalDoc(new indexlibv2::document::NormalDocument);
        normalDoc->SetSourceDocument(serDoc);
        ASSERT_TRUE(writer->AddDocument(normalDoc.get()).IsOK());
    }
}

void SourceMemIndexerTest::ReadFunc(std::shared_ptr<SourceMemIndexer>& writer)
{
    size_t times = OP_COUNT;
    while (times--) {
        indexlib::document::SourceDocument sourceDoc(NULL);
        docid_t docId = rand() % OP_COUNT;
        auto reader = writer;
        indexlib::document::SerializedSourceDocument serSourceDoc;
        SourceFormatter formatter;
        formatter.Init(_sourceSchema);
        if (!reader->GetDocument(docId, {0, 1, 2}, &serSourceDoc)) {
            // std::cout << docId << " failed " << std::endl;
            continue;
        }
        ASSERT_TRUE(formatter.DeserializeSourceDocument(&serSourceDoc, &sourceDoc).IsOK());
        ASSERT_EQ(StringView("v1"), sourceDoc.GetField(0, "f1"));
        ASSERT_EQ(StringView("v2"), sourceDoc.GetField(0, "f2"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(0, "f3"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(0, "f4"));
        ASSERT_EQ(StringView("v5"), sourceDoc.GetField(0, "f5"));
        ASSERT_EQ(StringView::empty_instance(), sourceDoc.GetField(0, "nid"));
    }
}

TEST_F(SourceMemIndexerTest, TestConcurrentReadWrite)
{
    // expect no exception throw
    std::shared_ptr<SourceMemIndexer> writer(new SourceMemIndexer(index::MemIndexerParameter {}));
    plain::DocumentInfoExtractorFactory factory;
    ASSERT_TRUE(writer->Init(_sourceSchema, &factory).IsOK());
    autil::mem_pool::Pool pool(1024);
    autil::ThreadPtr writeThread =
        autil::Thread::createThread(std::bind(&SourceMemIndexerTest::WriteFunc, this, writer, &pool));
    autil::ThreadPtr readThread = autil::Thread::createThread(std::bind(&SourceMemIndexerTest::ReadFunc, this, writer));
    writeThread->join();
    readThread->join();
}

} // namespace indexlibv2::index
