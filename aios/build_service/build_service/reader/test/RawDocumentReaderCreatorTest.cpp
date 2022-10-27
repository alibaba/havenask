#include "build_service/test/unittest.h"
#include "build_service/reader/RawDocumentReaderCreator.h"
#include "build_service/reader/SwiftRawDocumentReader.h"
#include "build_service/reader/DocumentSeparators.h"
#include "build_service/reader/test/CustomRawDocumentReader.h"
#include "build_service/reader/FileRawDocumentReader.h"
#include "build_service/reader/BinaryFileRawDocumentReader.h"
#include "build_service/reader/MultiFileDocumentReader.h"
#include "build_service/reader/FormatFileDocumentReader.h"
#include "build_service/reader/FixLenBinaryFileDocumentReader.h"
#include "build_service/reader/VarLenBinaryFileDocumentReader.h"
#include "build_service/reader/test/CustomRawDocumentReaderFactory.h"
#include "build_service/reader/FileListCollector.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/proto/test/ProtoCreator.h"
#include <autil/StringUtil.h>
#include <autil/ThreadPool.h>
#include <swift/protocol/ErrCode.pb.h>

using namespace std;
using namespace build_service::config;
using namespace build_service::util;
using namespace build_service::document;
using namespace autil;
using namespace build_service::proto;

SWIFT_USE_NAMESPACE(protocol);
SWIFT_USE_NAMESPACE(client);

namespace build_service {
namespace reader {

class RawDocumentReaderCreatorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    bool prepareTopicData(SwiftClient* swiftClient,
                          const string& topicNamePrefix,
                          size_t testReaderCount,
                          size_t msgCountPerTopic);

    void multiThreadTestReadFromSwift(
        const SwiftClientCreatorPtr& swiftClientCreator,
        const string& zkRoot,
        const string& topicNamePrefix,
        size_t testReaderCount,
        size_t msgCountPerTopic);

    void deleteTopics(SwiftClient* swiftClient,
                      const string& topicNamePrefix, size_t testReaderCount);

    void prepareConfig(const string& toReplaceIndexPath);

    void replaceFile(const string& filePath, const string& strToReplace,
                     const string& replaceStr);

private:
    string _configPath;
};

class TestWorkItem : public autil::WorkItem {
public:
    TestWorkItem(
        const RawDocumentReaderCreatorPtr& readerCreator,
        const string& zkRoot,
        const string& topicNamePrefix,
        size_t readerId,
        size_t msgCountPerTopic,
        int* output)
        : _readerCreator(readerCreator)
        , _zkRoot(zkRoot)
        , _readerId(readerId)
        , _msgCount(msgCountPerTopic)
        , _output(output)
    {
        _topicName = topicNamePrefix + StringUtil::toString(readerId);
        _output[readerId] = 0;
    }

    ~TestWorkItem() {}

public:
    /* override */ void process() {
        ResourceReaderPtr resourceReader;
        KeyValueMap kvMap;
        kvMap[RAW_DOCUMENT_FORMAT] = "self_explain";
        kvMap[DOC_STRING_FIELD_NAME] = "docStr";
        kvMap[READ_SRC_TYPE] = "swift";
        kvMap[SWIFT_ZOOKEEPER_ROOT] = _zkRoot;
        kvMap[SWIFT_TOPIC_NAME] = _topicName;
        proto::PartitionId partitionId;
        IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap());
        RawDocumentReader* docReader =
            _readerCreator->create(resourceReader, kvMap, partitionId, NULL, counterMap);

        if (!docReader) {
            DELETE_AND_SET_NULL(docReader);
            return;
        }

        SwiftRawDocumentReader* swiftDocReader = dynamic_cast<SwiftRawDocumentReader*>(docReader);
        ASSERT_TRUE(swiftDocReader);

        RawDocumentPtr rawDoc;
        int64_t offset;
        for (size_t i = 0; i < _msgCount; ++i) {
            docReader->read(rawDoc, offset);
            string expect = string("testing") + StringUtil::toString(i);
            if (expect == rawDoc->getField("docStr")) {
                _output[_readerId] += 1;
            }
        }
        DELETE_AND_SET_NULL(docReader);
    }

    /* override */ void destroy() {
        delete this;
    }
    /* override */ void drop() {
        destroy();
    }
private:
    RawDocumentReaderCreatorPtr _readerCreator;
    string _zkRoot;
    size_t _readerId;
    size_t _msgCount;
    string _topicName;
    int* _output;
};


void RawDocumentReaderCreatorTest::setUp() {
}

void RawDocumentReaderCreatorTest::tearDown() {
}

bool RawDocumentReaderCreatorTest::prepareTopicData(
    SwiftClient* swiftClient,
    const string& topicNamePrefix,
    size_t testReaderCount,
    size_t msgCountPerTopic)
{
    assert(swiftClient);
    SwiftAdminAdapterPtr adminAdapter = swiftClient->getAdminAdapter();
    if (!adminAdapter) {
        return false;
    }

    for (size_t i = 0; i < testReaderCount; ++i) {
        TopicCreationRequest tpCreateReq;
        string topicName = topicNamePrefix + StringUtil::toString(i);
        tpCreateReq.set_topicname(topicName);
        tpCreateReq.set_partitionmaxbuffersize(5120);
        tpCreateReq.set_partitioncount(1);
        tpCreateReq.set_deletetopicdata(true);

        adminAdapter->deleteTopic(topicName);
        ErrorCode ec = adminAdapter->createTopic(tpCreateReq);
        if (ec != ERROR_NONE) {
            return false;
        }

        if (!adminAdapter->waitTopicReady(topicName)) {
            return false;
        }

        swift::protocol::ErrorInfo errorInfo;
        string writerConfig = "topicName=" + topicName;
        SwiftWriter* swiftWriter = swiftClient->createWriter(writerConfig, &errorInfo);
        if (!swiftWriter || errorInfo.has_errcode()) {
            return false;
        }

        for (size_t j = 0; j < msgCountPerTopic; ++j) {
            MessageInfo messageInfo;
            messageInfo.hashStr = StringUtil::toString(j);
            messageInfo.data = string("testing") + StringUtil::toString(j);
            ec = swiftWriter->write(messageInfo);
            if (ec != ERROR_NONE) {
                DELETE_AND_SET_NULL(swiftWriter);
                return false;
            }
        }
        swiftWriter->waitFinished();
        DELETE_AND_SET_NULL(swiftWriter);
    }
    return true;
}

void RawDocumentReaderCreatorTest::multiThreadTestReadFromSwift(
    const SwiftClientCreatorPtr& swiftClientCreator,
    const string& zkRoot,
    const string& topicNamePrefix,
    size_t testReaderCount,
    size_t msgCountPerTopic)
{
    ThreadPool threadPool(testReaderCount, testReaderCount);
    RawDocumentReaderCreatorPtr readerCreator(
        new RawDocumentReaderCreator(swiftClientCreator));

    int* output = new int[testReaderCount];

    for (size_t i = 0; i < testReaderCount; ++i) {
        threadPool.pushWorkItem(
            new TestWorkItem(readerCreator, zkRoot, topicNamePrefix,
                             i, msgCountPerTopic, output));
    }

    threadPool.start();
    threadPool.stop(autil::ThreadPool::STOP_AFTER_QUEUE_EMPTY);

    for (size_t i = 0; i < testReaderCount; ++i) {
        EXPECT_EQ(msgCountPerTopic, output[i]);
    }

    delete [] output;
}

void RawDocumentReaderCreatorTest::deleteTopics(
    SwiftClient* swiftClient, const string& topicNamePrefix, size_t testReaderCount)
{
    assert(swiftClient);
    SwiftAdminAdapterPtr adminAdapter = swiftClient->getAdminAdapter();
    ASSERT_TRUE(adminAdapter);

    for (size_t i = 0; i < testReaderCount; ++i) {
        string topicName = topicNamePrefix + StringUtil::toString(i);
        adminAdapter->deleteTopic(topicName);
    }
}

void RawDocumentReaderCreatorTest::prepareConfig(const string& toReplaceIndexPath) {
    _configPath = GET_TEST_DATA_PATH() + "/config_prepare_data_source";
    FileUtil::removeIfExist(_configPath);
    string srcConfigPath = TEST_DATA_PATH"/config_prepare_data_source";
    FileUtil::atomicCopy(srcConfigPath, _configPath);
    RawDocumentReaderCreatorTest::replaceFile(_configPath + "/build_app.json", "hdfs://index/root/",
                toReplaceIndexPath);
}

void RawDocumentReaderCreatorTest::replaceFile(
    const string& filePath, const string& strToReplace,
    const string& replaceStr) {
    string content;
    FileUtil::readFile(filePath, content);
    auto pos = content.find(strToReplace);
    while (pos != string::npos) {
        content = content.replace(pos, strToReplace.size(), replaceStr);
        pos = content.find(strToReplace);
    }
    FileUtil::remove(filePath);
    FileUtil::writeFile(filePath, content);
}


TEST_F(RawDocumentReaderCreatorTest, testCreateSwiftReader) {
    // RawDocumentReaderCreator creator;
    // KeyValueMap kvMap;
    // ResourceReaderPtr resourceReader(new ResourceReader(
    //                 TEST_DATA_PATH"/rawdoc_reader_creator_test/swift", ""));
    // RawDocumentReader *reader = creator.create(kvMap, resourceReader);
    // SwiftRawDocumentReader *typedReader = ASSERT_CAST_AND_RETURN(
    //         SwiftRawDocumentReader,reader);
    // check swift url, partition, locator
}


TEST_F(RawDocumentReaderCreatorTest, testCreateFormatFileReader) {
    prepareConfig(GET_TEST_DATA_PATH());
    RawDocumentReaderCreator creator(SwiftClientCreatorPtr(new SwiftClientCreator));
    KeyValueMap kvMap;
    ResourceReaderPtr resourceReader(new ResourceReader(_configPath));
    proto::PartitionId partitionId =
        ProtoCreator::createPartitionId(ROLE_TASK, BUILD_STEP_INC, 0, 65535,
                                        "simple", 1, "cluster1", "", "", "");
    kvMap["src"] = "file";
    kvMap["type"] = "file";
    kvMap["document_format"] = "isearch";
    kvMap["data"] = TEST_DATA_PATH"/custom_rawdoc_reader_test";
    IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap());

    RawDocumentReader* reader =
        creator.create(resourceReader, kvMap, partitionId, NULL, counterMap);
    FileRawDocumentReader *typedReader = ASSERT_CAST_AND_RETURN(
            FileRawDocumentReader, reader);

    FormatFileDocumentReaderPtr docReader = DYNAMIC_POINTER_CAST(FormatFileDocumentReader,
            typedReader->_documentReader->_fileDocumentReaderPtr);
    ASSERT_TRUE(docReader.get());

    EXPECT_EQ(RAW_DOCUMENT_ISEARCH_SEP_PREFIX, docReader->_docPrefix._sep);
    EXPECT_EQ(RAW_DOCUMENT_ISEARCH_SEP_SUFFIX, docReader->_docSuffix._sep);
    delete reader;
}

TEST_F(RawDocumentReaderCreatorTest, testCreateFixLenBinaryFileReader) {
    RawDocumentReaderCreator creator(SwiftClientCreatorPtr(new SwiftClientCreator));
    KeyValueMap kvMap;
    ResourceReaderPtr resourceReader(new ResourceReader(TEST_DATA_PATH));
    proto::PartitionId partitionId;
    kvMap["src"] = "file";
    kvMap["type"] = "fix_length_binary_file";
    kvMap["document_format"] = "self_explain";
    kvMap["length"] = "16";
    kvMap["data"] = TEST_DATA_PATH"/custom_rawdoc_reader_test";
    IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap());
    RawDocumentReader *reader = creator.create(
            resourceReader, kvMap, partitionId, NULL, counterMap);
    BinaryFileRawDocumentReader *typedReader = ASSERT_CAST_AND_RETURN(
            BinaryFileRawDocumentReader, reader);

    FixLenBinaryFileDocumentReaderPtr docReader =
        DYNAMIC_POINTER_CAST(FixLenBinaryFileDocumentReader,
                             typedReader->_documentReader->_fileDocumentReaderPtr);
    ASSERT_TRUE(docReader.get());
    EXPECT_EQ((size_t)16, docReader->_fixLen);
    delete reader;
}

TEST_F(RawDocumentReaderCreatorTest, testCreateVarLenBinaryFileReader) {
    RawDocumentReaderCreator creator(SwiftClientCreatorPtr(new SwiftClientCreator));
    KeyValueMap kvMap;
    ResourceReaderPtr resourceReader(new ResourceReader(TEST_DATA_PATH));
    proto::PartitionId partitionId;
    kvMap["src"] = "file";
    kvMap["type"] = "var_length_binary_file";
    kvMap["document_format"] = "self_explain";
    kvMap["data"] = TEST_DATA_PATH"/custom_rawdoc_reader_test";
    IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap());
    RawDocumentReader *reader = creator.create(
            resourceReader, kvMap, partitionId, NULL, counterMap);
    BinaryFileRawDocumentReader *typedReader = ASSERT_CAST_AND_RETURN(
            BinaryFileRawDocumentReader, reader);

    VarLenBinaryFileDocumentReaderPtr docReader =
        DYNAMIC_POINTER_CAST(VarLenBinaryFileDocumentReader,
                             typedReader->_documentReader->_fileDocumentReaderPtr);
    ASSERT_TRUE(docReader.get());
    delete reader;
}


TEST_F(RawDocumentReaderCreatorTest, testCreateCustomReader) {
    RawDocumentReaderCreator creator(SwiftClientCreatorPtr(new SwiftClientCreator));
    KeyValueMap kvMap;
    ResourceReaderPtr resourceReader(new ResourceReader(
                    TEST_DATA_PATH"/custom_rawdoc_reader_test"));
    proto::PartitionId partitionId;
    kvMap["src"] = "src";
    kvMap["type"] = "plugin";
    kvMap["module_name"] = "CustomRawDocumentReader";
    kvMap["module_path"] = "libcustom_reader.so";
    IE_NAMESPACE(util)::CounterMapPtr counterMap(new IE_NAMESPACE(util)::CounterMap());
    RawDocumentReader *reader = creator.create(
        resourceReader, kvMap, partitionId, NULL, counterMap);
    CustomRawDocumentReader *typedReader = ASSERT_CAST_AND_RETURN(
            CustomRawDocumentReader, reader);
    (void)typedReader;
    delete reader;
}

TEST_F(RawDocumentReaderCreatorTest, testSwiftReaderShareSwiftClient) {
    // SwiftClientCreatorPtr swiftClientCreator(new SwiftClientCreator());
    // // prepare swift topic data
    // string zkRoot = "zfs://10.101.170.89:12181,10.101.170.90:12181,10.101.170.91:12181,10.101.170.92:12181,10.101.170.93:12181/swift/swift_service";
    // string topicNamePrefix = "bsDevTest";
    // string swiftClientConfig = "";
    // size_t testReaderCount = 3;
    // size_t msgCount = 3;
    // SwiftClient* swiftClient = swiftClientCreator->createSwiftClient(zkRoot, swiftClientConfig);
    // ASSERT_TRUE(swiftClient);

    // if (!prepareTopicData(swiftClient, topicNamePrefix, testReaderCount, msgCount)) {
    //     deleteTopics(swiftClient, topicNamePrefix, testReaderCount);
    //     ASSERT_TRUE(false) << "create topic failed." << endl;
    // }
    // multiThreadTestReadFromSwift(swiftClientCreator, zkRoot, topicNamePrefix, testReaderCount, msgCount);
    // deleteTopics(swiftClient, topicNamePrefix, testReaderCount);
}
}

}
