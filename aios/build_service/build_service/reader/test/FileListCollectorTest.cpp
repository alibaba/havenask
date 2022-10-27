#include "build_service/test/unittest.h"
#include "build_service/reader/FileListCollector.h"
#include "build_service/util/FileUtil.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/proto/BasicDefs.pb.h"
#include <autil/StringUtil.h>
#include <indexlib/storage/file_system_wrapper.h>

using namespace std;
using namespace autil;
using namespace fslib;
using namespace testing;
using namespace build_service::util;
using namespace build_service::config;

namespace build_service {
namespace reader {

class FileListCollectorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    string _rootDir;
    string _dataSource1;
    string _dataSource2;
    string _singleFilePath;
    vector<string> _dataSource1Files;
    vector<string> _dataSource2Files;
    vector<string> _allFiles;

    string _multiPath1;
    string _multiPath2;
    vector<string> _mutliPathMultiPattern;
};

MATCHER_P2(FileListMatcher, fileList, globalIds, "") {
    if (fileList.size() != arg.size()) {
        *result_listener << " CollectResults: " << arg.size() << " FileList: " << fileList.size();
    }
    for (size_t i = 0; i < fileList.size(); i++) {
        if (fileList[globalIds[i]] != arg[i]._fileName) {
            *result_listener << "id[" << i << "]" << " CollectResults: "
                             << arg[i]._fileName << " FileList: " << fileList[globalIds[i]];
            return false;
        }
        if (globalIds[i] != arg[i]._globalId) {
            *result_listener << "id[" << i << "]" << " expect global id: "
                             << globalIds[i] << " actual global id: " << arg[i]._globalId;
            return false;
        }
    }
    return true;
}

void FileListCollectorTest::setUp() {
    _rootDir = GET_TEST_DATA_PATH() + "/root";
    _dataSource1 = FileUtil::joinFilePath(_rootDir, "data1");
    _dataSource2 = FileUtil::joinFilePath(_rootDir, "data2");
    _singleFilePath = FileUtil::joinFilePath(_rootDir, "file");

    string multiPath = GET_TEST_DATA_PATH() + "/multi_path";
    _multiPath1 = FileUtil::joinFilePath(multiPath, "multi_path1");
    _multiPath2 = FileUtil::joinFilePath(multiPath, "multi_path2");

    _dataSource1Files.push_back(FileUtil::joinFilePath(_dataSource1, "0/a"));
    _dataSource1Files.push_back(FileUtil::joinFilePath(_dataSource1, "0/b"));
    _dataSource1Files.push_back(FileUtil::joinFilePath(_dataSource1, "0/c"));
    _dataSource2Files.push_back(FileUtil::joinFilePath(_dataSource2, "common-0.gz"));
    _dataSource2Files.push_back(FileUtil::joinFilePath(_dataSource2, "common-1.gz"));
    _dataSource2Files.push_back(FileUtil::joinFilePath(_dataSource2, "common-2.gz"));

    _mutliPathMultiPattern.push_back(FileUtil::joinFilePath(_multiPath1, "common-000.gz"));
    _mutliPathMultiPattern.push_back(FileUtil::joinFilePath(_multiPath1, "common-001.gz"));
    _mutliPathMultiPattern.push_back(FileUtil::joinFilePath(_multiPath1, "common-002.gz"));
    _mutliPathMultiPattern.push_back(FileUtil::joinFilePath(_multiPath1, "good-000.gz"));
    _mutliPathMultiPattern.push_back(FileUtil::joinFilePath(_multiPath1, "good-001.gz"));
    _mutliPathMultiPattern.push_back(FileUtil::joinFilePath(_multiPath1, "good-002.gz"));
    _mutliPathMultiPattern.push_back(FileUtil::joinFilePath(_multiPath2, "good-000.gz"));
    _mutliPathMultiPattern.push_back(FileUtil::joinFilePath(_multiPath2, "good-001.gz"));
    _mutliPathMultiPattern.push_back(FileUtil::joinFilePath(_multiPath2, "good-002.gz"));
    sort(_mutliPathMultiPattern.begin(), _mutliPathMultiPattern.end());
    ASSERT_TRUE(FileUtil::mkDir(multiPath, true));
    ASSERT_TRUE(FileUtil::mkDir(_multiPath1, true));
    ASSERT_TRUE(FileUtil::mkDir(_multiPath2, true));
    for (size_t i = 0; i < _mutliPathMultiPattern.size(); ++i) {
        FileUtil::writeFile(_mutliPathMultiPattern[i], "");
    }

    _allFiles.insert(_allFiles.end(), _dataSource1Files.begin(), _dataSource1Files.end());
    _allFiles.insert(_allFiles.end(), _dataSource2Files.begin(), _dataSource2Files.end());

    _allFiles.push_back(_singleFilePath);
    sort(_allFiles.begin(), _allFiles.end());

    FileUtil::mkDir(FileUtil::joinFilePath(_dataSource1, "0"), true);
    FileUtil::mkDir(_dataSource2, true);

    for (size_t i = 0; i < _allFiles.size(); i++) {
        FileUtil::writeFile(_allFiles[i], "");
    }
}

void FileListCollectorTest::tearDown() {
}

TEST_F(FileListCollectorTest, testGetAllCandidateFiles) {
    FileList result;
    ASSERT_TRUE(FileListCollector::getAllCandidateFiles(_rootDir, result));
    sort(result.begin(), result.end());
    EXPECT_EQ(_allFiles, result);
}

TEST_F(FileListCollectorTest, testCollectWithoutPattern) {
    {
        // only one root path
        CollectorDescription input;
        input.paths.push_back(_rootDir);
        input.rangeFrom = 0;
        input.rangeTo = 65535;
        CollectResults fileList;
        ASSERT_TRUE(FileListCollector::collectAllResults(input, fileList));
        uint32_t globalIds[7] = { 0, 1, 2, 3, 4, 5, 6 };
        EXPECT_THAT(fileList, FileListMatcher(_allFiles, globalIds));
    }
    {
        // dirs with single file
        CollectorDescription input;
        input.paths.push_back(_dataSource1);
        input.paths.push_back(_dataSource2);
        input.paths.push_back(_singleFilePath);
        input.rangeFrom = 0;
        input.rangeTo = 65535;
        CollectResults fileList;
        ASSERT_TRUE(FileListCollector::collectAllResults(input, fileList));
        uint32_t globalIds[7] = { 0, 1, 2, 3, 4, 5, 6 };
        EXPECT_THAT(fileList, FileListMatcher(_allFiles, globalIds));
    }
    {
        // dirs with 3 partitions
        CollectorDescription input;
        input.paths.push_back(_dataSource1);
        input.paths.push_back(_dataSource2);
        input.paths.push_back(_singleFilePath);
        input.rangeFrom = 0;
        input.rangeTo = 21845;
        CollectResults fileListAll;
        ASSERT_TRUE(FileListCollector::collectAllResults(input, fileListAll));
        CollectResults fileList;
        uint16_t from = 0, to = 21845;
        for (size_t i = 0; i < fileListAll.size(); i++) {
            uint16_t hashId = fileListAll[i]._rangeId;
            if (from <= hashId && hashId <= to) {
                fileList.push_back(fileListAll[i]);
            }
        }
        EXPECT_EQ(_dataSource1Files[0], fileList[0]._fileName);
        EXPECT_EQ(uint32_t(0), fileList[0]._globalId);

        EXPECT_EQ(_dataSource2Files[0], fileList[1]._fileName);
        EXPECT_EQ(uint32_t(3), fileList[1]._globalId);
    }
}

TEST_F(FileListCollectorTest, testCollectWithPattern) {
    {
        // invalid pattern
        CollectorDescription input;
        input.paths = _allFiles;
        input.rangeFrom = 0;
        input.rangeTo = 65535;
        input.filePattern = "invalid_pattern";
        CollectResults fileList;
        ASSERT_FALSE(FileListCollector::collectAllResults(input, fileList));
    }
    {
        // valid pattern
        CollectorDescription input;
        input.paths = _allFiles;
        input.rangeFrom = 0;
        input.rangeTo = 21845;
        input.filePattern = "common-<3>.gz";
        CollectResults fileListAll;
        ASSERT_TRUE(FileListCollector::collectAllResults(input, fileListAll));
        CollectResults fileList;
        for (size_t i = 0; i < fileListAll.size(); i++) {
            uint16_t hashId = fileListAll[i]._rangeId;
            if (input.rangeFrom <= hashId && hashId <= input.rangeTo) {
                fileList.push_back(fileListAll[i]);
            }
        }
        EXPECT_EQ(size_t(0), fileList.size());

        input.paths.push_back(_rootDir);
        input.filePattern = "data2/common-<3>.gz";
        ASSERT_TRUE(FileListCollector::collectAllResults(input, fileListAll));
        for (size_t i = 0; i < fileListAll.size(); i++)
        {
            uint16_t hashId = fileListAll[i]._rangeId;
            if (input.rangeFrom <= hashId && hashId <= input.rangeTo) {
                fileList.push_back(fileListAll[i]);
            }
        }
        EXPECT_EQ(size_t(1), fileList.size());
        EXPECT_EQ(_dataSource2Files[0], fileList[0]._fileName);
        EXPECT_EQ(uint32_t(0), fileList[0]._globalId);

        input.rangeFrom= 21846;
        input.rangeTo = 43690;
        fileListAll.clear();
        ASSERT_TRUE(FileListCollector::collectAllResults(input, fileListAll));
        fileList.clear();
        for (size_t i = 0; i < fileListAll.size(); i++)
        {
            uint16_t hashId = fileListAll[i]._rangeId;
            if (input.rangeFrom <= hashId && hashId <= input.rangeTo) {
                fileList.push_back(fileListAll[i]);
            }
        }
        EXPECT_EQ(size_t(1), fileList.size());
        EXPECT_EQ(_dataSource2Files[1], fileList[0]._fileName);
        EXPECT_EQ(uint32_t(1), fileList[0]._globalId);

        input.rangeFrom= 43691;
        input.rangeTo = 65535;
        fileList.clear();
        fileListAll.clear();
        ASSERT_TRUE(FileListCollector::collectAllResults(input, fileListAll));
        for (size_t i = 0; i < fileListAll.size(); i++)
        {
            uint16_t hashId = fileListAll[i]._rangeId;
            if (input.rangeFrom <= hashId && hashId <= input.rangeTo) {
                fileList.push_back(fileListAll[i]);
            }
        }
        EXPECT_EQ(size_t(1), fileList.size());
        EXPECT_EQ(_dataSource2Files[2], fileList[0]._fileName);
        EXPECT_EQ(uint32_t(2), fileList[0]._globalId);
    }
    {
        // multi file pattern with multi path
        // file pattern with 0 padding
        CollectorDescription input;
        KeyValueMap kv;
        kv["data"] = _multiPath1 + ',' + _multiPath2;
        kv["file_pattern"] = "common-<3>.gz;good-<3>.gz";
        proto::Range range;
        range.set_from(0);
        range.set_to(21845);
        CollectResults result;
        ASSERT_TRUE(FileListCollector::collect(kv, range, result));
        EXPECT_EQ(size_t(3), result.size());

        EXPECT_EQ(_mutliPathMultiPattern[0], result[0]._fileName);
        EXPECT_EQ(_mutliPathMultiPattern[3], result[1]._fileName);
        EXPECT_EQ(_mutliPathMultiPattern[6], result[2]._fileName);
        EXPECT_EQ((uint32_t)0, result[0]._globalId);
        EXPECT_EQ((uint32_t)1, result[1]._globalId);
        EXPECT_EQ((uint32_t)2, result[2]._globalId);

        range.set_from(21846);
        range.set_to(43690);
        ASSERT_TRUE(FileListCollector::collect(kv, range, result));
        EXPECT_EQ(size_t(3), result.size());
        EXPECT_EQ(_mutliPathMultiPattern[1], result[0]._fileName);
        EXPECT_EQ(_mutliPathMultiPattern[4], result[1]._fileName);
        EXPECT_EQ(_mutliPathMultiPattern[7], result[2]._fileName);
        EXPECT_EQ((uint32_t)3, result[0]._globalId);
        EXPECT_EQ((uint32_t)4, result[1]._globalId);
        EXPECT_EQ((uint32_t)5, result[2]._globalId);

        range.set_from(43691);
        range.set_to(65535);
        ASSERT_TRUE(FileListCollector::collect(kv, range, result));
        EXPECT_EQ(size_t(3), result.size());
        EXPECT_EQ(_mutliPathMultiPattern[2], result[0]._fileName);
        EXPECT_EQ(_mutliPathMultiPattern[5], result[1]._fileName);
        EXPECT_EQ(_mutliPathMultiPattern[8], result[2]._fileName);
        EXPECT_EQ((uint32_t)6, result[0]._globalId);
        EXPECT_EQ((uint32_t)7, result[1]._globalId);
        EXPECT_EQ((uint32_t)8, result[2]._globalId);
    }
}

TEST_F(FileListCollectorTest, testParseRawDocumentPaths) {
    {
        // normal dir
        vector<string> expectPaths;
        expectPaths.push_back(_dataSource1);
        expectPaths.push_back(_dataSource2);
        string dir = StringUtil::toString(expectPaths, " , ");
        vector<string> paths;
        EXPECT_TRUE(FileListCollector::parseRawDocumentPaths(paths, dir));
        EXPECT_EQ(expectPaths, paths);
    }
    {
        // empty dir
        vector<string> paths;
        EXPECT_FALSE(FileListCollector::parseRawDocumentPaths(paths, ""));
    }
    {
        // not exist dir
        vector<string> paths;
        vector<string> expectPaths;
        expectPaths.push_back(_dataSource1);
        expectPaths.push_back("not_exist");
        expectPaths.push_back(_dataSource2);
        string dir = StringUtil::toString(expectPaths, " , ");
        EXPECT_FALSE(FileListCollector::parseRawDocumentPaths(paths, ""));
    }
}

TEST_F(FileListCollectorTest, testKvMap2Description) {
    {
        // kv map without datapath is invalid
        KeyValueMap kvMap;
        CollectorDescription description;
        EXPECT_FALSE(FileListCollector::kvMap2Description(kvMap, description));
    }
    {
        // kv map with datapath is valid
        KeyValueMap kvMap;
        kvMap[DATA_PATH] = _dataSource1;
        CollectorDescription description;
        EXPECT_TRUE(FileListCollector::kvMap2Description(kvMap, description));
        EXPECT_EQ(string(), description.filePattern);
        EXPECT_THAT(description.paths, ElementsAre(_dataSource1));
    }
    {
        // kv map with datas
        KeyValueMap kvMap;
        kvMap[DATA_PATH] = _dataSource1 + "  ,  " + _dataSource2;
        kvMap[RAW_DOCUMENT_FILE_PATTERN] = "common-<8>.gz";
        CollectorDescription description;
        EXPECT_TRUE(FileListCollector::kvMap2Description(kvMap, description));
        EXPECT_THAT(description.paths, ElementsAre(_dataSource1, _dataSource2));
        EXPECT_EQ("common-<8>.gz", description.filePattern);
    }
}

TEST_F(FileListCollectorTest, testReadFromFilesSimple)
{
    KeyValueMap kvMap;
    string metaFilePath =
        FileUtil::joinFilePath(_rootDir, BS_RAW_FILE_META_DIR);
    string metaFileName =
        FileUtil::joinFilePath(metaFilePath, BS_RAW_FILES_META);
    kvMap[DATA_PATH] = _dataSource1;
    kvMap[BS_RAW_FILE_META_DIR] = metaFilePath;
    kvMap[DATA_DESCRIPTION_KEY] = "files";
    proto::Range range;
    range.set_from(0);
    range.set_to(65535);
    CollectResults results;
    EXPECT_TRUE(FileListCollector::collect(kvMap, range, results));
    uint32_t globalIds[3] = {0, 1, 2};
    EXPECT_THAT(results, FileListMatcher(_dataSource1Files, globalIds));

    DescriptionToCollectResultsMap descriptionResultMap;
    descriptionResultMap["files"] = results;
    string fileContent = ToJsonString(descriptionResultMap);
    IE_NAMESPACE(storage)::FileSystemWrapper::AtomicStore(
            metaFileName, fileContent.c_str(), fileContent.size());
    CollectResults results2;
    EXPECT_TRUE(FileListCollector::collect(kvMap, range, results2));
    EXPECT_THAT(results2, FileListMatcher(_dataSource1Files, globalIds));
}

TEST_F(FileListCollectorTest, testReadFromFiles)
{
    CollectorDescription input;
    KeyValueMap kv;
    string metaFilePath =
        FileUtil::joinFilePath(_rootDir, BS_RAW_FILE_META_DIR);
    string metaFileName =
        FileUtil::joinFilePath(metaFilePath, BS_RAW_FILES_META);
    kv["data"] = _multiPath1 + ',' + _multiPath2;
    kv["file_pattern"] = "common-<3>.gz;good-<3>.gz";
    kv[BS_RAW_FILE_META_DIR] = metaFilePath;
    kv[DATA_DESCRIPTION_KEY] = "files";

    proto::Range range;
    range.set_from(0);
    range.set_to(65535);
    CollectResults initResult;
    ASSERT_TRUE(FileListCollector::collect(kv, range, initResult));
    DescriptionToCollectResultsMap descriptionResultMap;
    descriptionResultMap["files"] = initResult;
    string fileContent = ToJsonString(descriptionResultMap);
    IE_NAMESPACE(storage)::FileSystemWrapper::AtomicStore(
            metaFileName, fileContent.c_str(), fileContent.size());

    range.set_from(0);
    range.set_to(21845);
    CollectResults result;
    ASSERT_TRUE(FileListCollector::collect(kv, range, result));
    EXPECT_EQ(size_t(3), result.size());
    EXPECT_EQ(_mutliPathMultiPattern[0], result[0]._fileName);
    EXPECT_EQ(_mutliPathMultiPattern[3], result[1]._fileName);
    EXPECT_EQ(_mutliPathMultiPattern[6], result[2]._fileName);
    EXPECT_EQ((uint32_t)0, result[0]._globalId);
    EXPECT_EQ((uint32_t)1, result[1]._globalId);
    EXPECT_EQ((uint32_t)2, result[2]._globalId);

    range.set_from(21846);
    range.set_to(43690);
    ASSERT_TRUE(FileListCollector::collect(kv, range, result));
    EXPECT_EQ(size_t(3), result.size());
    EXPECT_EQ(_mutliPathMultiPattern[1], result[0]._fileName);
    EXPECT_EQ(_mutliPathMultiPattern[4], result[1]._fileName);
    EXPECT_EQ(_mutliPathMultiPattern[7], result[2]._fileName);
    EXPECT_EQ((uint32_t)3, result[0]._globalId);
    EXPECT_EQ((uint32_t)4, result[1]._globalId);
    EXPECT_EQ((uint32_t)5, result[2]._globalId);

    range.set_from(43691);
    range.set_to(65535);
    ASSERT_TRUE(FileListCollector::collect(kv, range, result));
    EXPECT_EQ(size_t(3), result.size());
    EXPECT_EQ(_mutliPathMultiPattern[2], result[0]._fileName);
    EXPECT_EQ(_mutliPathMultiPattern[5], result[1]._fileName);
    EXPECT_EQ(_mutliPathMultiPattern[8], result[2]._fileName);
    EXPECT_EQ((uint32_t)6, result[0]._globalId);
    EXPECT_EQ((uint32_t)7, result[1]._globalId);
    EXPECT_EQ((uint32_t)8, result[2]._globalId);
}

}
}
