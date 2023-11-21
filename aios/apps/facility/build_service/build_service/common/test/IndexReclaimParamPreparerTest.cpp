#include "build_service/common/IndexReclaimParamPreparer.h"

#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <ext/alloc_traits.h>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "autil/HashFuncFactory.h"
#include "autil/HashFunctionBase.h"
#include "autil/StringUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/test/unittest.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/table/normal_table/index_task/PrepareIndexReclaimParamOperation.h"
#include "indexlib/table/normal_table/index_task/document_reclaim/IndexReclaimerParam.h"
#include "swift/client/SwiftReader.h"
#include "swift/testlib/MockSwiftReader.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace indexlibv2::table;

namespace build_service { namespace common {

class MockIndexReclaimParamPreparer : public IndexReclaimParamPreparer
{
public:
    MockIndexReclaimParamPreparer(const std::string& clusterName) : IndexReclaimParamPreparer(clusterName) {}
    virtual ~MockIndexReclaimParamPreparer() {}

public:
    MOCK_METHOD3(doCreateSwiftReader,
                 swift::client::SwiftReader*(const std::string& swiftRoot, const std::string& clientConfig,
                                             const std::string& readerConfig));
};

class IndexReclaimParamPreparerTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

private:
    std::pair<swift::testlib::MockSwiftReader*, int64_t>
    prepareMockReader(const std::vector<std::string>& messageDataStrs, int64_t ttlInSeconds);
    void checkConfigFile(const std::string& filePath, const std::vector<std::string>& msg);
    indexlibv2::framework::IndexTaskContext* createTaskContext(bool useHashByCluster, int64_t ttlTime);
    void innerTest(const vector<string>& swiftMsg, const std::string& clusterName, int64_t stopTsInSec,
                   const vector<string>& resultMsg, bool useHashByCluster, int64_t ttlTime);
};

void IndexReclaimParamPreparerTest::setUp() {}

void IndexReclaimParamPreparerTest::tearDown() {}

void IndexReclaimParamPreparerTest::checkConfigFile(const std::string& fileContent, const std::vector<std::string>& msg)
{
    std::map<std::string, std::vector<IndexReclaimerParam>> singleParams;
    std::vector<IndexReclaimerParam> andParams;
    for (size_t i = 0; i < msg.size(); i++) {
        std::vector<std::string> msgInfos;
        autil::StringUtil::fromString(msg[i], msgInfos, ";");
        IndexReclaimerParam param;
        if (msgInfos[0].find("&") != std::string::npos) {
            std::vector<IndexReclaimerParam::ReclaimOprand> ropVec;
            std::vector<std::vector<std::string>> andTermInfo;
            autil::StringUtil::fromString(msgInfos[0], andTermInfo, "=", "&");
            for (size_t j = 0; j < andTermInfo.size(); j++) {
                assert(andTermInfo[j].size() == 2);
                IndexReclaimerParam::ReclaimOprand rop;
                rop.indexName = andTermInfo[j][0];
                rop.term = andTermInfo[j][1];
                ropVec.push_back(rop);
            }
            param.TEST_SetReclaimOprands(ropVec);
            andParams.push_back(param);
        } else {
            std::vector<std::string> singleTermInfo;
            autil::StringUtil::fromString(msgInfos[0], singleTermInfo, "=");
            assert(singleTermInfo.size() == 2);
            param.SetReclaimIndex(singleTermInfo[0]);
            param.SetReclaimTerms(autil::StringUtil::split(singleTermInfo[1], ","));
            singleParams[singleTermInfo[0]].push_back(param);
        }
    }

    std::vector<IndexReclaimerParam> ret;
    for (auto& item : singleParams) {
        set<std::string> termSet;
        for (const auto& vec : item.second) {
            for (const auto& term : vec.GetReclaimTerms()) {
                termSet.insert(term);
            }
        }
        std::vector<std::string> terms;
        for (auto& term : termSet) {
            terms.push_back(term);
        }
        IndexReclaimerParam param;
        param.SetReclaimIndex(item.first);
        param.SetReclaimTerms(terms);
        ret.push_back(param);
    }
    ret.insert(ret.end(), andParams.begin(), andParams.end());
    ASSERT_EQ(ToJsonString(ret), fileContent);
}

std::pair<swift::testlib::MockSwiftReader*, int64_t>
IndexReclaimParamPreparerTest::prepareMockReader(const std::vector<std::string>& messageDataStrs,
                                                 int64_t stopTsInSeconds)
{
    int64_t lastTs = -1;
    swift::testlib::MockSwiftReader* reader = new swift::testlib::MockSwiftReader;
    for (size_t i = 0; i < messageDataStrs.size(); i++) {
        std::vector<std::string> msgInfos;
        autil::StringUtil::fromString(messageDataStrs[i], msgInfos, ";");
        if (msgInfos.size() < 2) {
            // invalid format
            reader->addMessage(messageDataStrs[i], lastTs + 1);
            continue;
        }

        IndexReclaimerParam param;
        if (msgInfos[0].find("&") != std::string::npos) {
            std::vector<IndexReclaimerParam::ReclaimOprand> ropVec;
            std::vector<std::vector<std::string>> andTermInfo;
            autil::StringUtil::fromString(msgInfos[0], andTermInfo, "=", "&");
            for (size_t j = 0; j < andTermInfo.size(); j++) {
                assert(andTermInfo[j].size() == 2);
                IndexReclaimerParam::ReclaimOprand rop;
                rop.indexName = andTermInfo[j][0];
                rop.term = andTermInfo[j][1];
                ropVec.push_back(rop);
            }
            param.TEST_SetReclaimOprands(ropVec);
        } else {
            std::vector<std::string> singleTermInfo;
            autil::StringUtil::fromString(msgInfos[0], singleTermInfo, "=");
            assert(singleTermInfo.size() == 2);
            param.SetReclaimIndex(singleTermInfo[0]);
            param.SetReclaimTerms(autil::StringUtil::split(singleTermInfo[1], ","));
        }
        assert(msgInfos[1].find("ts=") == 0);
        int64_t ts = autil::StringUtil::fromString<int64_t>(msgInfos[1].substr(3));
        assert(lastTs < ts);
        lastTs = ts;

        std::string str = ToJsonString(param);
        Any any = ParseJson(str);
        JsonMap jsonMap = AnyCast<JsonMap>(any);
        if (msgInfos.size() == 3) {
            assert(msgInfos[2].find("cluster=") == 0);
            std::string clusterName = msgInfos[2].substr(8);
            jsonMap["cluster_name"] = Any(clusterName);
        }
        std::string value = ToString(Any(jsonMap));
        reader->addMessage(value, ts);
    }
    // mock for read limit
    if (stopTsInSeconds == -1) {
        reader->addMessage("invalidMessage", lastTs + 1 * 1000 * 1000);
        lastTs = lastTs + 1 * 1000 * 1000;
    } else if (lastTs < (stopTsInSeconds + 1) * 1000 * 1000) {
        reader->addMessage("invalidMessage", (stopTsInSeconds + 1) * 1000 * 1000);
        lastTs = ((stopTsInSeconds + 1) * 1000 * 1000);
    }
    return {reader, lastTs / 1000 / 1000};
}

indexlibv2::framework::IndexTaskContext* IndexReclaimParamPreparerTest::createTaskContext(bool useHashByCluster,
                                                                                          int64_t ttlTime)
{
    auto tabletOptions = std::make_shared<indexlibv2::config::TabletOptions>();
    string config = R"( {
        "offline_index_config" : {
            "index_task_configs" : [
                {
                    "task_name" : "reclaim",
                    "task_type" : "reclaim",
                    "trigger" : "auto",
                    "settings" : {
                        "doc_reclaim_source" : {
                            "swift_root" : "http://test/swift",
                            "topic_name" : "test" )";
    if (useHashByCluster) {
        config += R"(,"swift_reader_config" : "hashByClusterName=true;clusterHashFunc=HASH;")";
    }
    if (ttlTime != -1) {
        config += ",\"ttl_in_seconds\" :" + std::to_string(ttlTime);
    }

    config += R"(}}}]}})";

    FromJsonString(*tabletOptions, config);
    auto context = new indexlibv2::framework::IndexTaskContext();
    context->SetTabletOptions(tabletOptions);
    context->TEST_SetFenceRoot(indexlib::file_system::Directory::GetPhysicalDirectory(GET_TEMP_DATA_PATH()));
    return context;
}

void IndexReclaimParamPreparerTest::innerTest(const vector<string>& swiftMsg, const std::string& clusterName,
                                              int64_t stopTsInSec, const vector<string>& resultMsg,
                                              bool useHashByCluster, int64_t ttlTime)
{
    auto [reader, maxTs] = prepareMockReader(swiftMsg, stopTsInSec);
    std::unique_ptr<indexlibv2::framework::IndexTaskContext> context(createTaskContext(useHashByCluster, ttlTime));
    string expectReaderConfig = "topicName=test";

    if (useHashByCluster) {
        autil::HashFunctionBasePtr hashFunc = autil::HashFuncFactory::createHashFunc("HASH");
        uint32_t id = hashFunc->getHashId(clusterName);
        expectReaderConfig = expectReaderConfig + ";from=" + autil::StringUtil::toString(id);
        expectReaderConfig = expectReaderConfig + ";to=" + autil::StringUtil::toString(id);
    }

    MockIndexReclaimParamPreparer preparer(clusterName);
    EXPECT_CALL(preparer, doCreateSwiftReader(_, _, expectReaderConfig)).WillOnce(Return(reader));
    std::map<std::string, std::string> params;
    auto stopTs = stopTsInSec > 0 ? stopTsInSec : maxTs - 1;
    params["task_stop_ts_in_seconds"] = std::to_string(stopTs);
    params["operation_id"] = "0";
    params["use_op_fence_dir"] = "1";
    auto [status, fenceDir] = context->CreateOpFenceRoot(0, true);
    ASSERT_TRUE(preparer.Run(context.get(), params).IsOK());

    std::string reclaimParamStr;
    std::string filePath = indexlibv2::PathUtil::JoinPath(PrepareIndexReclaimParamOperation::DOC_RECLAIM_DATA_DIR,
                                                          PrepareIndexReclaimParamOperation::DOC_RECLAIM_CONF_FILE);

    indexlib::file_system::ReaderOption readerOption(indexlib::file_system::FSOT_MEM);
    status = fenceDir->Load(filePath, readerOption, reclaimParamStr).Status();
    checkConfigFile(reclaimParamStr, resultMsg);

    // run again for mock fail over
    {
        auto [reader, maxTs] = prepareMockReader(swiftMsg, stopTsInSec);
        EXPECT_CALL(preparer, doCreateSwiftReader(_, _, expectReaderConfig)).WillOnce(Return(reader));
        auto [status, fenceDir] = context->CreateOpFenceRoot(0, true);
        ASSERT_TRUE(preparer.Run(context.get(), params).IsOK());
        reclaimParamStr = "";
        status = fenceDir->Load(filePath, readerOption, reclaimParamStr).Status();
        checkConfigFile(reclaimParamStr, resultMsg);
    }
}

TEST_F(IndexReclaimParamPreparerTest, testSimple)
{
    std::vector<std::string> msgs = {
        "appid=1,2;ts=100000000;cluster=a",      "appid=2,3,4,5;ts=200000000;cluster=a",
        "appid=9&test=1;ts=500000000;cluster=a", "service=8,9;ts=1000000000;cluster=a",
        "appid=12,15;ts=1200000000;cluster=b",   "appid=13&test=2;ts=2000000000;cluster=b"};
    innerTest(msgs, "a", 2000,
              {
                  "appid=1,2;ts=100000000;cluster=a",
                  "appid=2,3,4,5;ts=200000000;cluster=a",
                  "appid=9&test=1;ts=500000000;cluster=a",
                  "service=8,9;ts=1000000000;cluster=a",
              },
              false, -1);
    innerTest(msgs, "a", 400, {"appid=1,2;ts=100000000;cluster=a", "appid=2,3,4,5;ts=200000000;cluster=a"}, false, -1);
    innerTest(msgs, "b", 2100, {"appid=12,15;ts=12000000;cluster=b", "appid=13&test=2;ts=20000000;cluster=b"}, false,
              -1);
    innerTest(msgs, "b", 1500, {"appid=12,15;ts=12000000;cluster=b"}, false, -1);
}

TEST_F(IndexReclaimParamPreparerTest, testWithClusterHash)
{
    std::vector<std::string> msgs = {
        "appid=1,2;ts=100000000;cluster=a",      "appid=2,3,4,5;ts=200000000;cluster=a",
        "appid=9&test=1;ts=500000000;cluster=a", "service=8,9;ts=1000000000;cluster=a",
        "appid=12,15;ts=1200000000;cluster=b",   "appid=13&test=2;ts=2000000000;cluster=b"};
    innerTest(msgs, "a", 2000,
              {
                  "appid=1,2;ts=100000000;cluster=a",
                  "appid=2,3,4,5;ts=200000000;cluster=a",
                  "appid=9&test=1;ts=500000000;cluster=a",
                  "service=8,9;ts=1000000000;cluster=a",
              },
              true, -1);
    innerTest(msgs, "b", 1500, {"appid=12,15;ts=12000000;cluster=b"}, true, -1);
}

TEST_F(IndexReclaimParamPreparerTest, testInvalidMsg)
{
    {
        // invalid msg format
        vector<string> msgs = {"appid=1,2;ts=1000000;cluster=a", "invalid_format", "appid=3,4;ts=2000000;cluster=a"};

        vector<string> result = {"appid=1,2;ts=1000000;cluster=a", "appid=3,4;ts=2000000;cluster=a"};
        innerTest(msgs, "a", 25, result, false, -1);
    }
    {
        // invalid msg without clusterName
        vector<string> msgs = {"appid=1,2;ts=1000000;cluster=a", "appid=3,4;ts=2000000",
                               "appid=3,4;ts=3000000;cluster=a"};

        vector<string> result = {"appid=1,2;ts=1000000;cluster=a", "appid=3,4;ts=3000000;cluster=a"};
        innerTest(msgs, "a", 25, result, false, -1);
    }
}

TEST_F(IndexReclaimParamPreparerTest, testNoMatchClusterMsg)
{
    vector<string> msgs = {"appid=1,2;ts=1000000;cluster=a", "invalid_format", "appid=3,4;ts=2000000;cluster=a"};
    vector<string> result;
    innerTest(msgs, "nocluster", 25, result, false, -1);
}

TEST_F(IndexReclaimParamPreparerTest, testEnableMsgTTL)
{
    vector<string> msgs = {"appid=12,15;ts=12000000;cluster=b", "appid=13&test=2;ts=20000000;cluster=b"};
    innerTest(msgs, "b", 25, {"appid=13&test=2;ts=20000000;cluster=b"}, false, 8);
}

TEST_F(IndexReclaimParamPreparerTest, testForInvalidInput)
{
    {
        auto invalidOptions = std::make_shared<indexlibv2::config::TabletOptions>();
        string config = R"( {
        "offline_index_config" : {
            "index_task_configs" : []
        }})";

        FromJsonString(*invalidOptions, config);
        std::unique_ptr<indexlibv2::framework::IndexTaskContext> context(new indexlibv2::framework::IndexTaskContext());
        context->SetTabletOptions(invalidOptions);
        context->TEST_SetFenceRoot(indexlib::file_system::Directory::GetPhysicalDirectory(GET_TEMP_DATA_PATH()));
        std::map<std::string, std::string> params;
        params["task_stop_ts_in_seconds"] = std::to_string(100);
        MockIndexReclaimParamPreparer preparer("test");
        ASSERT_FALSE(preparer.Run(context.get(), params).IsOK());
    }
    {
        auto tabletOptions = std::make_shared<indexlibv2::config::TabletOptions>();
        string config = R"( {
        "offline_index_config" : {
            "index_task_configs" : [
                {
                    "task_name" : "reclaim",
                    "task_type" : "reclaim",
                    "trigger" : "auto",
                    "settings" : {
                        "doc_reclaim_source" : {
                            "swift_root" : "http://test/swift",
                            "topic_name" : "test"
                      }
                  }
               }
         ]
         }})";
        FromJsonString(*tabletOptions, config);
        std::unique_ptr<indexlibv2::framework::IndexTaskContext> context(new indexlibv2::framework::IndexTaskContext());
        context->SetTabletOptions(tabletOptions);
        context->TEST_SetFenceRoot(indexlib::file_system::Directory::GetPhysicalDirectory(GET_TEMP_DATA_PATH()));
        std::map<std::string, std::string> params;
        MockIndexReclaimParamPreparer preparer("test");
        ASSERT_FALSE(preparer.Run(context.get(), params).IsOK());
    }
}

}} // namespace build_service::common
