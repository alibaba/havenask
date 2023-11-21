#include "sql/ops/planTransform/GraphTransform.h"

#include <algorithm>
#include <ext/alloc_traits.h>
#include <iostream>
#include <map>
#include <memory>
#include <rapidjson/document.h>
#include <stdio.h>
#include <string>
#include <type_traits>
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "google/protobuf/text_format.h"
#include "iquan/common/Common.h"
#include "iquan/common/IquanException.h"
#include "iquan/config/ExecConfig.h"
#include "iquan/jni/DynamicParams.h"
#include "iquan/jni/IquanDqlRequest.h"
#include "iquan/jni/SqlPlan.h"
#include "navi/common.h"
#include "navi/proto/GraphDef.pb.h"
#include "rapidjson/filereadstream.h"
#include "sql/common/common.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace navi;
using namespace autil;
using namespace iquan;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace google::protobuf;

namespace sql {

class GraphTransformTest : public TESTBASE {
public:
    void setUp() override {
        ASSERT_NO_FATAL_FAILURE(loadSqlPlan());
        _request.sqlParams[IQUAN_PLAN_PREPARE_LEVEL] = string(IQUAN_REL_POST_OPTIMIZE);
        _execConfig.thisBizName = "qrs.default_sql";
    }

    void tearDown() override {
        if (::testing::Test::HasFailure()) {
            dumpGraphDef();
            dumpSubGraphDef();
        }
    }

public:
    void checkSuccess(const vector<string> &expectOutputPorts,
                      const vector<string> &expectOutputNodes) {
        ASSERT_NO_FATAL_FAILURE(loadGraphDef());
        ASSERT_TRUE(plan2graph());
        EXPECT_EQ(expectOutputPorts, _outputPorts);
        EXPECT_EQ(expectOutputNodes, _outputNodes);
        ASSERT_TRUE(TextFormat::PrintToString(_graph, &_actualGraphTxt));
        EXPECT_EQ(_expectGraphTxt, _actualGraphTxt);
    }

    void checkFail(GraphTransform::ErrorCode code) {
        ASSERT_FALSE(plan2graph());
        ASSERT_EQ(code, _trans->getErrorCode());
    }

    template <class T>
    void checkThrow() {
        ASSERT_THROW(plan2graph(), T);
    }

    struct SubGraphInfo {
        vector<string> inputNames;
        vector<string> inputDists;
        vector<string> outputNames;
    };

    void checkSubGraphs(const vector<SubGraphInfo> &infos) {
        EXPECT_NO_FATAL_FAILURE(collectDelayDpOpAttrs());
        EXPECT_EQ(infos.size(), _subGraphAttrs.size());
        auto size = _subGraphAttrs.size();
        for (size_t i = 0; i < size; ++i) {
            auto actual = extractSubGraphAttr(i);
            auto &info = infos[i];
            decltype(actual) expect = {
                {DELAY_DP_INPUT_NAMES_ATTR, info.inputNames},
                {DELAY_DP_INPUT_DISTS_ATTR, info.inputDists},
                {DELAY_DP_OUTPUT_NAMES_ATTR, info.outputNames},
            };
            EXPECT_EQ(expect, actual) << "sub graph normal attr not equal, index:" << i;
            EXPECT_TRUE(_expectSubGraphTxts[i] == _actualSubGraphTxts[i]);
        }
    }

private:
    bool plan2graph() {
        _config.reset(
            new GraphTransform::Config(_execConfig, _request, _sqlPlan.innerDynamicParams));
        _trans.reset(new GraphTransform(*_config));
        return _trans->sqlPlan2Graph(_sqlPlan, _graph, _outputPorts, _outputNodes);
    }

    std::string planJsonFileName() {
        return GET_CLASS_NAME() + "_iquan.json";
    }

    std::string graphDefFileName() {
        return GET_CLASS_NAME() + "_navi.pbtxt";
    }

    std::string subGraphDefFileName(size_t index) {
        return GET_CLASS_NAME() + "_sub_" + StringUtil::toString(index) + "_navi.pbtxt";
    }

    void loadSqlPlan() {
        _planPath = GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + planJsonFileName();
        ASSERT_TRUE(isFile(_planPath)) << _planPath << " not exist";
        auto fileDeleter = [](FILE *p) { fclose(p); };
        unique_ptr<FILE, decltype(fileDeleter)> inFile(fopen(_planPath.c_str(), "rb"), fileDeleter);
        vector<char> buffer(1024 * 10, 0);
        rapidjson::FileReadStream is(inFile.get(), buffer.data(), buffer.size());
        unique_ptr<rapidjson::Document> jsonDoc(new rapidjson::Document());
        jsonDoc->ParseStream(is);
        ASSERT_NO_THROW(FromRapidValue(_sqlPlan, *jsonDoc));
        _sqlPlan.rawRapidDoc.reset(jsonDoc.release());
    }

    void loadGraphDef() {
        auto path = GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + graphDefFileName();
        // ASSERT_TRUE(isFile(_graphPath)) << _graphPath << " not exist";
        _expectGraphTxt = readAll(path);
    }

    void loadSubGraphDef(size_t count) {
        _expectSubGraphTxts.resize(count);
        _actualSubGraphTxts.resize(count);
        for (size_t i = 0; i < count; ++i) {
            auto path = GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + subGraphDefFileName(i);
            _expectSubGraphTxts[i] = readAll(path);
        }
    }

    void collectDelayDpOpAttrs() {
        for (auto &subGraph : _graph.sub_graphs()) {
            for (auto &node : subGraph.nodes()) {
                if (node.kernel_name() != DELAY_DP_OP) {
                    continue;
                }
                auto p = &(node.binary_attrs());
                _subGraphAttrs.push_back(p);
            }
        }
        loadSubGraphDef(_subGraphAttrs.size());
    }

    void dumpGraphDef() {
        if (_actualGraphTxt.empty()) {
            return;
        }
        string expectFileName = graphDefFileName();
        string actualFileName = expectFileName + ".actual";
        string path = GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + actualFileName;
        writeAll(path, _actualGraphTxt);
        cout << "dump actual navi graph to: " << path << endl
             << "you can use below command to diff" << endl
             << "cd " << GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() << "; vimdiff " << expectFileName
             << " " << actualFileName << endl;
    }

    void dumpSubGraphDef() {
        for (size_t i = 0; i < _actualSubGraphTxts.size(); ++i) {
            string expectFileName = subGraphDefFileName(i);
            string actualFileName = expectFileName + ".actual";
            string path = GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + actualFileName;
            writeAll(path, _actualSubGraphTxts[i]);
            cout << "dump actual navi sub graph " << i << " to: " << path << endl;
        }
    }

    string readAll(const string &path) {
        ostringstream oss;
        FILE *inFile = fopen(path.c_str(), "rb");
        if (inFile == nullptr) {
            return string();
        }
        vector<char> buffer(1024 * 10, 0);
        size_t inBytes = fread(buffer.data(), 1, buffer.size(), inFile);
        while (inBytes > 0) {
            oss.write(buffer.data(), inBytes);
            inBytes = fread(buffer.data(), 1, buffer.size(), inFile);
        }
        fclose(inFile);
        return oss.str();
    }

    void writeAll(const string &path, const string &data) {
        FILE *outFile = fopen(path.c_str(), "wb");
        if (outFile == nullptr) {
            return;
        }
        const char *p = data.data();
        size_t totalBytes = data.size();
        size_t outBytes = fwrite(p, 1, totalBytes, outFile);
        while (outBytes > 0) {
            p += outBytes;
            totalBytes -= outBytes;
            outBytes = fwrite(p, 1, totalBytes, outFile);
        }
        fclose(outFile);
    }

    map<string, vector<string>> extractSubGraphAttr(size_t index) {
        DataBuffer buffer;
        map<string, vector<string>> opAttrs;
        auto &subGraphAttr = *(_subGraphAttrs[index]);
        EXPECT_EQ(5, subGraphAttr.size());

        buffer.initByString(subGraphAttr.at(DELAY_DP_INPUT_NAMES_ATTR), nullptr, false);
        buffer.read(opAttrs[DELAY_DP_INPUT_NAMES_ATTR]);

        buffer.initByString(subGraphAttr.at(DELAY_DP_INPUT_DISTS_ATTR), nullptr, false);
        buffer.read(opAttrs[DELAY_DP_INPUT_DISTS_ATTR]);

        buffer.initByString(subGraphAttr.at(DELAY_DP_OUTPUT_NAMES_ATTR), nullptr, false);
        buffer.read(opAttrs[DELAY_DP_OUTPUT_NAMES_ATTR]);

        GraphDef subGraph;
        EXPECT_TRUE(subGraph.ParseFromString(subGraphAttr.at(DELAY_DP_GRAPH_ATTR)))
            << "parse sub graph pb failed, index:" << index;
        EXPECT_TRUE(TextFormat::PrintToString(subGraph, &(_actualSubGraphTxts[index])))
            << "sub graph pb to string failed, index:" << index;

        EXPECT_EQ("0", subGraphAttr.at(DELAY_DP_DEBUG_ATTR));
        return opAttrs;
    }

protected:
    string _planPath;
    iquan::SqlPlan _sqlPlan;
    string _expectGraphTxt;
    string _actualGraphTxt;
    vector<string> _expectSubGraphTxts;
    vector<string> _actualSubGraphTxts;
    navi::GraphDef _graph;
    vector<string> _outputPorts;
    vector<string> _outputNodes;
    vector<const Map<string, string> *> _subGraphAttrs;
    ExecConfig _execConfig;
    iquan::IquanDqlRequest _request;
    unique_ptr<GraphTransform::Config> _config;
    unique_ptr<GraphTransform> _trans;
};

TEST_F(GraphTransformTest, testBuildGraphNoExchange) {
    checkSuccess({"output0"}, {"0"});
}

TEST_F(GraphTransformTest, testBuildGraphMultiExchange) {
    checkSuccess({"output0"}, {"0"});
}

TEST_F(GraphTransformTest, testBuildGraphParallel) {
    _execConfig.parallelConfig.parallelNum = 3;
    checkSuccess({"output0"}, {"0"});
}

TEST_F(GraphTransformTest, testScanExchangeSink) {
    checkSuccess({"output0"}, {"3"});
}

TEST_F(GraphTransformTest, testExchangeToSelf) {
    checkSuccess({"output0"}, {"7"});
}

TEST_F(GraphTransformTest, testHashJoinParallel) {
    _execConfig.parallelConfig.parallelNum = 2;
    _execConfig.parallelConfig.parallelTables = {"tableA"};
    checkSuccess({"output0"}, {"4"});
}

TEST_F(GraphTransformTest, testHashJoinWithoutParallel) {
    _execConfig.parallelConfig.parallelNum = 2;
    checkSuccess({"output0"}, {"4"});
}

TEST_F(GraphTransformTest, testDynamicParams) {
    _execConfig.parallelConfig.parallelNum = 2;
    _execConfig.parallelConfig.parallelTables = {"t1"};
    _request.dynamicParams._array = {{
        autil::legacy::Any(200),
        autil::legacy::Any(520.67),
        autil::legacy::Any(string("str1")),
    }};
    _request.sqlParams.erase(IQUAN_PLAN_PREPARE_LEVEL);
    _request.execParams[IQUAN_EXEC_SOURCE_ID] = string(IQUAN_EXEC_ATTR_SOURCE_ID);
    _request.execParams[IQUAN_EXEC_SOURCE_SPEC] = string(IQUAN_EXEC_ATTR_SOURCE_SPEC);
    checkSuccess({"output0"}, {"3"});
}

TEST_F(GraphTransformTest, testMissingDynamicParams) {
    _request.sqlParams.erase(IQUAN_PLAN_PREPARE_LEVEL);
    checkThrow<iquan::IquanException>();
}

TEST_F(GraphTransformTest, testInnerDynamicParams) {
    _sqlPlan.innerDynamicParams.reserveOneSqlParams();
    ASSERT_TRUE(_sqlPlan.innerDynamicParams.addKVParams("model_recall_0",
                                                        autil::legacy::Any(string("1,2,3,4"))));
    checkSuccess({"output0"}, {"3"});
}

TEST_F(GraphTransformTest, testMissingInnerDynamicParams) {
    checkThrow<iquan::IquanException>();
}

TEST_F(GraphTransformTest, testHintParams) {
    _request.dynamicParams._hint["p1"] = "123";
    _request.sqlParams[IQUAN_PLAN_PREPARE_LEVEL] = string(IQUAN_JNI_POST_OPTIMIZE);
    checkSuccess({"output0"}, {"3"});
}

TEST_F(GraphTransformTest, testMissingHintParams) {
    _request.sqlParams[IQUAN_PLAN_PREPARE_LEVEL] = string(IQUAN_JNI_POST_OPTIMIZE);
    checkThrow<iquan::IquanException>();
}

TEST_F(GraphTransformTest, testOverrideToLogicTable) {
    _request.execParams[IQUAN_EXEC_OVERRIDE_TO_LOGIC_TABLE] = "KhronosScanKernel";
    _request.execParams[IQUAN_EXEC_SEARCHER_BIZ_NAME] = "qrs.default_sql";
    checkSuccess({"output0"}, {"0"});
}

TEST_F(GraphTransformTest, testInlineModeAllGraph) {
    _execConfig.parallelConfig.parallelNum = 2;
    _request.execParams[IQUAN_EXEC_INLINE_WORKER] = "all";
    checkSuccess({"output0"}, {"4"});
}

TEST_F(GraphTransformTest, testInlineModeQrsGraph) {
    _execConfig.parallelConfig.parallelNum = 2;
    _request.execParams[IQUAN_EXEC_INLINE_WORKER] = "qrs";
    checkSuccess({"output0"}, {"4"});
}

TEST_F(GraphTransformTest, testInlineModeSearcherGraph) {
    _execConfig.parallelConfig.parallelNum = 2;
    _request.execParams[IQUAN_EXEC_INLINE_WORKER] = "searcher";
    checkSuccess({"output0"}, {"4"});
}

TEST_F(GraphTransformTest, testInlineModeOtherGraph) {
    _execConfig.parallelConfig.parallelNum = 2;
    _request.execParams[IQUAN_EXEC_INLINE_WORKER] = "ALL";
    checkSuccess({"output0"}, {"4"});
}

TEST_F(GraphTransformTest, testCteGraph) {
    checkSuccess({"output0"}, {"9"});
}

TEST_F(GraphTransformTest, testParallelScanUnion) {
    _execConfig.parallelConfig.parallelNum = 2;
    _execConfig.parallelConfig.parallelTables = {"t1"};
    checkSuccess({"output0"}, {"4"});
}

TEST_F(GraphTransformTest, testSqlValues) {
    checkSuccess({"output0"}, {"1"});
}

TEST_F(GraphTransformTest, testGroupingSet) {
    checkSuccess({"output0"}, {"15"});
}

TEST_F(GraphTransformTest, testTwoPhraseGraph) {
    checkSuccess({"output0"}, {"8"});
}

TEST_F(GraphTransformTest, testComplexCTE) {
    checkSuccess({"output0"}, {"16"});
}

TEST_F(GraphTransformTest, testExchangeInCTE) {
    checkSuccess({"output0"}, {"6"});
}

TEST_F(GraphTransformTest, testExchangeMultiOutput) {
    checkSuccess({"output0"}, {"3"});
}

TEST_F(GraphTransformTest, testTwoExhangeToJoin) {
    checkSuccess({"output0"}, {"10"});
}

TEST_F(GraphTransformTest, testParallelCTEJoin) {
    _execConfig.parallelConfig.parallelNum = 2;
    _execConfig.parallelConfig.parallelTables = {"t2"};
    checkSuccess({"output0"}, {"17"});
}

TEST_F(GraphTransformTest, testUseOutputDistribution) {
    checkSuccess({"output0"}, {"3"});
}

TEST_F(GraphTransformTest, testDelayDpSimple) {
    ASSERT_NO_FATAL_FAILURE(checkSuccess({"output0"}, {"4"}));
    ASSERT_NO_FATAL_FAILURE(checkSubGraphs(
        {{{"ph_0"},
          {
              R"json({"hash_mode":{"hash_fields":["$i1"],"hash_function":"HASH"},"partition_cnt":64})json",
          },
          {"2"}}}));
}

TEST_F(GraphTransformTest, testQrsDelayDp) {
    ASSERT_NO_FATAL_FAILURE(checkSuccess({"output0"}, {"4"}));
    ASSERT_NO_FATAL_FAILURE(checkSubGraphs(
        {{{"ph_0"},
          {
              R"json({"hash_mode":{"hash_fields":["$i1"],"hash_function":"HASH"},"partition_cnt":64})json",
          },
          {"2"}}}));
}

TEST_F(GraphTransformTest, testDelayDpMultiIO) {
    ASSERT_NO_FATAL_FAILURE(checkSuccess({"output0"}, {"16"}));
    ASSERT_NO_FATAL_FAILURE(checkSubGraphs(
        {{{"ph_0", "ph_1"},
          {R"json({"hash_mode":{"hash_fields":["$i1"],"hash_function":"HASH"},"partition_cnt":64})json",
           R"json({"hash_mode":{"hash_fields":["$i1"],"hash_function":"HASH"},"partition_cnt":64})json"},
          {"6", "8", "10"}}}));
}

TEST_F(GraphTransformTest, testTwoDelayDp) {
    ASSERT_NO_FATAL_FAILURE(checkSuccess({"output0"}, {"8"}));
    ASSERT_NO_FATAL_FAILURE(checkSubGraphs(
        {{{"ph_0"},
          {
              R"json({"hash_mode":{"hash_fields":["$i1"],"hash_function":"HASH"},"partition_cnt":64})json",
          },
          {"4"}},
         {{"ph_0"},
          {
              R"json({"hash_mode":{"hash_fields":["$i1"],"hash_function":"HASH"},"partition_cnt":8})json",
          },
          {"6"}}}));
}

TEST_F(GraphTransformTest, testTwoDelayDpMultiIO) {
    ASSERT_NO_FATAL_FAILURE(checkSuccess({"output0"}, {"14"}));
    ASSERT_NO_FATAL_FAILURE(checkSubGraphs(
        {{{"ph_0", "ph_1"},
          {
              R"json({"hash_mode":{"hash_fields":["$i1"],"hash_function":"HASH"},"partition_cnt":64})json",
              R"json({"hash_mode":{"hash_fields":["$i1"],"hash_function":"HASH"},"partition_cnt":64})json",
          },
          {"7"}},
         {{"ph_0", "ph_1"},
          {
              R"json({"hash_mode":{"hash_fields":["$i1"],"hash_function":"HASH"},"partition_cnt":8})json",
              R"json({"hash_mode":{"hash_fields":["$i1"],"hash_function":"HASH"},"partition_cnt":8})json",
          },
          {"12"}}}));
}

TEST_F(GraphTransformTest, testDelayDpToSelf) {
    ASSERT_NO_FATAL_FAILURE(checkSuccess({"output0"}, {"7"}));
    ASSERT_NO_FATAL_FAILURE(checkSubGraphs(
        {{{"ph_0"},
          {
              R"json({"hash_mode":{"hash_fields":["$id"],"hash_function":"HASH"},"partition_cnt":5,"type":"HASH_DISTRIBUTED"})json",
          },
          {"4"}}}));
}

TEST_F(GraphTransformTest, testComplexDelayDp) {
    ASSERT_NO_FATAL_FAILURE(checkSuccess({"output0"}, {"20"}));
    ASSERT_NO_FATAL_FAILURE(checkSubGraphs(
        {{{"ph_0"},
          {
              R"json({"hash_mode":{"hash_fields":["$biz_order_id"],"hash_function":"HASH"},"partition_cnt":4,"type":"HASH_DISTRIBUTED"})json",
          },
          {"13"}}}));
}

TEST_F(GraphTransformTest, testPartialDelayDp) {
    ASSERT_NO_FATAL_FAILURE(checkSuccess({"output0"}, {"7"}));
    ASSERT_NO_FATAL_FAILURE(checkSubGraphs({}));
}

TEST_F(GraphTransformTest, testGraphCircle) {
    checkFail(GraphTransform::ErrorCode::GRAPH_CIRCLE);
}

TEST_F(GraphTransformTest, testInputNotExist) {
    checkFail(GraphTransform::ErrorCode::NODE_NOT_EXIST);
}

TEST_F(GraphTransformTest, testExchangeLackInput1) {
    checkFail(GraphTransform::ErrorCode::LACK_NODE_INPUT);
}

TEST_F(GraphTransformTest, testExchangeLackInput2) {
    checkFail(GraphTransform::ErrorCode::LACK_NODE_INPUT);
}

TEST_F(GraphTransformTest, testMultiGroupInput) {
    checkFail(GraphTransform::ErrorCode::MULTI_GROUP_INPUT);
}

} // namespace sql
