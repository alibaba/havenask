#include "sql/ops/scan/DocIdRangesReduceOptimize.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <ext/alloc_traits.h>
#include <limits>
#include <map>
#include <memory>
#include <rapidjson/document.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "autil/mem_pool/PoolVector.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/PartitionInfoWrapper.h"
#include "indexlib/base/Types.h"
#include "indexlib/table/normal_table/DimensionDescription.h"
#include "navi/log/NaviLoggerProvider.h"
#include "sql/common/FieldInfo.h"
#include "sql/common/common.h"
#include "sql/ops/condition/Condition.h"
#include "sql/ops/condition/ConditionParser.h"
#include "sql/ops/scan/DocIdRangesReduceOptimize.hpp"
#include "sql/ops/scan/KeyRange.h"
#include "sql/ops/test/OpTestBase.h"
#include "suez/sdk/TableDefConfig.h"
#include "unittest/unittest.h"

using namespace autil;
using namespace std;
using namespace isearch;
using namespace isearch::search;

namespace sql {

size_t idx = 0;

class FakeIndexPartitionReaderWrapper : public IndexPartitionReaderWrapper {
public:
    FakeIndexPartitionReaderWrapper(const std::vector<indexlib::DocIdRangeVector> &docIdRanges,
                                    bool flag = true)
        : IndexPartitionReaderWrapper()
        , _docIdRanges(docIdRanges)
        , _flag(flag) {
        idx = 0;
    }

public:
    bool getSortedDocIdRanges(
        const std::vector<std::shared_ptr<indexlib::table::DimensionDescription>> &dimensions,
        const indexlib::DocIdRange &rangeLimits,
        indexlib::DocIdRangeVector &resultRanges) const override {
        resultRanges = _docIdRanges[idx++];
        return _flag;
    }

private:
    std::vector<indexlib::DocIdRangeVector> _docIdRanges;
    bool _flag;
};

class DocIdRangesReduceOptimizeTest : public OpTestBase {
public:
    void setUp();
    void tearDown();

private:
    std::map<std::string, FieldInfo> _fieldInfos;
    DocIdRangesReduceOptimizePtr _optimize;
};

void DocIdRangesReduceOptimizeTest::setUp() {
    std::vector<suez::SortDescription> sortDescs
        = {{"k1", "ASC"}, {"k2", "DESC"}, {"k3", "DESC"}, {"k4", "ASC"}, {"k5", "DESC"}};
    _fieldInfos = {{"k1", {"k1", "int64"}},
                   {"k2", {"k2", "uint64"}},
                   {"k3", {"k3", "int32"}},
                   {"attr1", {"attr1", "int32"}},
                   {"k4", {"k4", "double"}}};
    _optimize.reset(new DocIdRangesReduceOptimize(sortDescs, _fieldInfos));
}

void DocIdRangesReduceOptimizeTest::tearDown() {}

TEST_F(DocIdRangesReduceOptimizeTest, testGenKeyRange) {
    {
        // op  =
        string condStr = R"({"op":"=","params": ["$attr1", 5]})";
        ConditionParser parser;
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);

        const SimpleValue &leafCondition
            = dynamic_cast<LeafCondition *>(cond.get())->getCondition();
        string op(leafCondition[SQL_CONDITION_OPERATOR].GetString());
        const string &attrName = leafCondition[SQL_CONDITION_PARAMETER][0].GetString();
        const SimpleValue &value = leafCondition[SQL_CONDITION_PARAMETER][1];
        auto result = DocIdRangesReduceOptimize::genKeyRange<int32_t>(attrName, op, value);
        ASSERT_EQ(attrName, result->_attrName);
        ASSERT_EQ(1, result->_ranges.size());
        ASSERT_EQ(5, result->_ranges[0].first);
        ASSERT_EQ(5, result->_ranges[0].second);
        delete result;
    }
    {
        // op >
        string condStr = R"({"op":">","params": ["$attr1", 5]})";
        ConditionParser parser;
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);

        const SimpleValue &leafCondition
            = dynamic_cast<LeafCondition *>(cond.get())->getCondition();
        string op(leafCondition[SQL_CONDITION_OPERATOR].GetString());
        const string &attrName = leafCondition[SQL_CONDITION_PARAMETER][0].GetString();
        const SimpleValue &value = leafCondition[SQL_CONDITION_PARAMETER][1];
        auto result = DocIdRangesReduceOptimize::genKeyRange<int32_t>(attrName, op, value);
        ASSERT_EQ(attrName, result->_attrName);
        ASSERT_EQ(1, result->_ranges.size());
        ASSERT_EQ(5, result->_ranges[0].first);
        ASSERT_EQ(numeric_limits<int32_t>::max(), result->_ranges[0].second);
        delete result;
    }
    {
        // op <
        string condStr = R"({"op":"<","params": ["$attr1", 5]})";
        ConditionParser parser;
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);

        const SimpleValue &leafCondition
            = dynamic_cast<LeafCondition *>(cond.get())->getCondition();
        string op(leafCondition[SQL_CONDITION_OPERATOR].GetString());
        const string &attrName = leafCondition[SQL_CONDITION_PARAMETER][0].GetString();
        const SimpleValue &value = leafCondition[SQL_CONDITION_PARAMETER][1];
        auto result = DocIdRangesReduceOptimize::genKeyRange<int32_t>(attrName, op, value);
        ASSERT_EQ(attrName, result->_attrName);
        ASSERT_EQ(1, result->_ranges.size());
        ASSERT_EQ(numeric_limits<int32_t>::min(), result->_ranges[0].first);
        ASSERT_EQ(5, result->_ranges[0].second);
        delete result;
    }
    {
        // op >=
        string condStr = R"({"op":">=","params": ["$attr1", 5]})";
        ConditionParser parser;
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);

        const SimpleValue &leafCondition
            = dynamic_cast<LeafCondition *>(cond.get())->getCondition();
        string op(leafCondition[SQL_CONDITION_OPERATOR].GetString());
        const string &attrName = leafCondition[SQL_CONDITION_PARAMETER][0].GetString();
        const SimpleValue &value = leafCondition[SQL_CONDITION_PARAMETER][1];
        auto result = DocIdRangesReduceOptimize::genKeyRange<int32_t>(attrName, op, value);
        ASSERT_EQ(attrName, result->_attrName);
        ASSERT_EQ(1, result->_ranges.size());
        ASSERT_EQ(5, result->_ranges[0].first);
        ASSERT_EQ(numeric_limits<int32_t>::max(), result->_ranges[0].second);
        delete result;
    }
    {
        // op <=
        string condStr = R"({"op":"<=","params": ["$attr1", 5]})";
        ConditionParser parser;
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);

        const SimpleValue &leafCondition
            = dynamic_cast<LeafCondition *>(cond.get())->getCondition();
        string op(leafCondition[SQL_CONDITION_OPERATOR].GetString());
        const string &attrName = leafCondition[SQL_CONDITION_PARAMETER][0].GetString();
        const SimpleValue &value = leafCondition[SQL_CONDITION_PARAMETER][1];
        auto result = DocIdRangesReduceOptimize::genKeyRange<int32_t>(attrName, op, value);
        ASSERT_EQ(attrName, result->_attrName);
        ASSERT_EQ(1, result->_ranges.size());
        ASSERT_EQ(numeric_limits<int32_t>::min(), result->_ranges[0].first);
        ASSERT_EQ(5, result->_ranges[0].second);
        delete result;
    }
    {
        // IN expr
        string condStr = R"({"op":"IN", "params":["$attr1", 100, 200]})";
        ConditionParser parser;
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);

        const SimpleValue &leafCondition
            = dynamic_cast<LeafCondition *>(cond.get())->getCondition();
        string op(leafCondition[SQL_CONDITION_OPERATOR].GetString());
        const string &attrName = leafCondition[SQL_CONDITION_PARAMETER][0].GetString();
        const SimpleValue &value = leafCondition[SQL_CONDITION_PARAMETER];
        auto result = DocIdRangesReduceOptimize::genKeyRange<int32_t>(attrName, op, value);
        ASSERT_EQ(attrName, result->_attrName);
        ASSERT_EQ(2, result->_ranges.size());
        ASSERT_EQ(100, result->_ranges[0].first);
        ASSERT_EQ(100, result->_ranges[0].second);
        ASSERT_EQ(200, result->_ranges[1].first);
        ASSERT_EQ(200, result->_ranges[1].second);
        delete result;
    }
    {
        // Contain expr
        string condStr = R"({"op":"contain","params":["$attr1","1|2"],"type":"UDF"})";
        ConditionParser parser;
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);

        const SimpleValue &leafCondition
            = dynamic_cast<LeafCondition *>(cond.get())->getCondition();
        string op(leafCondition[SQL_CONDITION_OPERATOR].GetString());
        const string &attrName = leafCondition[SQL_CONDITION_PARAMETER][0].GetString();
        const SimpleValue &value = leafCondition[SQL_CONDITION_PARAMETER];
        auto result = DocIdRangesReduceOptimize::genKeyRange<int32_t>(attrName, op, value);
        ASSERT_EQ(attrName, result->_attrName);
        ASSERT_EQ(2, result->_ranges.size());
        ASSERT_EQ(1, result->_ranges[0].first);
        ASSERT_EQ(1, result->_ranges[0].second);
        ASSERT_EQ(2, result->_ranges[1].first);
        ASSERT_EQ(2, result->_ranges[1].second);
        delete result;
    }
    {
        // Contain expr 3 params
        string condStr = R"({"op":"contain","params":["$attr1","1&2", "&"],"type":"UDF"})";
        ConditionParser parser;
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);

        const SimpleValue &leafCondition
            = dynamic_cast<LeafCondition *>(cond.get())->getCondition();
        string op(leafCondition[SQL_CONDITION_OPERATOR].GetString());
        const string &attrName = leafCondition[SQL_CONDITION_PARAMETER][0].GetString();
        const SimpleValue &value = leafCondition[SQL_CONDITION_PARAMETER];
        auto result = DocIdRangesReduceOptimize::genKeyRange<int32_t>(attrName, op, value);
        ASSERT_EQ(attrName, result->_attrName);
        ASSERT_EQ(2, result->_ranges.size());
        ASSERT_EQ(1, result->_ranges[0].first);
        ASSERT_EQ(1, result->_ranges[0].second);
        ASSERT_EQ(2, result->_ranges[1].first);
        ASSERT_EQ(2, result->_ranges[1].second);
        delete result;
    }
}

TEST_F(DocIdRangesReduceOptimizeTest, testGenKeyRange_type) {
    {
        // type int32
        string condStr = R"({"op":"=","params": ["$attr1", 2200000000]})";
        ConditionParser parser;
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);

        const SimpleValue &leafCondition
            = dynamic_cast<LeafCondition *>(cond.get())->getCondition();
        string op(leafCondition[SQL_CONDITION_OPERATOR].GetString());
        const string &attrName = leafCondition[SQL_CONDITION_PARAMETER][0].GetString();
        const SimpleValue &value = leafCondition[SQL_CONDITION_PARAMETER][1];
        auto result = DocIdRangesReduceOptimize::genKeyRange<int32_t>(attrName, op, value);
        ASSERT_EQ(attrName, result->_attrName);
        ASSERT_EQ(1, result->_ranges.size());
        ASSERT_EQ(-2094967296, result->_ranges[0].first);
        ASSERT_EQ(-2094967296, result->_ranges[0].second);
        delete result;
    }
    {
        // type uint32
        string condStr = R"({"op":"=","params": ["$attr1", 2200000000]})";
        ConditionParser parser;
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);

        const SimpleValue &leafCondition
            = dynamic_cast<LeafCondition *>(cond.get())->getCondition();
        string op(leafCondition[SQL_CONDITION_OPERATOR].GetString());
        const string &attrName = leafCondition[SQL_CONDITION_PARAMETER][0].GetString();
        const SimpleValue &value = leafCondition[SQL_CONDITION_PARAMETER][1];
        auto result = DocIdRangesReduceOptimize::genKeyRange<uint32_t>(attrName, op, value);
        ASSERT_EQ(attrName, result->_attrName);
        ASSERT_EQ(1, result->_ranges.size());
        ASSERT_EQ(2200000000l, result->_ranges[0].first);
        ASSERT_EQ(2200000000l, result->_ranges[0].second);
        delete result;
    }
    {
        // type int64
        string condStr = R"({"op":"=","params": ["$attr1", 9223372036854775807]})";
        ConditionParser parser;
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);

        const SimpleValue &leafCondition
            = dynamic_cast<LeafCondition *>(cond.get())->getCondition();
        string op(leafCondition[SQL_CONDITION_OPERATOR].GetString());
        const string &attrName = leafCondition[SQL_CONDITION_PARAMETER][0].GetString();
        const SimpleValue &value = leafCondition[SQL_CONDITION_PARAMETER][1];
        auto result = DocIdRangesReduceOptimize::genKeyRange<int64_t>(attrName, op, value);
        ASSERT_EQ(attrName, result->_attrName);
        ASSERT_EQ(1, result->_ranges.size());
        ASSERT_EQ(9223372036854775807, result->_ranges[0].first);
        ASSERT_EQ(9223372036854775807, result->_ranges[0].second);
        delete result;
    }
    {
        // type uint64
        string condStr = R"({"op":"=","params": ["$attr1", 9223372036854775807]})";
        ConditionParser parser;
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);

        const SimpleValue &leafCondition
            = dynamic_cast<LeafCondition *>(cond.get())->getCondition();
        string op(leafCondition[SQL_CONDITION_OPERATOR].GetString());
        const string &attrName = leafCondition[SQL_CONDITION_PARAMETER][0].GetString();
        const SimpleValue &value = leafCondition[SQL_CONDITION_PARAMETER][1];
        auto result = DocIdRangesReduceOptimize::genKeyRange<uint64_t>(attrName, op, value);
        ASSERT_EQ(attrName, result->_attrName);
        ASSERT_EQ(1, result->_ranges.size());
        ASSERT_EQ(9223372036854775807, result->_ranges[0].first);
        ASSERT_EQ(9223372036854775807, result->_ranges[0].second);
        delete result;
    }
}

TEST_F(DocIdRangesReduceOptimizeTest, testTryAddKeyRange_filedVecMiss) {
    navi::NaviLoggerProvider provider("TRACE3");
    string condStr = R"({"op":"=","params": ["$k5", 5]})";
    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    const SimpleValue &leafCondition = dynamic_cast<LeafCondition *>(cond.get())->getCondition();
    string op(leafCondition[SQL_CONDITION_OPERATOR].GetString());
    const SimpleValue &attrName = leafCondition[SQL_CONDITION_PARAMETER][0];
    const SimpleValue &value = leafCondition[SQL_CONDITION_PARAMETER][1];
    _optimize->tryAddKeyRange(attrName, value, op);
    ASSERT_EQ(true, _optimize->_key2keyRange.empty());
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "attr [k5] not found in field infos", traces);
}

TEST_F(DocIdRangesReduceOptimizeTest, testTryAddKeyRange_keyVecMiss) {
    navi::NaviLoggerProvider provider("TRACE3");
    string condStr = R"({"op":"=","params": ["$attr1", 5]})";
    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    const SimpleValue &leafCondition = dynamic_cast<LeafCondition *>(cond.get())->getCondition();
    string op(leafCondition[SQL_CONDITION_OPERATOR].GetString());
    const SimpleValue &attrName = leafCondition[SQL_CONDITION_PARAMETER][0];
    const SimpleValue &value = leafCondition[SQL_CONDITION_PARAMETER][1];
    _optimize->tryAddKeyRange(attrName, value, op);
    ASSERT_EQ(true, _optimize->_key2keyRange.empty());
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "attr [attr1] not found in sort desc", traces);
}

TEST_F(DocIdRangesReduceOptimizeTest, testTryAddKeyRange_unknownOp) {
    navi::NaviLoggerProvider provider("TRACE3");
    string condStr = R"({"op":"+","params": ["$k1", 5]})";
    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    const SimpleValue &leafCondition = dynamic_cast<LeafCondition *>(cond.get())->getCondition();
    string op(leafCondition[SQL_CONDITION_OPERATOR].GetString());
    const SimpleValue &attrName = leafCondition[SQL_CONDITION_PARAMETER][0];
    const SimpleValue &value = leafCondition[SQL_CONDITION_PARAMETER][1];
    _optimize->tryAddKeyRange(attrName, value, op);
    ASSERT_FALSE(_optimize->_key2keyRange.empty())
        << autil::StringUtil::toString(provider.getTrace(""));
}

TEST_F(DocIdRangesReduceOptimizeTest, testTryAddKeyRange_unIntType) {
    navi::NaviLoggerProvider provider("TRACE3");
    string condStr = R"({"op":"=","params": ["$k4", 5.4]})";
    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    const SimpleValue &leafCondition = dynamic_cast<LeafCondition *>(cond.get())->getCondition();
    string op(leafCondition[SQL_CONDITION_OPERATOR].GetString());
    const SimpleValue &attrName = leafCondition[SQL_CONDITION_PARAMETER][0];
    const SimpleValue &value = leafCondition[SQL_CONDITION_PARAMETER][1];
    _optimize->tryAddKeyRange(attrName, value, op);
    ASSERT_TRUE(_optimize->_key2keyRange.empty());
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "type not supported, will skip", traces);
}

TEST_F(DocIdRangesReduceOptimizeTest, testTryAddKeyRange) {
    string condStr = R"({"op":"=","params": ["$k1", 4]})";
    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    const SimpleValue &leafCondition = dynamic_cast<LeafCondition *>(cond.get())->getCondition();
    string op(leafCondition[SQL_CONDITION_OPERATOR].GetString());
    const SimpleValue &attrName = leafCondition[SQL_CONDITION_PARAMETER][0];
    const SimpleValue &value = leafCondition[SQL_CONDITION_PARAMETER][1];
    _optimize->tryAddKeyRange(attrName, value, op);

    ASSERT_EQ(1, _optimize->_key2keyRange.size());
    ASSERT_TRUE(_optimize->_key2keyRange.end() != _optimize->_key2keyRange.find("k1"));
    KeyRangeBasePtr keyRange = _optimize->_key2keyRange["k1"];
    auto *result = dynamic_cast<KeyRangeTyped<int64_t> *>(keyRange.get());
    ASSERT_EQ("k1", result->_attrName);
    ASSERT_EQ(1, result->_ranges.size());
    ASSERT_EQ(4, result->_ranges[0].first);
    ASSERT_EQ(4, result->_ranges[0].second);
}

TEST_F(DocIdRangesReduceOptimizeTest, testParseArithmeticOp_rightColumn) {
    navi::NaviLoggerProvider provider("TRACE2");
    string condStr = R"({"op":"=","params": [5, "$k1"]})";
    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    const SimpleValue &leafCondition = dynamic_cast<LeafCondition *>(cond.get())->getCondition();
    string op(leafCondition[SQL_CONDITION_OPERATOR].GetString());
    const SimpleValue &param = leafCondition[SQL_CONDITION_PARAMETER];
    _optimize->parseArithmeticOp(param, op);

    ASSERT_EQ(1, _optimize->_key2keyRange.size()) << StringUtil::toString(provider.getTrace(""));
    KeyRangeBasePtr keyRange = _optimize->_key2keyRange.begin()->second;
    auto *result = dynamic_cast<KeyRangeTyped<int64_t> *>(keyRange.get());
    ASSERT_TRUE(result);
    ASSERT_EQ("k1", result->_attrName);
    ASSERT_EQ(1, result->_ranges.size());
    ASSERT_EQ(5, result->_ranges[0].first);
    ASSERT_EQ(5, result->_ranges[0].second);
}

TEST_F(DocIdRangesReduceOptimizeTest, testParseArithmeticOp_leftColumn) {
    navi::NaviLoggerProvider provider("TRACE2");
    string condStr = R"({"op":"=","params": ["$k1", 5]})";
    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    const SimpleValue &leafCondition = dynamic_cast<LeafCondition *>(cond.get())->getCondition();
    string op(leafCondition[SQL_CONDITION_OPERATOR].GetString());
    const SimpleValue &param = leafCondition[SQL_CONDITION_PARAMETER];
    _optimize->parseArithmeticOp(param, op);

    ASSERT_EQ(1, _optimize->_key2keyRange.size()) << StringUtil::toString(provider.getTrace(""));
    KeyRangeBasePtr keyRange = _optimize->_key2keyRange.begin()->second;
    auto *result = dynamic_cast<KeyRangeTyped<int64_t> *>(keyRange.get());
    ASSERT_TRUE(result);
    ASSERT_EQ("k1", result->_attrName);
    ASSERT_EQ(1, result->_ranges.size());
    ASSERT_EQ(5, result->_ranges[0].first);
    ASSERT_EQ(5, result->_ranges[0].second);
}

TEST_F(DocIdRangesReduceOptimizeTest, testParseArithmeticOp_argNotNumber) {
    navi::NaviLoggerProvider provider("TRACE2");
    string condStr = R"({"op":"=","params": [{"op":"+", "params":[2,3]}, "$k1"]})";
    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    const SimpleValue &leafCondition = dynamic_cast<LeafCondition *>(cond.get())->getCondition();
    string op(leafCondition[SQL_CONDITION_OPERATOR].GetString());
    const SimpleValue &param = leafCondition[SQL_CONDITION_PARAMETER];
    _optimize->parseArithmeticOp(param, op);

    ASSERT_EQ(0, _optimize->_key2keyRange.size());
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "args are not column, skip", traces);
}

TEST_F(DocIdRangesReduceOptimizeTest, testVisitLeafCondition_notContainUdf) {
    string condStr = R"({"op":"not_exist","params":["$attr1","1&2", "&"],"type":"UDF"})";
    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    auto *leafCondition = dynamic_cast<LeafCondition *>(cond.get());
    _optimize->visitLeafCondition(leafCondition);
    ASSERT_EQ(true, _optimize->_key2keyRange.empty());
}

TEST_F(DocIdRangesReduceOptimizeTest, testVisitLeafCondition) {
    // op  =
    string condStr = R"({"op":"=","params": ["$k1", 5]})";
    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    auto *leafCondition = dynamic_cast<LeafCondition *>(cond.get());
    _optimize->visitLeafCondition(leafCondition);
    ASSERT_EQ(1, _optimize->_key2keyRange.size());
}

TEST_F(DocIdRangesReduceOptimizeTest, testVisitAndCondition_01) {
    // k1 = 1 && k2 = 10
    string condStr1 = R"({"op":"=", "params":["$k1", 1]})";
    string condStr2 = R"({"op":"=", "params":["$k2", 10]})";
    string condStr = R"({"op" : "AND" , "params" : [)" + condStr1 + "," + condStr2 + "]}";

    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    auto *andCondition = dynamic_cast<AndCondition *>(cond.get());
    _optimize->visitAndCondition(andCondition);
    auto &key2keyRange = _optimize->_key2keyRange;

    ASSERT_EQ(2, key2keyRange.size());
    ASSERT_TRUE(key2keyRange.find("k1") != key2keyRange.end());
    ASSERT_TRUE(key2keyRange.find("k2") != key2keyRange.end());
    KeyRangeBasePtr keyRange1 = key2keyRange["k1"];
    KeyRangeBasePtr keyRange2 = key2keyRange["k2"];
    auto *result1 = dynamic_cast<KeyRangeTyped<int64_t> *>(keyRange1.get());
    auto *result2 = dynamic_cast<KeyRangeTyped<uint64_t> *>(keyRange2.get());
    ASSERT_TRUE(result1);
    ASSERT_TRUE(result2);
    ASSERT_EQ("k1", result1->_attrName);
    ASSERT_EQ(1, result1->_ranges.size());
    ASSERT_EQ(1, result1->_ranges[0].first);
    ASSERT_EQ(1, result1->_ranges[0].second);
    ASSERT_EQ("k2", result2->_attrName);
    ASSERT_EQ(1, result2->_ranges.size());
    ASSERT_EQ(10, result2->_ranges[0].first);
    ASSERT_EQ(10, result2->_ranges[0].second);
}

TEST_F(DocIdRangesReduceOptimizeTest, testVisitAndCondition_02) {
    // k1 > 1 && k2 = 10
    string condStr1 = R"({"op":">", "params":["$k1", 1]})";
    string condStr2 = R"({"op":"=", "params":["$k2", 10]})";
    string condStr = R"({"op" : "AND" , "params" : [)" + condStr1 + "," + condStr2 + "]}";

    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    auto *andCondition = dynamic_cast<AndCondition *>(cond.get());
    _optimize->visitAndCondition(andCondition);
    auto &key2keyRange = _optimize->_key2keyRange;

    ASSERT_EQ(2, key2keyRange.size());
    ASSERT_TRUE(key2keyRange.find("k1") != key2keyRange.end());
    ASSERT_TRUE(key2keyRange.find("k2") != key2keyRange.end());
    KeyRangeBasePtr keyRange1 = key2keyRange["k1"];
    KeyRangeBasePtr keyRange2 = key2keyRange["k2"];
    auto *result1 = dynamic_cast<KeyRangeTyped<int64_t> *>(keyRange1.get());
    auto *result2 = dynamic_cast<KeyRangeTyped<uint64_t> *>(keyRange2.get());
    ASSERT_TRUE(result1);
    ASSERT_TRUE(result2);
    ASSERT_EQ("k1", result1->_attrName);
    ASSERT_EQ(1, result1->_ranges.size());
    ASSERT_EQ(1, result1->_ranges[0].first);
    ASSERT_EQ(numeric_limits<int64_t>::max(), result1->_ranges[0].second);
    ASSERT_EQ("k2", result2->_attrName);
    ASSERT_EQ(1, result2->_ranges.size());
    ASSERT_EQ(10, result2->_ranges[0].first);
    ASSERT_EQ(10, result2->_ranges[0].second);
}

TEST_F(DocIdRangesReduceOptimizeTest, testVisitAndCondition_03) {
    // k1 > 1 && k2 < 10
    string condStr1 = R"({"op":">", "params":["$k1", 1]})";
    string condStr2 = R"({"op":"<", "params":["$k2", 10]})";
    string condStr = R"({"op" : "AND" , "params" : [)" + condStr1 + "," + condStr2 + "]}";

    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    auto *andCondition = dynamic_cast<AndCondition *>(cond.get());
    _optimize->visitAndCondition(andCondition);
    auto &key2keyRange = _optimize->_key2keyRange;

    ASSERT_EQ(2, key2keyRange.size());
    ASSERT_TRUE(key2keyRange.find("k1") != key2keyRange.end());
    ASSERT_TRUE(key2keyRange.find("k2") != key2keyRange.end());
    KeyRangeBasePtr keyRange1 = key2keyRange["k1"];
    KeyRangeBasePtr keyRange2 = key2keyRange["k2"];
    auto *result1 = dynamic_cast<KeyRangeTyped<int64_t> *>(keyRange1.get());
    auto *result2 = dynamic_cast<KeyRangeTyped<uint64_t> *>(keyRange2.get());
    ASSERT_TRUE(result1);
    ASSERT_TRUE(result2);
    ASSERT_EQ("k1", result1->_attrName);
    ASSERT_EQ(1, result1->_ranges.size());
    ASSERT_EQ(1, result1->_ranges[0].first);
    ASSERT_EQ(numeric_limits<int64_t>::max(), result1->_ranges[0].second);
    ASSERT_EQ("k2", result2->_attrName);
    ASSERT_EQ(1, result2->_ranges.size());
    ASSERT_EQ(numeric_limits<uint64_t>::min(), result2->_ranges[0].first);
    ASSERT_EQ(10, result2->_ranges[0].second);
}

TEST_F(DocIdRangesReduceOptimizeTest, testVisitAndCondition_04) {
    // k1 > 10 && k1 < 5
    string condStr1 = R"({"op":">", "params":["$k1", 10]})";
    string condStr2 = R"({"op":"<", "params":["$k1", 5]})";
    string condStr = R"({"op" : "AND" , "params" : [)" + condStr1 + "," + condStr2 + "]}";

    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    auto *andCondition = dynamic_cast<AndCondition *>(cond.get());
    _optimize->visitAndCondition(andCondition);
    auto &key2keyRange = _optimize->_key2keyRange;

    ASSERT_EQ(1, key2keyRange.size());
    ASSERT_TRUE(key2keyRange.find("k1") != key2keyRange.end());
    KeyRangeBasePtr keyRange1 = key2keyRange["k1"];
    auto *result1 = dynamic_cast<KeyRangeTyped<int64_t> *>(keyRange1.get());
    ASSERT_TRUE(result1);
    ASSERT_EQ("k1", result1->_attrName);
    ASSERT_TRUE(result1->_ranges.empty());
}

TEST_F(DocIdRangesReduceOptimizeTest, testVisitAndCondition_05) {
    // k1 > 1 && k1 = 10
    string condStr1 = R"({"op":">", "params":["$k1", 1]})";
    string condStr2 = R"({"op":"=", "params":["$k1", 10]})";
    string condStr = R"({"op" : "AND" , "params" : [)" + condStr1 + "," + condStr2 + "]}";

    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    auto *andCondition = dynamic_cast<AndCondition *>(cond.get());
    _optimize->visitAndCondition(andCondition);
    auto &key2keyRange = _optimize->_key2keyRange;

    ASSERT_EQ(1, key2keyRange.size());
    ASSERT_TRUE(key2keyRange.find("k1") != key2keyRange.end());
    KeyRangeBasePtr keyRange1 = key2keyRange["k1"];
    auto *result1 = dynamic_cast<KeyRangeTyped<int64_t> *>(keyRange1.get());
    ASSERT_TRUE(result1);
    ASSERT_EQ("k1", result1->_attrName);
    ASSERT_EQ(1, result1->_ranges.size());
    ASSERT_EQ(10, result1->_ranges[0].first);
    ASSERT_EQ(10, result1->_ranges[0].second);
}

TEST_F(DocIdRangesReduceOptimizeTest, testVisitAndCondition_06) {
    // k1 > 1 && k2 = 10 && k1 < 9
    string condStr1 = R"({"op":">", "params":["$k1", 1]})";
    string condStr2 = R"({"op":"=", "params":["$k2", 10]})";
    string condStr3 = R"({"op":"<", "params":["$k1", 9]})";
    string condStr4 = R"({"op" : "AND" , "params" : [)" + condStr1 + "," + condStr2 + "]}";
    string condStr = R"({"op" : "AND" , "params" : [)" + condStr3 + "," + condStr4 + "]}";

    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    auto *andCondition = dynamic_cast<AndCondition *>(cond.get());
    _optimize->visitAndCondition(andCondition);
    auto &key2keyRange = _optimize->_key2keyRange;

    ASSERT_EQ(2, key2keyRange.size());
    ASSERT_TRUE(key2keyRange.find("k1") != key2keyRange.end());
    ASSERT_TRUE(key2keyRange.find("k2") != key2keyRange.end());
    KeyRangeBasePtr keyRange1 = key2keyRange["k1"];
    KeyRangeBasePtr keyRange2 = key2keyRange["k2"];
    auto *result1 = dynamic_cast<KeyRangeTyped<int64_t> *>(keyRange1.get());
    auto *result2 = dynamic_cast<KeyRangeTyped<uint64_t> *>(keyRange2.get());
    ASSERT_TRUE(result1);
    ASSERT_TRUE(result2);
    ASSERT_EQ("k1", result1->_attrName);
    ASSERT_EQ(1, result1->_ranges.size());
    ASSERT_EQ(1, result1->_ranges[0].first);
    ASSERT_EQ(9, result1->_ranges[0].second);
    ASSERT_EQ("k2", result2->_attrName);
    ASSERT_EQ(1, result2->_ranges.size());
    ASSERT_EQ(10, result2->_ranges[0].first);
    ASSERT_EQ(10, result2->_ranges[0].second);
}

TEST_F(DocIdRangesReduceOptimizeTest, testVisitOrCondition_01) {
    // k1 = 1 || k1 = 5
    string condStr1 = R"({"op":"=", "params":["$k1", 1]})";
    string condStr2 = R"({"op":"=", "params":["$k1", 5]})";
    string condStr = R"({"op" : "OR" , "params" : [)" + condStr1 + "," + condStr2 + "]}";

    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    auto *orCondition = dynamic_cast<OrCondition *>(cond.get());
    _optimize->visitOrCondition(orCondition);
    auto &key2keyRange = _optimize->_key2keyRange;

    ASSERT_EQ(1, key2keyRange.size());
    ASSERT_TRUE(key2keyRange.find("k1") != key2keyRange.end());
    KeyRangeBasePtr keyRange1 = key2keyRange["k1"];
    auto *result1 = dynamic_cast<KeyRangeTyped<int64_t> *>(keyRange1.get());
    ASSERT_TRUE(result1);
    ASSERT_EQ("k1", result1->_attrName);
    ASSERT_EQ(2, result1->_ranges.size());
    ASSERT_EQ(1, result1->_ranges[0].first);
    ASSERT_EQ(1, result1->_ranges[0].second);
    ASSERT_EQ(5, result1->_ranges[1].first);
    ASSERT_EQ(5, result1->_ranges[1].second);
}

TEST_F(DocIdRangesReduceOptimizeTest, testVisitOrCondition_02) {
    // k1 = 1 || k1 > 5
    string condStr1 = R"({"op":"=", "params":["$k1", 1]})";
    string condStr2 = R"({"op":">", "params":["$k1", 5]})";
    string condStr = R"({"op" : "OR" , "params" : [)" + condStr1 + "," + condStr2 + "]}";

    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    auto *orCondition = dynamic_cast<OrCondition *>(cond.get());
    _optimize->visitOrCondition(orCondition);
    auto &key2keyRange = _optimize->_key2keyRange;

    ASSERT_EQ(1, key2keyRange.size());
    ASSERT_TRUE(key2keyRange.find("k1") != key2keyRange.end());
    KeyRangeBasePtr keyRange1 = key2keyRange["k1"];
    auto *result1 = dynamic_cast<KeyRangeTyped<int64_t> *>(keyRange1.get());
    ASSERT_TRUE(result1);
    ASSERT_EQ("k1", result1->_attrName);
    ASSERT_EQ(2, result1->_ranges.size());
    ASSERT_EQ(1, result1->_ranges[0].first);
    ASSERT_EQ(1, result1->_ranges[0].second);
    ASSERT_EQ(5, result1->_ranges[1].first);
    ASSERT_EQ(std::numeric_limits<int64_t>::max(), result1->_ranges[1].second);
}

TEST_F(DocIdRangesReduceOptimizeTest, testVisitOrCondition_03) {
    // k1 = 1 || k1 < 5
    string condStr1 = R"({"op":"=", "params":["$k1", 1]})";
    string condStr2 = R"({"op":"<", "params":["$k1", 5]})";
    string condStr = R"({"op" : "OR" , "params" : [)" + condStr1 + "," + condStr2 + "]}";

    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    auto *orCondition = dynamic_cast<OrCondition *>(cond.get());
    _optimize->visitOrCondition(orCondition);
    auto &key2keyRange = _optimize->_key2keyRange;

    ASSERT_EQ(1, key2keyRange.size());
    ASSERT_TRUE(key2keyRange.find("k1") != key2keyRange.end());
    KeyRangeBasePtr keyRange1 = key2keyRange["k1"];
    auto *result1 = dynamic_cast<KeyRangeTyped<int64_t> *>(keyRange1.get());
    ASSERT_TRUE(result1);
    ASSERT_EQ("k1", result1->_attrName);
    ASSERT_EQ(1, result1->_ranges.size());
    ASSERT_EQ(std::numeric_limits<int64_t>::min(), result1->_ranges[0].first);
    ASSERT_EQ(5, result1->_ranges[0].second);
}

TEST_F(DocIdRangesReduceOptimizeTest, testVisitOrCondition_04) {
    // k1 < 10 || k1 < 5
    string condStr1 = R"({"op":"<", "params":["$k1", 10]})";
    string condStr2 = R"({"op":"<", "params":["$k1", 5]})";
    string condStr = R"({"op" : "OR" , "params" : [)" + condStr1 + "," + condStr2 + "]}";

    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    auto *orCondition = dynamic_cast<OrCondition *>(cond.get());
    _optimize->visitOrCondition(orCondition);
    auto &key2keyRange = _optimize->_key2keyRange;

    ASSERT_EQ(1, key2keyRange.size());
    ASSERT_TRUE(key2keyRange.find("k1") != key2keyRange.end());
    KeyRangeBasePtr keyRange1 = key2keyRange["k1"];
    auto *result1 = dynamic_cast<KeyRangeTyped<int64_t> *>(keyRange1.get());
    ASSERT_TRUE(result1);
    ASSERT_EQ("k1", result1->_attrName);
    ASSERT_EQ(1, result1->_ranges.size());
    ASSERT_EQ(std::numeric_limits<int64_t>::min(), result1->_ranges[0].first);
    ASSERT_EQ(10, result1->_ranges[0].second);
}

TEST_F(DocIdRangesReduceOptimizeTest, testVisitOrCondition_05) {
    // k1 > 1 || k1 < 5
    string condStr1 = R"({"op":">", "params":["$k1", 1]})";
    string condStr2 = R"({"op":"<", "params":["$k1", 5]})";
    string condStr = R"({"op" : "OR" , "params" : [)" + condStr1 + "," + condStr2 + "]}";

    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    auto *orCondition = dynamic_cast<OrCondition *>(cond.get());
    _optimize->visitOrCondition(orCondition);
    auto &key2keyRange = _optimize->_key2keyRange;

    ASSERT_EQ(1, key2keyRange.size());
    ASSERT_TRUE(key2keyRange.find("k1") != key2keyRange.end());
    KeyRangeBasePtr keyRange1 = key2keyRange["k1"];
    auto *result1 = dynamic_cast<KeyRangeTyped<int64_t> *>(keyRange1.get());
    ASSERT_TRUE(result1);
    ASSERT_EQ("k1", result1->_attrName);
    ASSERT_EQ(1, result1->_ranges.size());
    ASSERT_EQ(std::numeric_limits<int64_t>::min(), result1->_ranges[0].first);
    ASSERT_EQ(std::numeric_limits<int64_t>::max(), result1->_ranges[0].second);
}

TEST_F(DocIdRangesReduceOptimizeTest, testVisitOrCondition_06) {
    {
        // k1 = 1 || k2 = 5
        string condStr1 = R"({"op":"=", "params":["$k1", 1]})";
        string condStr2 = R"({"op":"=", "params":["$k2", 5]})";
        string condStr = R"({"op" : "OR" , "params" : [)" + condStr1 + "," + condStr2 + "]}";

        ConditionParser parser;
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);

        auto *orCondition = dynamic_cast<OrCondition *>(cond.get());
        _optimize->visitOrCondition(orCondition);
        auto &key2keyRange = _optimize->_key2keyRange;

        ASSERT_TRUE(key2keyRange.empty());
    }
    {
        // k1 = 1 || k2 > 5
        string condStr1 = R"({"op":"=", "params":["$k1", 1]})";
        string condStr2 = R"({"op":">", "params":["$k2", 5]})";
        string condStr = R"({"op" : "OR" , "params" : [)" + condStr1 + "," + condStr2 + "]}";

        ConditionParser parser;
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);

        auto *orCondition = dynamic_cast<OrCondition *>(cond.get());
        _optimize->visitOrCondition(orCondition);
        auto &key2keyRange = _optimize->_key2keyRange;

        ASSERT_TRUE(key2keyRange.empty());
    }
    {
        // k1 < 1 || k2 > 5
        string condStr1 = R"({"op":"<", "params":["$k1", 1]})";
        string condStr2 = R"({"op":">", "params":["$k2", 5]})";
        string condStr = R"({"op" : "OR" , "params" : [)" + condStr1 + "," + condStr2 + "]}";

        ConditionParser parser;
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);

        auto *orCondition = dynamic_cast<OrCondition *>(cond.get());
        _optimize->visitOrCondition(orCondition);
        auto &key2keyRange = _optimize->_key2keyRange;

        ASSERT_TRUE(key2keyRange.empty());
    }
}

TEST_F(DocIdRangesReduceOptimizeTest, testVisitOrCondition_07) {
    // empty || k1 < 5
    string condStr1 = R"({"op":"not_exist","params":["$k1","1&2", "&"],"type":"UDF"})";
    string condStr2 = R"({"op":"<", "params":["$k1", 5]})";
    string condStr = R"({"op" : "OR" , "params" : [)" + condStr1 + "," + condStr2 + "]}";

    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    auto *orCondition = dynamic_cast<OrCondition *>(cond.get());
    _optimize->visitOrCondition(orCondition);
    auto &key2keyRange = _optimize->_key2keyRange;

    ASSERT_EQ(0, key2keyRange.size());
}

TEST_F(DocIdRangesReduceOptimizeTest, testVisitOrCondition_08) {
    // empty || empty
    string condStr1 = R"({"op":"not_exist","params":["$k1","1&2", "&"],"type":"UDF"})";
    string condStr2 = R"({"op":"not_exist","params":["$k1","1&2", "&"],"type":"UDF"})";
    string condStr = R"({"op" : "OR" , "params" : [)" + condStr1 + "," + condStr2 + "]}";

    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    auto *orCondition = dynamic_cast<OrCondition *>(cond.get());
    _optimize->visitOrCondition(orCondition);
    auto &key2keyRange = _optimize->_key2keyRange;

    ASSERT_EQ(0, key2keyRange.size());
}

TEST_F(DocIdRangesReduceOptimizeTest, testVisitOrCondition_09) {
    // k1 = 1 || k1 > 5 || k2 < 10 || k2 = 15
    string condStr1 = R"({"op":"=", "params":["$k1", 1]})";
    string condStr2 = R"({"op":">", "params":["$k1", 5]})";
    string condStr3 = R"({"op" : "OR" , "params" : [)" + condStr1 + "," + condStr2 + "]}";
    string condStr4 = R"({"op" : "<", "params" : ["$k2", 10]})";
    string condStr5 = R"({"op" : "=", "params" : ["$k2", 15]})";
    string condStr6 = R"({"op" : "OR" , "params" : [)" + condStr4 + "," + condStr5 + "]}";
    string condStr = R"({"op" : "OR" , "params" : [)" + condStr3 + "," + condStr6 + "]}";

    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    auto *orCondition = dynamic_cast<OrCondition *>(cond.get());
    _optimize->visitOrCondition(orCondition);
    auto &key2keyRange = _optimize->_key2keyRange;

    ASSERT_TRUE(key2keyRange.empty());
}

TEST_F(DocIdRangesReduceOptimizeTest, testComplicateCondition_01) {
    // k1 = 1 && (k2 = 5 || k2 > 10)
    string condStr1 = R"({"op":"=", "params":["$k1", 1]})";
    string condStr2 = R"({"op":"=", "params":["$k2", 5]})";
    string condStr3 = R"({"op" : ">", "params" : ["$k2", 10]})";
    string condStr4 = R"({"op" : "OR" , "params" : [)" + condStr2 + "," + condStr3 + "]}";
    string condStr = R"({"op" : "AND" , "params" : [)" + condStr1 + "," + condStr4 + "]}";

    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    auto *andCondition = dynamic_cast<AndCondition *>(cond.get());
    _optimize->visitAndCondition(andCondition);
    auto &key2keyRange = _optimize->_key2keyRange;

    ASSERT_EQ(2, key2keyRange.size());
    ASSERT_TRUE(key2keyRange.find("k1") != key2keyRange.end());
    ASSERT_TRUE(key2keyRange.find("k2") != key2keyRange.end());
    KeyRangeBasePtr keyRange1 = key2keyRange["k1"];
    KeyRangeBasePtr keyRange2 = key2keyRange["k2"];
    auto *result1 = dynamic_cast<KeyRangeTyped<int64_t> *>(keyRange1.get());
    auto *result2 = dynamic_cast<KeyRangeTyped<uint64_t> *>(keyRange2.get());
    ASSERT_TRUE(result1);
    ASSERT_TRUE(result2);
    ASSERT_EQ("k1", result1->_attrName);
    ASSERT_EQ("k2", result2->_attrName);
    ASSERT_EQ(1, result1->_ranges.size());
    ASSERT_EQ(1, result1->_ranges[0].first);
    ASSERT_EQ(1, result1->_ranges[0].second);

    ASSERT_EQ(2, result2->_ranges.size());
    ASSERT_EQ(5, result2->_ranges[0].first);
    ASSERT_EQ(5, result2->_ranges[0].second);
    ASSERT_EQ(10, result2->_ranges[1].first);
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(), result2->_ranges[1].second);
}

TEST_F(DocIdRangesReduceOptimizeTest, testComplicateCondition_02) {
    // (k1 = 1 || k1 < 5) && (k2 = 5 || k2 > 10)
    string condStr1 = R"({"op":"=", "params":["$k1", 1]})";
    string condStr2 = R"({"op":"<", "params":["$k1", 5]})";
    string condStr3 = R"({"op":"=", "params":["$k2", 5]})";
    string condStr4 = R"({"op" : ">", "params" : ["$k2", 10]})";
    string condStr5 = R"({"op" : "OR" , "params" : [)" + condStr1 + "," + condStr2 + "]}";
    string condStr6 = R"({"op" : "OR" , "params" : [)" + condStr3 + "," + condStr4 + "]}";
    string condStr = R"({"op" : "AND" , "params" : [)" + condStr5 + "," + condStr6 + "]}";

    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    auto *andCondition = dynamic_cast<AndCondition *>(cond.get());
    _optimize->visitAndCondition(andCondition);
    auto &key2keyRange = _optimize->_key2keyRange;

    ASSERT_EQ(2, key2keyRange.size());
    ASSERT_TRUE(key2keyRange.find("k1") != key2keyRange.end());
    ASSERT_TRUE(key2keyRange.find("k2") != key2keyRange.end());
    KeyRangeBasePtr keyRange1 = key2keyRange["k1"];
    KeyRangeBasePtr keyRange2 = key2keyRange["k2"];
    auto *result1 = dynamic_cast<KeyRangeTyped<int64_t> *>(keyRange1.get());
    auto *result2 = dynamic_cast<KeyRangeTyped<uint64_t> *>(keyRange2.get());
    ASSERT_TRUE(result1);
    ASSERT_TRUE(result2);
    ASSERT_EQ("k1", result1->_attrName);
    ASSERT_EQ("k2", result2->_attrName);

    ASSERT_EQ(1, result1->_ranges.size());
    ASSERT_EQ(std::numeric_limits<int64_t>::min(), result1->_ranges[0].first);
    ASSERT_EQ(5, result1->_ranges[0].second);

    ASSERT_EQ(2, result2->_ranges.size());
    ASSERT_EQ(5, result2->_ranges[0].first);
    ASSERT_EQ(5, result2->_ranges[0].second);
    ASSERT_EQ(10, result2->_ranges[1].first);
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(), result2->_ranges[1].second);
}

TEST_F(DocIdRangesReduceOptimizeTest, testComplicateCondition_03) {
    // (k1 = 1 || k1 < 5) && (k1 = 7 || k2 > 10)
    string condStr1 = R"({"op":"=", "params":["$k1", 1]})";
    string condStr2 = R"({"op":"<", "params":["$k1", 5]})";
    string condStr3 = R"({"op":"=", "params":["$k1", 7]})";
    string condStr4 = R"({"op" : ">", "params" : ["$k2", 10]})";
    string condStr5 = R"({"op" : "OR" , "params" : [)" + condStr1 + "," + condStr2 + "]}";
    string condStr6 = R"({"op" : "OR" , "params" : [)" + condStr3 + "," + condStr4 + "]}";
    string condStr = R"({"op" : "AND" , "params" : [)" + condStr5 + "," + condStr6 + "]}";

    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    auto *andCondition = dynamic_cast<AndCondition *>(cond.get());
    _optimize->visitAndCondition(andCondition);
    auto &key2keyRange = _optimize->_key2keyRange;

    ASSERT_EQ(1, key2keyRange.size());
    ASSERT_TRUE(key2keyRange.find("k1") != key2keyRange.end());
    KeyRangeBasePtr keyRange1 = key2keyRange["k1"];
    auto *result1 = dynamic_cast<KeyRangeTyped<int64_t> *>(keyRange1.get());
    ASSERT_TRUE(result1);
    ASSERT_EQ("k1", result1->_attrName);

    ASSERT_EQ(1, result1->_ranges.size());
    ASSERT_EQ(std::numeric_limits<int64_t>::min(), result1->_ranges[0].first);
    ASSERT_EQ(5, result1->_ranges[0].second);
}

TEST_F(DocIdRangesReduceOptimizeTest, testComplicateCondition_04) {
    // k2 = 1 && (k1 < 5 || k2 > 10)
    string condStr1 = R"({"op":"=", "params":["$k2", 1]})";
    string condStr2 = R"({"op":"<", "params":["$k1", 5]})";
    string condStr3 = R"({"op" : ">", "params" : ["$k2", 10]})";
    string condStr4 = R"({"op" : "OR" , "params" : [)" + condStr2 + "," + condStr3 + "]}";
    string condStr = R"({"op" : "AND" , "params" : [)" + condStr1 + "," + condStr4 + "]}";

    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    auto *andCondition = dynamic_cast<AndCondition *>(cond.get());
    _optimize->visitAndCondition(andCondition);
    auto &key2keyRange = _optimize->_key2keyRange;

    ASSERT_EQ(1, key2keyRange.size());
    ASSERT_TRUE(key2keyRange.find("k2") != key2keyRange.end());
    KeyRangeBasePtr keyRange1 = key2keyRange["k2"];
    auto *result1 = dynamic_cast<KeyRangeTyped<uint64_t> *>(keyRange1.get());
    ASSERT_TRUE(result1);
    ASSERT_EQ("k2", result1->_attrName);

    ASSERT_EQ(1, result1->_ranges.size());
    ASSERT_EQ(1, result1->_ranges[0].first);
    ASSERT_EQ(1, result1->_ranges[0].second);
}

TEST_F(DocIdRangesReduceOptimizeTest, testComplicateCondition_05) {
    // (k1 = 1 && k1 > 5) || k2 > 10
    string condStr1 = R"({"op":"=", "params":["$k1", 1]})";
    string condStr2 = R"({"op":">", "params":["$k1", 5]})";
    string condStr3 = R"({"op" : ">", "params" : ["$k2", 10]})";
    string condStr4 = R"({"op" : "AND" , "params" : [)" + condStr1 + "," + condStr2 + "]}";
    string condStr = R"({"op" : "OR" , "params" : [)" + condStr4 + "," + condStr3 + "]}";

    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    auto *orCondition = dynamic_cast<OrCondition *>(cond.get());
    _optimize->visitOrCondition(orCondition);
    auto &key2keyRange = _optimize->_key2keyRange;

    ASSERT_EQ(0, key2keyRange.size());
}

TEST_F(DocIdRangesReduceOptimizeTest, testComplicateCondition_06) {
    // (k1 = 1 && k1 > 5) || k1 > 10
    string condStr1 = R"({"op":"=", "params":["$k1", 1]})";
    string condStr2 = R"({"op":">", "params":["$k1", 5]})";
    string condStr3 = R"({"op" : ">", "params" : ["$k1", 10]})";
    string condStr4 = R"({"op" : "AND" , "params" : [)" + condStr1 + "," + condStr2 + "]}";
    string condStr = R"({"op" : "OR" , "params" : [)" + condStr4 + "," + condStr3 + "]}";

    ConditionParser parser;
    ConditionPtr cond;
    bool success = parser.parseCondition(condStr, cond);
    ASSERT_TRUE(success);

    auto *orCondition = dynamic_cast<OrCondition *>(cond.get());
    _optimize->visitOrCondition(orCondition);
    auto &key2keyRange = _optimize->_key2keyRange;

    ASSERT_EQ(1, key2keyRange.size());
    ASSERT_TRUE(key2keyRange.find("k1") != key2keyRange.end());
    KeyRangeBasePtr keyRange1 = key2keyRange["k1"];
    auto *result1 = dynamic_cast<KeyRangeTyped<int64_t> *>(keyRange1.get());
    ASSERT_TRUE(result1);
    ASSERT_EQ("k1", result1->_attrName);
    ASSERT_EQ(1, result1->_ranges.size());
    ASSERT_EQ(10, result1->_ranges[0].first);
    ASSERT_EQ(std::numeric_limits<int64_t>::max(), result1->_ranges[0].second);
}

TEST_F(DocIdRangesReduceOptimizeTest, testConvertDimens) {
    {
        _optimize->_key2keyRange = {};
        const auto &dimens = _optimize->convertDimens();
        ASSERT_TRUE(dimens.empty());
    }
    {
        KeyRangeTyped<int64_t> *k2(new KeyRangeTyped<int64_t>("k2"));
        k2->_ranges = {{1, 2}};
        _optimize->_key2keyRange = {{"k2", KeyRangeBasePtr(k2)}};
        const auto &dimens = _optimize->convertDimens();
        ASSERT_TRUE(dimens.empty());
    }
    {
        KeyRangeTyped<int64_t> *k1(new KeyRangeTyped<int64_t>("k1"));
        k1->_ranges = {{1, 20}};
        KeyRangeTyped<int64_t> *k2(new KeyRangeTyped<int64_t>("k2"));
        k2->_ranges = {{1, 2}};
        _optimize->_key2keyRange = {{"k1", KeyRangeBasePtr(k1)}, {"k2", KeyRangeBasePtr(k2)}};
        const auto &dimens = _optimize->convertDimens();
        ASSERT_EQ(1, dimens.size());
        ASSERT_EQ("k1", dimens[0]->name);
    }
    {
        KeyRangeTyped<int64_t> *k1(new KeyRangeTyped<int64_t>("k1"));
        k1->_ranges = {{1, 2}};
        KeyRangeTyped<int64_t> *k2(new KeyRangeTyped<int64_t>("k2"));
        k2->_ranges = {{1, 20}};
        _optimize->_key2keyRange = {{"k1", KeyRangeBasePtr(k1)}, {"k2", KeyRangeBasePtr(k2)}};
        const auto &dimens = _optimize->convertDimens();
        ASSERT_EQ(2, dimens.size());
        ASSERT_EQ("k1", dimens[0]->name);
        ASSERT_EQ("k2", dimens[1]->name);
    }
    {
        KeyRangeTyped<int64_t> *k1(new KeyRangeTyped<int64_t>("k1"));
        k1->_ranges = {{1, 2}};
        KeyRangeTyped<int64_t> *k3(new KeyRangeTyped<int64_t>("k3"));
        k3->_ranges = {{1, 20}};
        _optimize->_key2keyRange = {{"k1", KeyRangeBasePtr(k1)}, {"k3", KeyRangeBasePtr(k3)}};
        const auto &dimens = _optimize->convertDimens();
        ASSERT_EQ(1, dimens.size());
        ASSERT_EQ("k1", dimens[0]->name);
    }
}

TEST_F(DocIdRangesReduceOptimizeTest, testReduceDocIdRange) {
    {
        search::LayerMetaPtr layerMeta(new search::LayerMeta(_poolPtr.get()));
        layerMeta->push_back({0, 100, DocIdRangeMeta::OT_ORDERED});
        std::vector<indexlib::DocIdRangeVector> expected = {{{1, 5}}};
        search::IndexPartitionReaderWrapperPtr fakeReader(
            new FakeIndexPartitionReaderWrapper(expected));
        auto result = _optimize->reduceDocIdRange(layerMeta, _poolPtr.get(), fakeReader);
        ASSERT_EQ(1, result->size());
        ASSERT_EQ(1, (*result)[0].begin);
        ASSERT_EQ(4, (*result)[0].end);
    }
    {
        search::LayerMetaPtr layerMeta(new search::LayerMeta(_poolPtr.get()));
        layerMeta->push_back({0, 6, DocIdRangeMeta::OT_ORDERED});
        layerMeta->push_back({7, 100, DocIdRangeMeta::OT_ORDERED});
        std::vector<indexlib::DocIdRangeVector> expected = {{{1, 5}}, {{7, 9}, {10, 12}}};
        search::IndexPartitionReaderWrapperPtr fakeReader(
            new FakeIndexPartitionReaderWrapper(expected));
        auto result = _optimize->reduceDocIdRange(layerMeta, _poolPtr.get(), fakeReader);
        ASSERT_EQ(3, result->size());
        ASSERT_EQ(1, (*result)[0].begin);
        ASSERT_EQ(4, (*result)[0].end);
        ASSERT_EQ(7, (*result)[1].begin);
        ASSERT_EQ(8, (*result)[1].end);
        ASSERT_EQ(10, (*result)[2].begin);
        ASSERT_EQ(11, (*result)[2].end);
    }
    {
        search::LayerMetaPtr layerMeta(new search::LayerMeta(_poolPtr.get()));
        layerMeta->push_back({0, 6, DocIdRangeMeta::OT_ORDERED});
        layerMeta->push_back({100, 7, DocIdRangeMeta::OT_ORDERED});
        std::vector<indexlib::DocIdRangeVector> expected = {{{1, 5}}, {{7, 9}, {10, 12}}};
        search::IndexPartitionReaderWrapperPtr fakeReader(
            new FakeIndexPartitionReaderWrapper(expected));
        auto result = _optimize->reduceDocIdRange(layerMeta, _poolPtr.get(), fakeReader);
        ASSERT_EQ(1, result->size());
        ASSERT_EQ(1, (*result)[0].begin);
        ASSERT_EQ(4, (*result)[0].end);
    }
    {
        search::LayerMetaPtr layerMeta(new search::LayerMeta(_poolPtr.get()));
        layerMeta->push_back({0, 100, DocIdRangeMeta::OT_ORDERED});
        std::vector<indexlib::DocIdRangeVector> expected = {{{1, 5}}};
        search::IndexPartitionReaderWrapperPtr fakeReader(
            new FakeIndexPartitionReaderWrapper(expected, false));
        auto result = _optimize->reduceDocIdRange(layerMeta, _poolPtr.get(), fakeReader);
        ASSERT_EQ(1, result->size());
        ASSERT_EQ(0, (*result)[0].begin);
        ASSERT_EQ(100, (*result)[0].end);
    }
}

} // namespace sql
