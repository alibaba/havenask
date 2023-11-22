#include "sql/ops/tableSplit/TableSplit.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "autil/HashFunctionBase.h"
#include "autil/legacy/exception.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Trait.h"
#include "sql/common/TableDistribution.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "table/test/TableTestUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace table;

namespace sql {

class TableSplitTest : public TESTBASE {
public:
    TableDistribution invalidDist() {
        return makeDist(R"json({
            "hash_mode": {
            },
            "partition_cnt" : 4
        })json");
    }

    TableDistribution singleDist() {
        return makeDist(R"json({
            "hash_mode": {
                "hash_function": "HASH",
                "hash_field": "$id"
            },
            "partition_cnt" : 1
        })json");
    }

    TableDistribution oneFieldSplitDist(const string &field) {
        auto dist = makeDist(R"json({
            "hash_mode": {
                "hash_function": "HASH",
                "hash_field": "$id"
            },
            "partition_cnt" : 4
        })json");
        dist.hashMode._hashField = field;
        dist.hashMode._hashFields = {field};
        return dist;
    }

    TableDistribution twoFieldRoutingDist(const string &field1, const string &field2) {
        auto dist = makeDist(R"json({
            "hash_mode" : {
                "hash_fields" : [ "$batch", "$id" ],
                "hash_function" : "ROUTING_HASH",
                "hash_params" : {
                    "hash_func" : "HASH",
                    "hot_values" : "2,3",
                    "hot_values_ratio" : "0.25",
                    "routing_ratio" : "0"
                }
            },
            "partition_cnt" : 4
        })json");
        dist.hashMode._hashFields = {field1, field2};
        return dist;
    }

    TablePtr fakeTable1() {
        MatchDocAllocatorPtr allocator;
        const auto &docs = _util.createMatchDocs(allocator, 8);
        _util.extendMatchDocAllocator<int64_t>(allocator, docs, "id", {0, 1, 2, 3, 4, 5, 6, 7});
        return Table::fromMatchDocs(docs, allocator);
    }

    TablePtr fakeTable2() {
        MatchDocAllocatorPtr allocator;
        const auto &docs = _util.createMatchDocs(allocator, 8);
        _util.extendMatchDocAllocator<int64_t>(allocator, docs, "id", {0, 1, 2, 3, 4, 5, 6, 7});
        _util.extendMatchDocAllocator<int64_t>(allocator, docs, "batch", {0, 0, 1, 1, 2, 2, 3, 3});
        return Table::fromMatchDocs(docs, allocator);
    }

    TableDistribution bizOrderSellerDist() {
        return makeDist(R"json({
            "hash_mode":{
                "hash_fields":["$seller_id","$biz_order_id"],
                "hash_function":"ROUTING_HASH",
                "hash_params":{
                    "routing_ratio":"0.02",
                    "hot_values":"2832360567,2680068332;2201271538470,2935370446,2634283647,619123122,3549840132,725677994,2207644760486;2549841410,1776456424,3937219703,2508852773,3935526498,3935007972,3935417197,2346617193,2186520681,1825191249,809107100,1904163228,3472039790,392147177,2207704277426,2614256607,2274485302,835231129,1065192270,2210334007368;2816408103,3918883988,2928278102,2373391030,2200784241956,3170267126,2160051554,1018514559,2616970884,3129008056,2978398582,3039519252,2203660013,106852162,1730713672,2364017372,217101303,2211427111538,352802234,732501769,321333242,1764043733,787311414,2119886126,2209041379559,2979386326,2201101921646,2094336397,196993935,2095455386,533497499,2206603592034,2658963920,2994593924,2298281240,3175282461,2759121048,2155902607;3064652855,238074758,3914850390,3925342844,3221796641,2082098619,2200657724895,190903296,407675891,3528106876,2929053804,1962852535,1714128138,3424280638,2204103009798,3164246188,1049653664,2619709162,2207525808447,1771975008,385132127,134363478,938691334,859515618,880734502,1612901358,2773692553,430490406,3548027723,2891651035,1994132278,2275046294,925772082,2360209412,2206384590783,2509849149,2200757237600,1730458062,1910161129,749391658,92688455,1006402840,890482188,2064892827,46034682,411329340,2104668892,2207497636963,3027700871,3375170974,603671336,2687866620,2735822823,1677292491,1879183449,2206593042565,570031615,2068983963,353571709,349740505,2200606797002,137115748,397341302,2926585976,2975747228,1627473180,845001562,4260076097,3837691883,2219509495,2605193237,236931166,2081314055,2200800525598,906018622,3782501612,363607599,2200645086695,3899027575,3012860579,2200657724932,1611265871,2966805740,2794371653,2208897144183,698461607,3231887644,446338500,1020227772,642320867,795109648,1810383795,201749140,2631942246,628189716,3164711246,2966735737,725827827,2211535999146,2984496242,2883984979,143584903,2255775604,101450072,379424083,2200799949596,1652864050,3704749792,2646612614,3481427467,2259702327,743750137,3157354417,475108112,1736598751,2210195254848,3295853809,2927925573,750756158,2176028867,2206593044809,2436848051,2830036684,700459267,2736841214,793375733,2032045929,2207559659308,1102000075,1917047079,3170729146,470168984,4144020062,3035493001;2940753275,2635083319,2200798406814,2263306098,1710482305,1652554937,366268061,2081948193,277627577,1944046363,3369266294,892621993,364914988,2844713590,2815374617,3405569954,2207506995490,2212949550656,2206610105495,2526051849,3346148564,726212202,2782420494,2927210366,2065617942,2206445356761,849090736,2911453676,4091091457,2202135510,407910984,2528254892,2838892713,352469034,2424338511,2999761311,3981784227,2203600174,1835946814,2194740761,247777394,676606897,2201110857290,734211748,1114511827,2927731773,2462152977,1741393998,2146742267,2200717121882,826207123,2937365488,739643635,509142617,2201173399681,420567757,2744696515,2206638927335,1879194783,2206736426581,2206600725623,288902762,4128631814,356060330,2201161815285,2587036732,604477464,3626596873,2532208428,4120449433,354550048,2857903654,2258006982,519286239,3437510533,3037817444,2224996251,2206610273653,2170746564,3392536705,2207496786086,305358018,2456877352,4033652247,2206593087688,2781172367,3423372923,2931172601,4101083861,354300146,303439955,3914367079,2843869674,2691044240,2129855716,2089252756,4263377845,2030008797,2549449826,50983440,2091012777,2181779871,921828131,2159616682,849441921,1813033576,1023528406,3699547347,1746960915,2206692357221,885684871,2203090716636,2568052038,2200796645823,374544688,1910885287,2982483029,3520028609,2631960033,2206643867466,749311050,2913908707,2200789800321,3527212490,2206625891430,2257482711,1622630691,2165616006,2957921311,2207618701749,2721013369,2917137261,2603093836,2200654384845,2746434747,2207594307163,2160137792,1857230007,10550225,2207486045360,730053232,1664945146,2200657715182,1105025069,458694874,2455164953,820521956,1818112088,734584252,2188945577,1954675918,2956756848,2807304908,2135832463,4132990577,916243692,4018746651,520557274,713464357,2203546645546,2786278078,2202825144761,3357241566,2207536914466,485735498,2200590176338,2541315686,2206618226151,2200789451934,2201174591381,152579056,2206703718408,4050418704,2895810697,1023696028,2058664592,805439789,3243743397,667286523",
                    "hot_values_ratio":"1;0.5;0.25;0.125;0.06;0.04",
                    "hash_func":"HASH"
                }
            },
            "partition_cnt":256,
            "type":"HASH_DISTRIBUTED"
        })json");
    }

    TablePtr fakeBizOrderSellerTable1() {
        MatchDocAllocatorPtr allocator;
        const auto &docs = _util.createMatchDocs(allocator, 1);
        _util.extendMatchDocAllocator<int64_t>(
            allocator, docs, "biz_order_id", {3276173988120935717});
        _util.extendMatchDocAllocator<int64_t>(allocator, docs, "seller_id", {2680068332});
        return Table::fromMatchDocs(docs, allocator);
    }

    TableDistribution makeDist(const std::string &json) {
        TableDistribution dist;
        try {
            FastFromJsonString(dist, json);
        } catch (legacy::ExceptionBase &e) {}
        return dist;
    }

    TablePtr makePartTable(Table &table, const vector<Row> &rows) {
        return Table::fromMatchDocs(rows, table::Table::toMatchDocs(table));
    }

protected:
    TableSplit _tableSplit;
    vector<vector<Row>> _partRows;
    MatchDocUtil _util;
};

TEST_F(TableSplitTest, testInvalidDist) {
    _tableSplit.init(invalidDist());
    auto table = fakeTable1();
    ASSERT_FALSE(_tableSplit.split(*table, _partRows));
    ASSERT_TRUE(_partRows.empty());
}

TEST_F(TableSplitTest, testSingleDist) {
    _tableSplit.init(singleDist());
    auto table = fakeTable1();
    ASSERT_FALSE(_tableSplit.split(*table, _partRows));
    ASSERT_TRUE(_partRows.empty());
}

TEST_F(TableSplitTest, testOneFieldSplitDist) {
    _tableSplit.init(oneFieldSplitDist("id"));
    auto table = fakeTable1();
    ASSERT_TRUE(_tableSplit.split(*table, _partRows));
    ASSERT_EQ(4, _partRows.size());
    {
        auto partTable = makePartTable(*table, _partRows[0]);
        EXPECT_NO_FATAL_FAILURE(
            TableTestUtil::checkOutputColumn<int64_t>(partTable, "id", {1, 3, 4}));
    }
    {
        auto partTable = makePartTable(*table, _partRows[1]);
        EXPECT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int64_t>(partTable, "id", {0}));
    }
    {
        auto partTable = makePartTable(*table, _partRows[2]);
        EXPECT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int64_t>(partTable, "id", {2, 5}));
    }
    {
        auto partTable = makePartTable(*table, _partRows[3]);
        EXPECT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int64_t>(partTable, "id", {6, 7}));
    }
}

TEST_F(TableSplitTest, testOneFieldSplitMissingField) {
    _tableSplit.init(oneFieldSplitDist("not_exist"));
    auto table = fakeTable1();
    ASSERT_FALSE(_tableSplit.split(*table, _partRows));
    ASSERT_TRUE(_partRows.empty());
}

TEST_F(TableSplitTest, testTwoFieldRoutingDistPartial) {
    _tableSplit.init(twoFieldRoutingDist("id", "not_exist"));
    auto table = fakeTable1();
    ASSERT_TRUE(_tableSplit.split(*table, _partRows));
    ASSERT_EQ(4, _partRows.size());
    {
        auto partTable = makePartTable(*table, _partRows[0]);
        EXPECT_NO_FATAL_FAILURE(
            TableTestUtil::checkOutputColumn<int64_t>(partTable, "id", {1, 3, 4}));
    }
    {
        auto partTable = makePartTable(*table, _partRows[1]);
        EXPECT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int64_t>(partTable, "id", {0, 3}));
    }
    {
        auto partTable = makePartTable(*table, _partRows[2]);
        EXPECT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int64_t>(partTable, "id", {2, 5}));
    }
    {
        auto partTable = makePartTable(*table, _partRows[3]);
        EXPECT_NO_FATAL_FAILURE(
            TableTestUtil::checkOutputColumn<int64_t>(partTable, "id", {2, 6, 7}));
    }
}

TEST_F(TableSplitTest, testTwoFieldRoutingDist) {
    _tableSplit.init(twoFieldRoutingDist("batch", "id"));
    auto table = fakeTable2();
    ASSERT_TRUE(_tableSplit.split(*table, _partRows));
    ASSERT_EQ(4, _partRows.size());
    {
        auto partTable = makePartTable(*table, _partRows[0]);
        EXPECT_NO_FATAL_FAILURE(
            TableTestUtil::checkOutputColumn<int64_t>(partTable, "batch", {1, 1, 3, 3}));
        EXPECT_NO_FATAL_FAILURE(
            TableTestUtil::checkOutputColumn<int64_t>(partTable, "id", {2, 3, 6, 7}));
    }
    {
        auto partTable = makePartTable(*table, _partRows[1]);
        EXPECT_NO_FATAL_FAILURE(
            TableTestUtil::checkOutputColumn<int64_t>(partTable, "batch", {0, 0, 3, 3}));
        EXPECT_NO_FATAL_FAILURE(
            TableTestUtil::checkOutputColumn<int64_t>(partTable, "id", {0, 1, 6, 7}));
    }
    {
        auto partTable = makePartTable(*table, _partRows[2]);
        EXPECT_NO_FATAL_FAILURE(
            TableTestUtil::checkOutputColumn<int64_t>(partTable, "batch", {2, 2}));
        EXPECT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int64_t>(partTable, "id", {4, 5}));
    }
    {
        auto partTable = makePartTable(*table, _partRows[3]);
        EXPECT_NO_FATAL_FAILURE(
            TableTestUtil::checkOutputColumn<int64_t>(partTable, "batch", {2, 2}));
        EXPECT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int64_t>(partTable, "id", {4, 5}));
    }
}

TEST_F(TableSplitTest, testTwoFieldRoutingDistMissingField) {
    _tableSplit.init(twoFieldRoutingDist("not_exist", "id"));
    auto table = fakeTable1();
    ASSERT_FALSE(_tableSplit.split(*table, _partRows));
    ASSERT_TRUE(_partRows.empty());
}

TEST_F(TableSplitTest, testBizOrderSellerBadcase) {
    _tableSplit.init(bizOrderSellerDist());
    auto table = fakeBizOrderSellerTable1();
    ASSERT_TRUE(_tableSplit.split(*table, _partRows));
    const size_t expectPartCount = 256;
    ASSERT_EQ(expectPartCount, _partRows.size());
    for (size_t i = 0; i < expectPartCount; ++i) {
        auto partTable = makePartTable(*table, _partRows[i]);
        EXPECT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int64_t>(
            partTable, "biz_order_id", {3276173988120935717}))
            << "part " << i;
        EXPECT_NO_FATAL_FAILURE(
            TableTestUtil::checkOutputColumn<int64_t>(partTable, "seller_id", {2680068332}))
            << "part " << i;
    }
}

} // namespace sql
