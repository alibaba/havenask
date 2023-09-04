#include "sql/ops/scan/udf_to_query/SpUdfToQuery.h"

#include <cstddef>
#include <memory>
#include <string>

#include "autil/legacy/RapidJsonCommon.h"
#include "ha3/common/Query.h"
#include "ha3/turing/common/ModelConfig.h"
#include "sql/ops/scan/test/ScanConditionTestUtil.h"
#include "sql/ops/scan/udf_to_query/UdfToQuery.h"
#include "unittest/unittest.h"

using namespace std;
using namespace suez::turing;

using namespace isearch::common;
using namespace isearch::turing;

namespace sql {
class SpUdfToQueryTest : public TESTBASE {
public:
    SpUdfToQueryTest();
    ~SpUdfToQueryTest();

public:
    void setUp();
    void tearDown();

private:
    SpUdfToQuery _spUdfToQuery;
    ScanConditionTestUtil _scanConditionTestUtil;
    std::shared_ptr<UdfToQueryParam> _param;
};

SpUdfToQueryTest::SpUdfToQueryTest() {}

SpUdfToQueryTest::~SpUdfToQueryTest() {}

void SpUdfToQueryTest::setUp() {
    _scanConditionTestUtil.init(GET_TEMPLATE_DATA_PATH(), GET_TEST_DATA_PATH());
    _param = _scanConditionTestUtil.createUdfToQueryParam();
}

void SpUdfToQueryTest::tearDown() {}

TEST_F(SpUdfToQueryTest, testSpToQuery_valid) {
    // valid sp query
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"sp_query_match\", "
                     "\"params\":[\"(+(attr2:1|name:test)+index_2:index)\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_spUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query != NULL);
}

TEST_F(SpUdfToQueryTest, testSpToQuery_invalid) {
    // invalid sp query
    autil::SimpleDocument simpleDoc;
    string condStr = "{\"op\":\"sp_query_match\", \"params\":[\"123456\"],\"type\":\"UDF\"}";
    simpleDoc.Parse(condStr.c_str());
    ASSERT_FALSE(simpleDoc.HasParseError());
    QueryPtr query(_spUdfToQuery.toQuery(simpleDoc, *_param));
    ASSERT_TRUE(query == NULL);
}

} // namespace sql
