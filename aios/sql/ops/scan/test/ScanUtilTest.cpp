#include "sql/ops/scan/ScanUtil.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "autil/DefaultHashFunction.h"
#include "autil/RangeUtil.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/index/common/Types.h"
#include "sql/common/TableDistribution.h"
#include "sql/ops/scan/ScanInitParamR.h"
#include "sql/ops/scan/ScanR.h"
#include "sql/ops/scan/udf_to_query/ContainToQueryImpl.h"
#include "sql/resource/PartitionInfoR.h"
#include "suez/turing/expression/util/IndexInfos.h"
#include "table/Table.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;

namespace sql {

class ScanUtilTest : public TESTBASE {
public:
    ScanUtilTest() {
        _scanR._partitionInfoR = &_partitionInfoR;
    }
    ~ScanUtilTest() {}

public:
    void buildIndexInfo(suez::turing::IndexInfo *info,
                        ::InvertedIndexType pkType,
                        const std::string &field = "",
                        const std::string &index = "") {
        info->indexType = pkType;
        info->fieldName = field;
        info->setIndexName(index.c_str());
    }

public:
    ScanInitParamR _param;
    PartitionInfoR _partitionInfoR;
    ScanR _scanR;
};

TEST_F(ScanUtilTest, testFilterPksByParallel) {
    vector<string> pks = {"1", "2", "123"};
    _param.parallelNum = 2;
    _param.parallelIndex = 0;
    ScanUtil::filterPksByParallel(_param, pks);
    vector<string> expect = {"1", "123"};
    ASSERT_EQ(expect.size(), pks.size());
    ASSERT_EQ(StringUtil::toString(expect), StringUtil::toString(pks));
}

TEST_F(ScanUtilTest, testFilterPksByParam) {
    vector<string> pks = {"1", "2", "123"};
    suez::turing::IndexInfo indexInfo;
    _param.hashFields = {"id"};
    _param.tableDist.partCnt = 1;
    _param.hashType = "HASH";
    _param.parallelNum = 2;
    _param.parallelIndex = 0;
    buildIndexInfo(&indexInfo, it_primarykey128, "id");
    PartitionRange &range = _partitionInfoR._range;
    range.first = 0;
    range.second = 1;
    ScanUtil::filterPksByParam(&_scanR, _param, &indexInfo, pks);
    vector<string> expect = {"1", "123"};
    ASSERT_EQ(expect.size(), pks.size());
    ASSERT_EQ(StringUtil::toString(expect), StringUtil::toString(pks));
}

TEST_F(ScanUtilTest, testFilterPksByHashRangeWithBroadCastTable) {
    vector<string> pks = {"1", "2", "123"};
    suez::turing::IndexInfo indexInfo;
    _param.hashFields = {"id"};
    _param.tableDist.partCnt = 1;
    _param.hashType = "HASH";
    buildIndexInfo(&indexInfo, it_primarykey128, "id");
    PartitionRange &range = _partitionInfoR._range;
    range.first = 0;
    range.second = 1;
    ScanUtil::filterPksByHashRange(&_scanR, _param, &indexInfo, pks);
    vector<string> expect = {"1", "2", "123"};
    ASSERT_EQ(StringUtil::toString(expect), StringUtil::toString(pks));
}

TEST_F(ScanUtilTest, testFilterPksByHashRangeWithPkAsHashField) {
    vector<string> rawPks = {"1", "2", "123"};
    suez::turing::IndexInfo indexInfo;
    {
        _param.hashFields = {"id"};
        _param.hashType = "HASH";
        buildIndexInfo(&indexInfo, it_primarykey128, "id");
    }
    PartitionRange &range = _partitionInfoR._range;
    {
        range.first = 0;
        range.second = 65535;
        vector<string> pks = rawPks;
        ScanUtil::filterPksByHashRange(&_scanR, _param, &indexInfo, pks);
        vector<string> expect = {"1", "2", "123"};
        ASSERT_EQ(StringUtil::toString(expect), StringUtil::toString(pks));
    }
    {
        rawPks.clear();
        for (size_t i = 0; i < 10000; ++i) {
            rawPks.emplace_back(autil::StringUtil::toString(i));
        }
        size_t limit = 65535;
        size_t partSize = 16384;
        for (size_t from = 0; from <= limit; from += partSize) {
            size_t to = from + partSize - 1;
            range.first = from;
            range.second = to;
            DefaultHashFunction hashFunc("HASH", 65536);
            vector<string> expect;
            for (size_t i = 0; i < rawPks.size(); ++i) {
                auto hashId = hashFunc.getHashId(rawPks[i]);
                if (from <= hashId && hashId <= to) {
                    expect.push_back(rawPks[i]);
                }
            }
            vector<string> pks = rawPks;
            ScanUtil::filterPksByHashRange(&_scanR, _param, &indexInfo, pks);
            ASSERT_EQ(StringUtil::toString(expect), StringUtil::toString(pks));
        }
    }
}

TEST_F(ScanUtilTest, testFilterPksByHashRangeWithoutPkAsHashField) {
    vector<string> rawPks = {"1", "2", "123"};
    vector<string> pks = rawPks;
    suez::turing::IndexInfo indexInfo;
    _param.hashFields = {"attr1"};
    _param.hashType = "HASH";
    buildIndexInfo(&indexInfo, it_primarykey128, "id");
    PartitionRange &range = _partitionInfoR._range;
    range.first = 0;
    range.second = 0;
    ScanUtil::filterPksByHashRange(&_scanR, _param, &indexInfo, pks);
    vector<string> expect = {"1", "2", "123"};
    ASSERT_EQ(StringUtil::toString(expect), StringUtil::toString(pks));
}

TEST_F(ScanUtilTest, testIndexTypeToString) {
    ASSERT_EQ("text", ContainToQueryImpl::indexTypeToString(it_text));
    ASSERT_EQ("number", ContainToQueryImpl::indexTypeToString(it_number_uint8));
    ASSERT_EQ("pk", ContainToQueryImpl::indexTypeToString(it_primarykey64));
    ASSERT_EQ("unknown", ContainToQueryImpl::indexTypeToString(999));
}

TEST_F(ScanUtilTest, testCreateEmptyTable) {
    {
        vector<string> outputFileds
            = {"double_field", "string_field", "multi_double_field", "multi_string_field"};
        vector<string> outputFiledsType = {"DOUBLE", "CHAR", "ARRAY(DOUBLE)", "ARRAY(CHAR)"};
        auto pool = std::make_shared<autil::mem_pool::Pool>();
        auto table = ScanUtil::createEmptyTable(outputFileds, outputFiledsType, pool);
        ASSERT_TRUE(table);
        ASSERT_EQ(4, table->getColumnCount());
        ASSERT_EQ(0, table->getRowCount());
    }
    {
        vector<string> outputFileds
            = {"double_field", "string_field", "multi_double_field", "multi_string_field"};
        vector<string> outputFiledsType = {};
        auto pool = std::make_shared<autil::mem_pool::Pool>();
        auto table = ScanUtil::createEmptyTable(outputFileds, outputFiledsType, pool);
        ASSERT_FALSE(table);
    }
    {
        vector<string> outputFileds
            = {"double_field", "string_field", "multi_double_field", "multi_string_field"};
        vector<string> outputFiledsType = {"DOUBLE", "CHAR", "ARRAY(DOUBLE)", "ARRAY(CHAR)"};
        auto table = ScanUtil::createEmptyTable(outputFileds, outputFiledsType, nullptr);
        ASSERT_FALSE(table);
    }
}

} // namespace sql
