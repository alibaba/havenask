#include <ha3_sdk/testlib/func_expression/FunctionTestHelper.h>
#include <ha3_sdk/testlib/common/MatchDocCreator.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(func_expression);
HA3_LOG_SETUP(function, FunctionTestHelper);

FunctionTestHelper::FunctionTestHelper() {
    init();
}

FunctionTestHelper::~FunctionTestHelper() {
    clear();
}

void FunctionTestHelper::init() {
    _pool = new Pool;
    _mdAllocator = new Ha3MatchDocAllocator(_pool);
    _dataProvider = new DataProvider();
    _funcProvider = new FunctionProvider(FunctionResource());
    _attrExprMaker = new FakeAttributeExpressionMaker(_pool);
}

void FunctionTestHelper::clear() {
    DELETE_AND_SET_NULL(_attrExprMaker);
    DELETE_AND_SET_NULL(_dataProvider);
    DELETE_AND_SET_NULL(_funcProvider);
    DELETE_AND_SET_NULL(_mdAllocator);
    DELETE_AND_SET_NULL(_pool);
}

void FunctionTestHelper::allocateMatchDocs(const string &docIdStr,
        vector<matchdoc::MatchDoc> &matchDocs)
{
    MatchDocCreator::allocateMatchDocs(docIdStr, matchDocs, _mdAllocator);
}

void FunctionTestHelper::deallocateMatchDocs(const vector<matchdoc::MatchDoc> &matchDocs)
{
    MatchDocCreator::deallocateMatchDocs(matchDocs, _mdAllocator);
}

END_HA3_NAMESPACE(func_expression);
