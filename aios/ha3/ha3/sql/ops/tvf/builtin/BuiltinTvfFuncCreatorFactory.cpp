#include <ha3/sql/ops/tvf/builtin/BuiltinTvfFuncCreatorFactory.h>
#include <ha3/sql/ops/tvf/builtin/UnPackMultiValueTvfFunc.h>
#include <ha3/sql/ops/tvf/builtin/PrintTableTvfFunc.h>
#include <ha3/sql/ops/tvf/builtin/GraphSearchTvfFunc.h>
#include <ha3/sql/ops/tvf/builtin/RankTvfFunc.h>
#include <ha3/sql/ops/tvf/builtin/SortTvfFunc.h>
#include <ha3/sql/ops/tvf/builtin/EnableShuffleTvfFunc.h>
#include <ha3/sql/ops/tvf/builtin/InputTableTvfFunc.h>
#include <ha3/sql/ops/tvf/builtin/DistinctTopNTvfFunc.h>

using namespace std;
BEGIN_HA3_NAMESPACE(sql);

AUTIL_LOG_SETUP(sql, BuiltinTvfFuncCreatorFactory);

BuiltinTvfFuncCreatorFactory::BuiltinTvfFuncCreatorFactory() {
}

BuiltinTvfFuncCreatorFactory::~BuiltinTvfFuncCreatorFactory() {
}

bool BuiltinTvfFuncCreatorFactory::registerTvfFunctions() {
    REGISTER_TVF_FUNC_CREATOR("unpackMultiValue", UnPackMultiValueTvfFuncCreator);
    REGISTER_TVF_FUNC_CREATOR("printTableTvf", PrintTableTvfFuncCreator);
    REGISTER_TVF_FUNC_CREATOR("graphSearchTvf", GraphSearchTvfFuncCreator);
    REGISTER_TVF_FUNC_CREATOR("rankTvf", RankTvfFuncCreator);
    REGISTER_TVF_FUNC_CREATOR("sortTvf", SortTvfFuncCreator);
    REGISTER_TVF_FUNC_CREATOR("enableShuffleTvf", EnableShuffleTvfFuncCreator);
    REGISTER_TVF_FUNC_CREATOR("inputTableTvf", InputTableTvfFuncCreator);
    REGISTER_TVF_FUNC_CREATOR("distinctTopNTvf", DistinctTopNTvfFuncCreator);
    // REGISTER_TVF_FUNC_CREATOR(SummaryTvfFuncCreator);
    return true;
}

END_HA3_NAMESPACE(sql);
