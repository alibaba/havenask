#include "ha3/sql/ops/tvf/TvfFuncCreator.h"

using namespace std;

USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(sql);

HA3_LOG_SETUP(sql, TvfFuncCreator);

bool TvfFuncCreator::regTvfModels(iquan::TvfModels &tvfModels)
{
    for (auto pair: _sqlTvfProfileInfos) {
        iquan::TvfModel tvfModel;
        if (!generateTvfModel(tvfModel, pair.second)) {
            SQL_LOG(ERROR, "generate tvf model [%s] failed", pair.first.c_str());
            return false;
        }
        if (!initTvfModel(tvfModel, pair.second)) {
            SQL_LOG(ERROR, "init tvf model [%s] failed", pair.first.c_str());
            return false;
        }
        if (!checkTvfModel(tvfModel)) {
            SQL_LOG(ERROR, "tvf model [%s] invalid", pair.first.c_str());
            return false;
        }
        tvfModels.functions.push_back(tvfModel);
    }
    return true;
}

bool TvfFuncCreator::generateTvfModel(iquan::TvfModel &tvfModel,
                                      const HA3_NS(config)::SqlTvfProfileInfo &info)
{
    try {
        FromJsonString(tvfModel, _tvfDef);
    } catch(...) {
        SQL_LOG(ERROR, "register tvf models failed [%s].",
                _tvfDef.c_str());
        return false;
    }
    tvfModel.functionName = info.tvfName;
    return true;
}

bool TvfFuncCreator::checkTvfModel(const iquan::TvfModel& tvfModel) {
    if (tvfModel.functionContent.tvfs.size() != 1) {
        SQL_LOG(ERROR, "tvf [%s] func prototypes not equal 1.", tvfModel.functionName.c_str());
        return false;
    }
    const auto &tvfDef = tvfModel.functionContent.tvfs[0];
    for (const auto &paramType : tvfDef.params.scalars) {
        if (paramType.type != "string") {
            SQL_LOG(ERROR, "tvf [%s] func prototypes only support string type. now is [%s]",
                    tvfModel.functionName.c_str(), paramType.type.c_str());
            return false;
        }
    }
    return true;
}

END_HA3_NAMESPACE(sql)
