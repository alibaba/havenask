/**
 * @file   ParamUtil.cpp
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Tue Jan 22 10:08:14 2019
 *
 * @brief
 *
 *
 */

#include "param_util.h"

#include "aitheta_indexer/plugins/aitheta/common_define.h"

using namespace std;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(aitheta_plugin);

IE_LOG_SETUP(aitheta_plugin, ParamUtil);

ParamUtil::ParamUtil() {}

ParamUtil::~ParamUtil() {}

void ParamUtil::MergeParams(const KeyValueMap& srcParams, KeyValueMap& params, bool allowCover) {
    for (const auto itr : srcParams) {
        const auto paramItr = params.find(itr.first);
        // 仅仅merge params中不存在的选项
        if (params.end() == paramItr) {
            params.insert(pair<string, string>(itr));
        } else if (allowCover) {
            params[itr.first] = itr.second;
        }
    }
}

IE_NAMESPACE_END(aitheta_plugin);
