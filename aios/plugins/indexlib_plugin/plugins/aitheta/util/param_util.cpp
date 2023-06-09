/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * @file   ParamUtil.cpp
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Tue Jan 22 10:08:14 2019
 *
 * @brief
 *
 *
 */

#include "indexlib_plugin/plugins/aitheta/util/param_util.h"

#include "indexlib_plugin/plugins/aitheta/common_define.h"

using namespace std;
using namespace indexlib::util;
namespace indexlib {
namespace aitheta_plugin {

IE_LOG_SETUP(aitheta_plugin, ParamUtil);

ParamUtil::ParamUtil() {}

ParamUtil::~ParamUtil() {}

void ParamUtil::MergeParams(const KeyValueMap& srcParams, KeyValueMap& params, bool allowCover) {
    for (const auto &itr : srcParams) {
        const auto paramItr = params.find(itr.first);
        // 仅仅merge params中不存在的选项
        if (params.end() == paramItr) {
            params.insert(pair<string, string>(itr));
        } else if (allowCover) {
            params[itr.first] = itr.second;
        }
    }
}

}
}
