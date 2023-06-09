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
#include <iosfwd>
#include <string>

#include "autil/EnvUtil.h"
#include "ha3/turing/searcher/SearcherServiceImpl.h"
#include "ha3/turing/qrs/QrsServiceImpl.h"
#include "suez/sdk/CmdLineDefine.h"
#include "suez/sdk/SearchManager.h"
#include "suez_navi/search/NaviSearchManager.h"

using namespace std;
namespace suez {
static string QRS_ROLE_TYPE = "qrs";
static string SEARCHER_ROLE_TYPE = "searcher";
static string SUEZ_NAVI_ROLE_TYPE = "suez_navi";

SearchManager *createSearchManager() {
    string roleType = autil::EnvUtil::getEnv(ROLE_TYPE, "");
    if (roleType == QRS_ROLE_TYPE) {
        return new isearch::turing::QrsServiceImpl();
    } else if (roleType == SEARCHER_ROLE_TYPE) {
        return new isearch::turing::SearcherServiceImpl();
    } else if (roleType == SUEZ_NAVI_ROLE_TYPE) {
        return new suez_navi::NaviSearchManager();
    } else {
        return nullptr;
    }
}
} // namespace suez
