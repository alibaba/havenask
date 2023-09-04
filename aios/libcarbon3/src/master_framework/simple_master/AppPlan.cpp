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
#include "master_framework/AppPlan.h"
#include "common/Log.h"

using namespace hippo;
using namespace std;
using namespace autil::legacy;

BEGIN_MASTER_FRAMEWORK_NAMESPACE(simple_master);

MASTER_FRAMEWORK_LOG_SETUP(simple_master, AppPlan);

AppPlan::AppPlan() {
}

AppPlan::~AppPlan() {
}

void AppPlan::Jsonize(Jsonizable::JsonWrapper& json) {
    json.Jsonize("rolePlans", rolePlans);
    json.Jsonize("packageInfos", packageInfos);
    json.Jsonize("prohibitedIps", prohibitedIps, prohibitedIps);
}

bool AppPlan::fromString(const string &content) {
    try {
        AppPlan tmpPlan;
        FromJsonString(tmpPlan, content);
        *this = tmpPlan;
        return true;
    } catch (const autil::legacy::ExceptionBase &e) {
        MF_LOG(ERROR, "AppPlan from json fail, exception[%s]",
                  e.what());
        return false;
    }
}

bool AppPlan::operator == (const AppPlan &appPlan) const {
    return ToJsonString(*this) == ToJsonString(appPlan);
}

END_MASTER_FRAMEWORK_NAMESPACE(simple_master);

