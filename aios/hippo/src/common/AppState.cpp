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
#include "common/AppState.h"

using namespace std;
BEGIN_HIPPO_NAMESPACE(common);

HIPPO_LOG_SETUP(common, AppState);

AppState::AppState() {
}

AppState::~AppState() {
}

bool AppState::read(const std::string &fileName, Serializeable *obj) const {
    assert(obj);
    std::string content;
    if (read(fileName, content)) {
        if (obj->deserialize(content)) {
            return true;
        }
        else {
            HIPPO_LOG(ERROR, "AppState deserialize file %s failed.",
                      fileName.c_str());
        }
    }
    else {
        HIPPO_LOG(ERROR, "AppState read file %s failed.",
                  fileName.c_str());
    }
    return false;
}

bool AppState::write(const std::string &fileName, const Serializeable *obj) {
    assert(obj);
    if (write(fileName, obj->serialize())) {
        return true;
    }
    HIPPO_LOG(ERROR, "AppState write file %s failed.",
              fileName.c_str());
    return false;
}


END_HIPPO_NAMESPACE(common);

