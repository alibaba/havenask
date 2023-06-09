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
#pragma once
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/plugin/InterfaceManager.h"

namespace isearch {
namespace sql {

class UdfToQueryManager : public autil::InterfaceManager {
public:
    UdfToQueryManager() {}
    ~UdfToQueryManager() {};

private:
    UdfToQueryManager(const UdfToQueryManager &);
    UdfToQueryManager &operator=(const UdfToQueryManager &);

public:
    bool init();
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<UdfToQueryManager> UdfToQueryManagerPtr;
} // namespace sql
} // namespace isearch
