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

#include <list>
#include <memory>

#include "suez/sdk/SearchManager.h"

namespace suez {

class BuiltinSearchManagerList final : public SearchManager {
public:
    BuiltinSearchManagerList();

public:
    bool init(const SearchInitParam &initParam) override;
    UPDATE_RESULT update(const UpdateArgs &updateArgs,
                         UpdateIndications &updateIndications,
                         SuezErrorCollector &errorCollector) override;
    void stopService() override;
    void stopWorker() override;

private:
    std::list<std::unique_ptr<SearchManager>> _builtinSearchManagers;
};

} // namespace suez
