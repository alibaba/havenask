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

#include <atomic>
#include <memory>

#include "autil/Lock.h"
#include "suez/sdk/SearchManager.h"
#include "suez/service/SuezControl.pb.h"

namespace suez {

class ControlServiceImpl final : public SearchManager, public ControlService {
public:
    ControlServiceImpl();
    ~ControlServiceImpl();

public:
    bool init(const SearchInitParam &initParam) override;
    UPDATE_RESULT update(const UpdateArgs &updateArgs,
                         UpdateIndications &updateIndications,
                         SuezErrorCollector &errorCollector) override;
    void stopService() override;
    void stopWorker() override;

    void control(google::protobuf::RpcController *controller,
                 const ControlParam *request,
                 ControlParam *response,
                 google::protobuf::Closure *done) override;

private:
    std::atomic<bool> _init;
    std::atomic<bool> _update;
    std::atomic<bool> _stop;
    std::shared_ptr<IndexProvider> _indexProvider;
    autil::ThreadMutex _mutex;
};

} // namespace suez
