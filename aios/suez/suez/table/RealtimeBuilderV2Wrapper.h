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

#include <memory>

#include "build_service/proto/BasicDefs.pb.h"
#include "suez/table/TableBuilder.h"

namespace build_service {
namespace workflow {
struct RealtimeBuilderResource;
class RealtimeBuilderV2;
} // namespace workflow
} // namespace build_service

namespace indexlibv2 {
namespace framework {
class ITablet;
}
} // namespace indexlibv2

namespace suez {

class RealtimeBuilderV2Wrapper : public TableBuilder {
public:
    RealtimeBuilderV2Wrapper(const build_service::proto::PartitionId &pid,
                             const std::shared_ptr<indexlibv2::framework::ITablet> &tablet,
                             const build_service::workflow::RealtimeBuilderResource &resource,
                             const std::string &configPath);
    ~RealtimeBuilderV2Wrapper();

public:
    bool start() override;
    void stop() override;
    void suspend() override;
    void resume() override;
    void skip(int64_t timestamp) override;
    void forceRecover() override;
    bool isRecovered() override;
    bool needCommit() const override;
    std::pair<bool, TableVersion> commit() override;

private:
    build_service::proto::PartitionId _pid;
    std::unique_ptr<build_service::workflow::RealtimeBuilderV2> _builder;
};

} // namespace suez
