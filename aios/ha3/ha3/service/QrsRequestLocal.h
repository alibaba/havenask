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
#include <stdint.h>
#include <string>
#include "ha3/service/QrsRequest.h"

namespace suez {
class RpcServer;
}

namespace isearch {
namespace service {

class QrsRequestLocal : public QrsRequest {
public:
    QrsRequestLocal(const std::string &bizName,
                    google::protobuf::Message *message,
                    uint32_t timeout,
                    const std::shared_ptr<google::protobuf::Arena> &arena,
                    suez::RpcServer *rpcServer);
    ~QrsRequestLocal();
private:
    QrsRequestLocal(const QrsRequestLocal &);
    QrsRequestLocal &operator=(const QrsRequestLocal &);

public:
    google::protobuf::Service *createStub(google::protobuf::RpcChannel *channel) override;

private:
    google::protobuf::Service *_localStub = nullptr;
    suez::RpcServer *_rpcServer = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QrsRequestLocal> QrsRequestLocalPtr;

} // namespace service
} // namespace isearch
