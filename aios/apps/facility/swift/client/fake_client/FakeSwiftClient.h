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
#include <string>

#include "autil/Log.h"
#include "swift/client/SwiftClient.h"
#include "swift/client/SwiftClientConfig.h"
#include "swift/client/fake_client/FakeSwiftAdminAdapter.h"
#include "swift/common/Common.h"
#include "swift/config/ClientAuthorizerInfo.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/network/SwiftRpcChannelManager.h"

namespace swift {
namespace client {

class FakeSwiftClient : public SwiftClient {
public:
    FakeSwiftClient() {}
    virtual ~FakeSwiftClient() {}

private:
    FakeSwiftClient(const FakeSwiftClient &);
    FakeSwiftClient &operator=(const FakeSwiftClient &);

private:
    virtual network::SwiftAdminAdapterPtr createSwiftAdminAdapter(const SwiftClientConfig &config,
                                                                  network::SwiftRpcChannelManagerPtr channelManager) {
        (void)config;
        (void)channelManager;
        network::SwiftAdminAdapterPtr adapter(new FakeSwiftAdminAdapter());
        auth::ClientAuthorizerInfo info;
        info.username = config.username;
        info.passwd = config.passwd;
        adapter->setAuthenticationConf(info);
        return adapter;
    }

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(FakeSwiftClient);

} // namespace client
} // namespace swift
