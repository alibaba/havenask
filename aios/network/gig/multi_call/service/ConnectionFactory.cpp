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
#include "aios/network/gig/multi_call/service/ConnectionFactory.h"

#include <grpc++/generic/generic_stub.h>

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, ConnectionFactory);

void GrpcConnectionFactory::initChannelArgs(GrpcChannelInitParams params) {
    _channelArgs.reset(new grpc::ChannelArguments());
    _channelArgs->SetMaxReceiveMessageSize(-1);
    _channelArgs->SetMaxSendMessageSize(-1);
    if (params.keepAliveInterval > 0) {
        // ref https://github.com/grpc/grpc/blob/master/doc/keepalive.md
        AUTIL_LOG(INFO, "enable grpc keep-alive with interval[%ld] timeout[%ld]",
                  params.keepAliveInterval, params.keepAliveTimeout);
        _channelArgs->SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, params.keepAliveInterval);
        _channelArgs->SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, params.keepAliveTimeout);
    }
    if (_secureConfig.pemRootCerts.empty()) {
        _channelCredentials = grpc::InsecureChannelCredentials();
    } else {
        _channelCredentials = grpc::SslCredentials(getSslOptions());
        _channelArgs->SetSslTargetNameOverride(_secureConfig.targetName);
    }
}

grpc::SslCredentialsOptions GrpcConnectionFactory::getSslOptions() const {
    grpc::SslCredentialsOptions option;
    option.pem_root_certs = _secureConfig.pemRootCerts;
    option.pem_private_key = _secureConfig.pemPrivateKey;
    option.pem_cert_chain = _secureConfig.pemCertChain;
    return option;
}

} // namespace multi_call
