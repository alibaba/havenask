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
#include "master/ANetTransportSingleton.h"

using namespace std;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, ANetTransportSingleton);

anet::Transport *ANetTransportSingleton::_transport = nullptr;

anet::Transport *ANetTransportSingleton::getInstance() {
    return _transport;
}

bool ANetTransportSingleton::init() {
    if (_transport != nullptr) {
        return true;
    }
    _transport = new anet::Transport(64 /*io thread num*/);
    return _transport->start();
}

void ANetTransportSingleton::stop() {
    if (_transport == NULL) {
        return;
    }
    _transport->stop();
    _transport->wait();
    DELETE_AND_SET_NULL(_transport);
}

END_CARBON_NAMESPACE(master);

