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
#ifndef CARBON_ANETTRANSPORTSINGLETON_H
#define CARBON_ANETTRANSPORTSINGLETON_H

#include "carbon/Log.h"
#include "common/common.h"
#include "aios/network/anet/anet.h"

BEGIN_CARBON_NAMESPACE(master);

class ANetTransportSingleton
{
private:
    ANetTransportSingleton();
    ~ANetTransportSingleton();
public:
    static anet::Transport *getInstance();
    static bool init();
    static void stop();
private:
    static anet::Transport *_transport;
private:
    CARBON_LOG_DECLARE();
};

END_CARBON_NAMESPACE(master);

#endif //CARBON_ANETTRANSPORTSINGLETON_H
