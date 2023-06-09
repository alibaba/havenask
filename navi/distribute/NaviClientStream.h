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
#ifndef NAVI_NAVICLIENTSTREAM_H
#define NAVI_NAVICLIENTSTREAM_H

#include "navi/distribute/NaviStreamBase.h"
#include <multi_call/stream/GigClientStream.h>

namespace navi {

class RemoteSubGraphBase;

class NaviClientStream : public multi_call::GigClientStream,
                         public NaviStreamBase
{
public:
    NaviClientStream(const std::string &bizName, const NaviLoggerPtr &logger);
    ~NaviClientStream();
private:
    NaviClientStream(const NaviClientStream &);
    NaviClientStream &operator=(const NaviClientStream &);
public:
    google::protobuf::Message *newReceiveMessage(
            google::protobuf::Arena *arena) const override;
    bool receive(const multi_call::GigStreamMessage &message) override;
    void receiveCancel(const multi_call::GigStreamMessage &message,
                       multi_call::MultiCallErrorCode ec) override;
    bool notifyReceive(multi_call::PartIdTy partId) override;
    void notifyIdle(multi_call::PartIdTy partId) override;
};

NAVI_TYPEDEF_PTR(NaviClientStream);

}

#endif //NAVI_NAVICLIENTSTREAM_H
