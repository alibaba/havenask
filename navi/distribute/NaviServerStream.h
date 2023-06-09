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
#ifndef NAVI_NAVISERVERSTREAM_H
#define NAVI_NAVISERVERSTREAM_H

#include "navi/engine/Navi.h"
#include "navi/distribute/NaviStreamBase.h"
#include <multi_call/stream/GigServerStream.h>

namespace navi {

class ServerRemoteGraph;
class NaviServerStreamCreator;

class NaviServerStream : public multi_call::GigServerStream,
                         public NaviStreamBase
{
public:
    NaviServerStream(NaviServerStreamCreator *creator, Navi *navi);
    ~NaviServerStream();
private:
    NaviServerStream(const NaviServerStream &);
    NaviServerStream &operator=(const NaviServerStream &);
public:
    google::protobuf::Message *newReceiveMessage(
            google::protobuf::Arena *arena) const override;
    bool receive(const multi_call::GigStreamMessage &message) override;
    void receiveCancel(const multi_call::GigStreamMessage &message,
                       multi_call::MultiCallErrorCode ec) override;
    void notifyIdle(multi_call::PartIdTy partId) override;
    bool notifyReceive(multi_call::PartIdTy partId) override;
private:
    void doFinish() override;
private:
    void initThreadPool();
    bool runGraph(NaviMessage *request, const ArenaPtr &arena);
    Navi *getNavi() const;
    std::shared_ptr<NaviSnapshot> getSnapshot() const;
private:
    NaviServerStreamCreator *_creator = nullptr;
    Navi *_navi = nullptr;
};

NAVI_TYPEDEF_PTR(NaviServerStream);

}

#endif //NAVI_NAVISERVERSTREAM_H
