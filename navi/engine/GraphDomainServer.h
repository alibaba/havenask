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
#ifndef NAVI_GRAPHDOMAINSERVER_H
#define NAVI_GRAPHDOMAINSERVER_H

#include "navi/engine/GraphDomainRemoteBase.h"

namespace navi {

class GraphDomainServer : public GraphDomainRemoteBase
{
public:
    GraphDomainServer(Graph *graph);
    ~GraphDomainServer();
private:
    GraphDomainServer(const GraphDomainServer &);
    GraphDomainServer &operator=(const GraphDomainServer &);
public:
    ErrorCode doPreInit() override;
    bool run() override;
    bool doSend(NaviPartId gigPartId, NaviMessage &naviMessage) override;
    void beforePartReceiveFinished(NaviPartId partId) override;
    void receiveCallBack(NaviPartId gigPartId) override;
    void logCancel(const multi_call::GigStreamMessage &message,
                   multi_call::MultiCallErrorCode ec) override;
    NaviPartId getStreamPartId(NaviPartId partId) override;
    void tryCancel(const multi_call::GigStreamBasePtr &stream) override;
    bool onSendFailure(NaviPartId gigPartId) override;
    void fillTrace(NaviMessage &naviMessage);
    void fillMetaInfo(NaviMessage &naviMessage);
    void fillFiniMessage(NaviMessage &naviMessage);
private:
    bool sendEof() const;
};

}

#endif //NAVI_GRAPHDOMAINSERVER_H
