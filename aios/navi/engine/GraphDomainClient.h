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
#ifndef NAVI_GRAPHDOMAINCLIENT_H
#define NAVI_GRAPHDOMAINCLIENT_H

#include "navi/engine/GraphDomainRemoteBase.h"
#include <multi_call/interface/SearchService.h>

namespace navi {

class NaviClientStream;

class GraphDomainClient : public GraphDomainRemoteBase
{
public:
    GraphDomainClient(Graph *graph,
                      const std::string &bizName);
    ~GraphDomainClient();
private:
    GraphDomainClient(const GraphDomainClient &);
    GraphDomainClient &operator=(const GraphDomainClient &);
private:
    ErrorCode doPreInit() override;
    bool postInit() override;
    bool run() override;
    bool receive(const multi_call::GigStreamMessage &message) override;
    bool receiveCancel(const multi_call::GigStreamMessage &message, multi_call::MultiCallErrorCode ec) override;
    bool doSend(NaviPartId gigPartId, NaviMessage &naviMessage) override;
    NaviPartId getStreamPartId(NaviPartId partId) override;
    void tryCancel(const multi_call::GigStreamBasePtr &stream) override;
    bool onSendFailure(NaviPartId gigPartId) override;
public:
    const PartInfo &getPartInfo() const;
    multi_call::GigStreamRpcInfoVec stealStreamRpcInfo();
private:
    void finishPart(NaviPartId gigPartId, const multi_call::GigStreamBasePtr &stream);
    void collectRpcInfo(NaviPartId partId);
    void receiveCallBack(NaviPartId gigPartId, bool forceStop);
    bool initGigStream(const LocationDef &location);
    void addTags(const std::shared_ptr<NaviClientStream> &stream, const GigInfoDef &gigInfo) const;
    bool fillInitMessage(NaviPartId partId, NaviMessage &naviMessage) const;
    bool sendEof(NaviPartId partId) const;
    bool receiveEof() const;
    void notifyFinishAsync(ErrorCode ec);
    std::shared_ptr<NaviClientStream> createClientStream();
private:
    std::string _bizName;
    PartInfo _partInfo;
    std::string _subGraphStr;
    NaviCompressType _compressType = CT_NONE;
};

}

#endif //NAVI_GRAPHDOMAINCLIENT_H
