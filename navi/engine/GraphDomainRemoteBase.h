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
#ifndef NAVI_GRAPHDOMAINREMOTEBASE_H
#define NAVI_GRAPHDOMAINREMOTEBASE_H

#include "navi/engine/GraphDomain.h"
#include "navi/engine/NaviWorkerBase.h"
#include "navi/engine/PartState.h"
#include "navi/proto/NaviStream.pb.h"
#include "navi/util/ReadyBitMap.h"
#include "aios/network/gig/multi_call/stream/GigStreamBase.h"
#include "aios/network/gig/multi_call/stream/GigStreamRpcInfo.h"

namespace navi {

class NaviStreamBase;

class GraphDomainRemoteBase
    : public GraphDomain,
      public std::enable_shared_from_this<GraphDomainRemoteBase>
{
public:
    GraphDomainRemoteBase(Graph *graph, GraphDomainType type);
    ~GraphDomainRemoteBase();
private:
    GraphDomainRemoteBase(const GraphDomainRemoteBase &);
    GraphDomainRemoteBase &operator=(const GraphDomainRemoteBase &);
public:
    void addSubBorder(const SubGraphBorderPtr &border);
public:
    void notifySend(NaviPartId partId) override;
    bool receive(const multi_call::GigStreamMessage &message);
    bool receiveCancel(const multi_call::GigStreamMessage &message,
                       multi_call::MultiCallErrorCode ec);
    void notifyIdle(NaviPartId partId);
    void setStream(const multi_call::GigStreamBasePtr &stream);
private:
    virtual bool doSend(NaviPartId gigPartId, NaviMessage &naviMessage) = 0;
    virtual void beforePartReceiveFinished(NaviPartId partId) = 0;
    virtual void receiveCallBack(NaviPartId gigPartId) = 0;
    virtual void logCancel(const multi_call::GigStreamMessage &message,
                           multi_call::MultiCallErrorCode ec) = 0;
    virtual NaviPartId getStreamPartId(NaviPartId partId) = 0;
    virtual void tryCancel(const multi_call::GigStreamBasePtr &stream) = 0;
    virtual bool onSendFailure(NaviPartId gigPartId) = 0;
public:
    bool preInit() override;
    virtual ErrorCode doPreInit();
    void doFinish(ErrorCode ec) override;
    void incMessageCount(NaviPartId partId) override;
    void decMessageCount(NaviPartId partId) override;
protected:
    bool bindStream();
    void unBindStream();
    multi_call::GigStreamBasePtr getStream() const;
private:
    NaviStreamBase *getStreamBase();
    void send(NaviPartId partId);
    bool collectData(NaviPartId partId, NaviMessage &naviMessage);
    bool doReceive(const multi_call::GigStreamMessage &message);
    bool borderReceive(NaviBorderData &borderData);
    SubGraphBorder *getOutputBorder(const BorderId &borderId) const;
    void receivePartTrace(NaviMessage *naviMessage);
    bool receivePartMetaInfo(NaviMessage *naviMessage);
    void receivePartResult(NaviMessage *naviMessage);
protected:
    mutable autil::ReadWriteLock _streamLock;
    multi_call::GigStreamBasePtr _stream;
    StreamState _streamState;
};

NAVI_TYPEDEF_PTR(GraphDomainRemoteBase);

}

#endif //NAVI_GRAPHDOMAINREMOTEBASE_H
