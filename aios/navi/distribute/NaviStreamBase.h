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
#ifndef NAVI_NAVISTREAMBASE_H
#define NAVI_NAVISTREAMBASE_H

#include <multi_call/stream/GigStreamBase.h>

#include "autil/ObjectTracer.h"
#include "navi/engine/DomainHolder.h"
#include "navi/engine/GraphDomainRemoteBase.h"
#include "navi/engine/NaviThreadPool.h"
#include "navi/engine/TaskQueue.h"

namespace navi {

class NaviStreamReceiveItem : public TaskQueueScheduleItemBase
{
public:
    NaviStreamReceiveItem(const multi_call::GigStreamBasePtr &stream, multi_call::PartIdTy partId)
        : _stream(stream)
        , _partId(partId)
    {
    }
public:
    void process() override {
        if (_stream) {
            if (!_stream->asyncReceive(_partId)) {
                _stream->sendCancel(_partId, nullptr);
            }
            _stream.reset();
        }
    }
    void destroy() override {
        if (_stream) {
            _stream->sendCancel(_partId, nullptr);
            _stream.reset();
        }
        delete this;
    }
private:
    multi_call::GigStreamBasePtr _stream;
    multi_call::PartIdTy _partId;
};

class NaviStreamBase
{
public:
    NaviStreamBase(const NaviLoggerPtr &logger);
    ~NaviStreamBase();
private:
    NaviStreamBase(const NaviStreamBase &);
    NaviStreamBase &operator=(const NaviStreamBase &);
public:
    google::protobuf::Message *newReceiveMessageBase(
            google::protobuf::Arena *arena) const;
    bool receiveBase(const multi_call::GigStreamMessage &message);
    void receiveCancelBase(const multi_call::GigStreamMessage &message,
                           multi_call::MultiCallErrorCode ec);
    void notifyIdleBase(multi_call::PartIdTy partId);
    bool notifyReceiveBase(const multi_call::GigStreamBasePtr &stream, multi_call::PartIdTy partId);
    bool setDomain(const GraphDomainRemoteBasePtr &domain);
    bool initialized();
    void finish();
    void setThreadPool(const std::shared_ptr<NaviThreadPool> &threadPool);
private:
    virtual void doFinish() {
    }
protected:
    DECLARE_LOGGER();
    std::shared_ptr<NaviThreadPool> _threadPool;
    DomainHolder<GraphDomainRemoteBase> _domainHolder;
    bool _initialized;
    GraphDomainRemoteBase *_domainGdb = nullptr;
};

}

#endif //NAVI_NAVISTREAMBASE_H
