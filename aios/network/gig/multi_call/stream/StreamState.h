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
#ifndef ISEARCH_MULTI_CALL_STREAMSTATE_H
#define ISEARCH_MULTI_CALL_STREAMSTATE_H

#include "aios/network/gig/multi_call/common/common.h"
#include "autil/Lock.h"

namespace multi_call {

enum HandlerOp {
    HO_RUN,
    HO_CANCEL,
    HO_SEND_CANCEL,
    HO_SEND,
    HO_FINISH,
    HO_IDLE,
    HO_FINISHED,
};

enum FinishStatus {
    FS_INIT,
    FS_RUNNING,
    FS_CANCEL_TRIGGER,
    FS_CANCELLING,
    FS_FINISHING,
    FS_FINISHED,
};

class StreamState
{
public:
    StreamState();
    ~StreamState();

private:
    StreamState(const StreamState &);
    StreamState &operator=(const StreamState &);

public:
    // state change
    HandlerOp next();
    void initFailed();
    void endInit();
    bool startCancel();
    void endCancel();
    void endFinish();
    void disableReceive();

public:
    // input change
    void setSendEof();
    bool sendEof() const;
    void setSendFinished();
    void setReceiveEof();
    bool receiveEof() const;
    void setReceiveFinished();
    void setReceiveCancel();
    bool bidiEof() const;
    bool running() const;
    void setErrorCode(MultiCallErrorCode ec);
    MultiCallErrorCode getErrorCode() const;
    bool hasError() const;
    bool isCancelled() const;
    void logState(bool useError = false) const;
    void setHandlerId(int64_t handlerId);

private:
    HandlerOp nextRunning();

public:
    static const char *getOpString(HandlerOp op);
    static const char *getStatusString(FinishStatus status);

private:
    mutable autil::ReadWriteLock _lock;
    FinishStatus _finishStatus;
    MultiCallErrorCode _ec;
    bool _sendEof;
    bool _sendFinished;
    bool _receiveEof;
    bool _receiveFinished;
    bool _initFailed;
    bool _cancelled;
    bool _finished;
    bool _receiveCancel;
    bool _disableReceive;
    int64_t _handlerId;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(StreamState);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_STREAMSTATE_H
