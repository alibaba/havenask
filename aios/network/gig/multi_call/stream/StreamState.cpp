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
#include "aios/network/gig/multi_call/stream/StreamState.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, StreamState);

StreamState::StreamState()
    : _finishStatus(FS_INIT)
    , _ec(MULTI_CALL_ERROR_NONE)
    , _sendEof(false)
    , _sendFinished(false)
    , _receiveEof(false)
    , _receiveFinished(false)
    , _initFailed(false)
    , _cancelled(false)
    , _finished(false)
    , _receiveCancel(false)
    , _disableReceive(false)
    , _handlerId(0) {
}

StreamState::~StreamState() {
    if ((FS_INIT != _finishStatus) && (FS_FINISHED != _finishStatus)) {
        AUTIL_LOG(ERROR, "invalid destruct status [%d], handler [%p]", _finishStatus,
                  (void *)_handlerId);
        logState(true);
    }
}

HandlerOp StreamState::next() {
    autil::ScopedWriteLock lock(_lock);
    switch (_finishStatus) {
    case FS_INIT: {
        if (_initFailed) {
            // start call not invoked or failed
            _finishStatus = FS_FINISHED;
            return HO_FINISHED;
        }
        return HO_IDLE;
    }
    case FS_RUNNING:
        return nextRunning();
    case FS_CANCEL_TRIGGER:
        return HO_SEND_CANCEL;
    case FS_CANCELLING:
    case FS_FINISHING:
    case FS_FINISHED:
        return HO_IDLE;
    default:
        assert(false);
        return HO_IDLE;
    }
}

HandlerOp StreamState::nextRunning() {
    if (_finished) {
        _finishStatus = FS_FINISHED;
        return HO_FINISHED;
    }
    bool eof = _sendFinished && _receiveFinished;
    bool noError = _ec <= MULTI_CALL_ERROR_DEC_WEIGHT;
    if (noError && !eof) {
        if (_disableReceive) {
            return HO_SEND;
        } else {
            return HO_RUN;
        }
    }
    if (!eof && !_cancelled && !_receiveCancel) {
        _finishStatus = FS_CANCEL_TRIGGER;
        return HO_CANCEL;
    }
    _finishStatus = FS_FINISHING;
    return HO_FINISH;
}

void StreamState::initFailed() {
    autil::ScopedWriteLock lock(_lock);
    _initFailed = true;
}

void StreamState::endInit() {
    autil::ScopedWriteLock lock(_lock);
    if (FS_INIT == _finishStatus) {
        _finishStatus = FS_RUNNING;
    }
}

bool StreamState::startCancel() {
    autil::ScopedWriteLock lock(_lock);
    if (FS_CANCEL_TRIGGER != _finishStatus) {
        return false;
    }
    assert(!_cancelled);
    _finishStatus = FS_CANCELLING;
    return true;
}

void StreamState::endCancel() {
    autil::ScopedWriteLock lock(_lock);
    assert(_finishStatus == FS_CANCELLING);
    assert(!_cancelled);
    _finishStatus = FS_RUNNING;
    _cancelled = true;
}

void StreamState::endFinish() {
    autil::ScopedWriteLock lock(_lock);
    assert(_finishStatus == FS_FINISHING);
    assert(!_finished);
    _finishStatus = FS_RUNNING;
    _finished = true;
}

void StreamState::disableReceive() {
    autil::ScopedWriteLock lock(_lock);
    _disableReceive = true;
}

void StreamState::setSendEof() {
    autil::ScopedWriteLock lock(_lock);
    _sendEof = true;
}

bool StreamState::sendEof() const {
    autil::ScopedReadLock lock(_lock);
    return _sendEof;
}

void StreamState::setSendFinished() {
    autil::ScopedWriteLock lock(_lock);
    _sendFinished = true;
}

void StreamState::setReceiveEof() {
    autil::ScopedWriteLock lock(_lock);
    _receiveEof = true;
}

bool StreamState::receiveEof() const {
    autil::ScopedReadLock lock(_lock);
    return _receiveEof;
}

void StreamState::setReceiveFinished() {
    autil::ScopedWriteLock lock(_lock);
    _receiveFinished = true;
}

void StreamState::setReceiveCancel() {
    autil::ScopedWriteLock lock(_lock);
    _receiveCancel = true;
}

bool StreamState::bidiEof() const {
    autil::ScopedReadLock lock(_lock);
    return _sendEof && _receiveEof;
}

bool StreamState::running() const {
    autil::ScopedReadLock lock(_lock);
    return _ec <= MULTI_CALL_ERROR_DEC_WEIGHT &&
           (FS_RUNNING == _finishStatus || FS_INIT == _finishStatus);
}

void StreamState::setErrorCode(MultiCallErrorCode ec) {
    if (MULTI_CALL_ERROR_NONE == ec) {
        return;
    }
    autil::ScopedWriteLock lock(_lock);
    if (_ec <= MULTI_CALL_ERROR_DEC_WEIGHT) {
        AUTIL_LOG(DEBUG, "[%p] setErrorCode, ec [%s]", (void *)_handlerId, translateErrorCode(ec));
        logState();
        _ec = ec;
    }
}

MultiCallErrorCode StreamState::getErrorCode() const {
    autil::ScopedReadLock lock(_lock);
    return _ec;
}

bool StreamState::isCancelled() const {
    autil::ScopedReadLock lock(_lock);
    return _cancelled;
}

bool StreamState::hasError() const {
    autil::ScopedReadLock lock(_lock);
    return _ec > MULTI_CALL_ERROR_DEC_WEIGHT;
}

void StreamState::setHandlerId(int64_t handlerId) {
    _handlerId = handlerId;
}

const char *StreamState::getOpString(HandlerOp op) {
    switch (op) {
    case HO_RUN:
        return "HO_RUN";
    case HO_CANCEL:
        return "HO_CANCEL";
    case HO_SEND_CANCEL:
        return "HO_SEND_CANCEL";
    case HO_SEND:
        return "HO_SEND";
    case HO_FINISH:
        return "HO_FINISH";
    case HO_IDLE:
        return "HO_IDLE";
    case HO_FINISHED:
        return "HO_FINISHED";
    default:
        return "HO_UNKNOWN";
    }
}

const char *StreamState::getStatusString(FinishStatus status) {
    switch (status) {
    case FS_INIT:
        return "FS_INIT";
    case FS_RUNNING:
        return "FS_RUNNING";
    case FS_CANCEL_TRIGGER:
        return "FS_CANCEL_TRIGGER";
    case FS_CANCELLING:
        return "FS_CANCELLING";
    case FS_FINISHING:
        return "FS_FINISHING";
    case FS_FINISHED:
        return "FS_FINISHED";
    default:
        return "FS_UNKNOWN";
    }
}

void StreamState::logState(bool useError) const {
    if (useError) {
        AUTIL_LOG(ERROR,
                  "[%p] ec: %s, finishStatus: %s, sendEof: %d, "
                  "sendFinished: %d, receiveEof: %d, receiveFinished: %d, "
                  "initFailed: %d, cancelled: %d, finished: %d, disableReceive: %d",
                  (void *)_handlerId, translateErrorCode(_ec),
                  StreamState::getStatusString(_finishStatus), _sendEof, _sendFinished, _receiveEof,
                  _receiveFinished, _initFailed, _cancelled, _finished, _disableReceive);
    } else {
        AUTIL_LOG(DEBUG,
                  "[%p] ec: %s, finishStatus: %s, sendEof: %d, "
                  "sendFinished: %d, receiveEof: %d, receiveFinished: %d, "
                  "initFailed: %d, cancelled: %d, finished: %d, disableReceive: %d",
                  (void *)_handlerId, translateErrorCode(_ec),
                  StreamState::getStatusString(_finishStatus), _sendEof, _sendFinished, _receiveEof,
                  _receiveFinished, _initFailed, _cancelled, _finished, _disableReceive);
    }
}

} // namespace multi_call
