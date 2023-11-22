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
#include "navi/engine/NaviUserResult.h"
#include "navi/engine/GraphResult.h"
#include "navi/engine/DomainHolder.h"
#include "navi/engine/GraphDomainUser.h"
#include "navi/engine/NaviWorkerBase.h"
#include "navi/engine/Port.h"
#include "navi/log/TraceAppender.h"
#include "navi/util/CommonUtil.h"
#include "navi/util/ReadyBitMap.h"

using namespace multi_call;

namespace navi {

NaviUserResult::NaviUserResult()
    : _finished(false)
    , _domainHolder(new DomainHolder<GraphDomainUser>())
    , _finishMap(nullptr)
    , _loopIndex(0)
    , _terminated(false)
    , _gdbPtr(nullptr) {
    atomic_set(&_dataCount, 0);
}

NaviUserResult::~NaviUserResult() {
    DELETE_AND_SET_NULL(_domainHolder);
    ReadyBitMap::freeReadyBitMap(_finishMap);
}

bool NaviUserResult::nextPort(NaviUserData &userData, bool &eof, ErrorCode &retEc) {
    eof = _finished;
    DomainHolderScope<GraphDomainUser> scope(*_domainHolder);
    auto domain = scope.getDomain();
    if (!domain) {
        return false;
    }
    size_t loopCount = 0;
    bool ret = false;
    while (loopCount < _portVec.size()) {
        auto portIndex = _loopIndex;
        auto &portDataInfo = _portVec[portIndex];
        _loopIndex++;
        if (_loopIndex >= _portVec.size()) {
            _loopIndex = 0;
        }
        auto port = portDataInfo.port;
        auto partId = portDataInfo.partId;
        auto ec = port->getData(partId, userData.data, userData.eof);
        if (EC_NONE == ec) {
            if (userData.eof) {
                _finishMap->setFinish(portIndex, true);
            }
            userData.name = port->getNode();
            userData.partId = partId;
            userData.index = portIndex;
            userData.streamId = portDataInfo.streamId++;
            atomic_dec(&_dataCount);
            ret = true;
            break;
        } else if (EC_NO_DATA != ec) {
            eof = true;
            _finished = true;
            retEc = ec;
            ret = false;
            break;
        }
        loopCount++;
    }
    eof = eof || _finishMap->isFinish();
    NAVI_LOG(SCHEDULE2,
             "[%p], ret: %d, loopCount: %lu, portMap size: %lu, finish map %d, "
             "finished: %d, eof: %d, name: "
             "%s, data: "
             "%p, data eof: %d, partId: %d, streamId: %lu",
             this, ret, loopCount, _portVec.size(), _finishMap->isFinish(),
             _finished, eof, userData.name.c_str(), userData.data.get(),
             userData.eof, userData.partId, userData.streamId);
    return ret;
}

void NaviUserResult::setGraphResult(const GraphResultPtr &result) {
    _graphResult = result;
}

const std::shared_ptr<GraphResult> &NaviUserResult::getGraphResult() const {
    return _graphResult;
}

bool NaviUserResult::bindDomain(const GraphDomainUserPtr &domain) {
    if (!collectPort(domain)) {
        return false;
    }
    initFinishMap(_portVec.size());
    domain->setUserResult(shared_from_this());
    // every member should be inited before domainHolder
    if (!_domainHolder->init(domain)) {
        return false;
    }
    _logger = NaviObjectLogger(this, "user result", domain->getLogger().logger);
    notify(false);
    return true;
}

bool NaviUserResult::collectPort(const GraphDomainUserPtr &domain) {
    std::set<std::string> outputSet;
    const auto &portMap = domain->getPortMap();
    for (const auto &pair : portMap) {
        auto port = pair.second;
        const auto &partInfo = port->getPartInfo();
        const auto &outputName = port->getNode();
        if (outputSet.end() != outputSet.find(outputName)) {
            NAVI_LOG(ERROR, "graph has more than one output named [%s]",
                     outputName.c_str());
            return false;
        }
        outputSet.insert(outputName);
        for (auto partId : partInfo) {
            _portVec.emplace_back(port, partId);
        }
    }
    return true;
}

void NaviUserResult::initFinishMap(uint32_t portCount) {
    _finishMap = ReadyBitMap::createReadyBitMap(portCount);
    _finishMap->setFinish(false);
}

void NaviUserResult::terminate(ErrorCode ec) {
    bool expect = false;
    if (!_terminated.compare_exchange_weak(expect, true)) {
        return;
    }
    {
        DomainHolderScope<GraphDomainUser> scope(*_domainHolder);
        auto domain = scope.getDomain();
        if (domain) {
            NAVI_LOG(SCHEDULE2, "user result finish, ec: %s", CommonUtil::getErrorString(ec));
            updateError(ec);
            domain->notifyFinish(ec, true);
        }
    }
    _domainHolder->terminate();
}

void NaviUserResult::updateError(ErrorCode ec) {
    if (_graphResult && EC_NONE != ec) {
        _graphResult->updateError(_graphResult->makeError(_logger.logger, ec));
    }
}

void NaviUserResult::setWorker(NaviWorkerBase *worker) {
    _gdbPtr = worker;
}

void NaviUserResult::abort(ErrorCode ec) {
    terminate(ec);
    notify(true);
}

void NaviUserResult::notifyData() {
    atomic_inc(&_dataCount);
    notify(false);
}

bool NaviUserResult::checkFinish() const {
    if (!_terminated.load(std::memory_order_relaxed)) {
        return false;
    }
    if (!_graphResult) {
        return false;
    }
    if (!_graphResult->isFinished()) {
        return false;
    }
    return true;
}

NaviResultPtr NaviUserResult::getNaviResult() {
    if (!checkFinish()) {
        return nullptr;
    }
    return getNaviResultWithoutCheck();
}

NaviResultPtr NaviUserResult::getNaviResultWithoutCheck() {
    autil::ScopedLock lock(_resultMutex);
    if (_naviResult) {
        return _naviResult;
    }
    _naviResult = _graphResult->createNaviResult();
    _naviResult->_graphResult = _graphResult;
    return _naviResult;
}

bool NaviUserResult::hasOutput() const {
    return !_portVec.empty();
}

// not ready until bind finished
size_t NaviUserResult::outputCount() const {
    return _portVec.size();
}

bool NaviSyncUserResult::nextData(NaviUserData &data, bool &eof) {
    auto ret = false;
    ErrorCode ec = EC_NONE;
    while (true) {
        if (nextPort(data, eof, ec)) {
            ret = true;
            break;
        } else if (!eof) {
            wait();
        } else {
            ret = false;
            break;
        }
    }
    if (eof) {
        terminate(ec);
    }
    return ret;
}

void NaviSyncUserResult::notify(bool finish) {
    NAVI_KERNEL_LOG(SCHEDULE1, "start notify, finish [%d]", finish);
    {
        autil::ScopedLock lock(_cond);
        if (_finished) {
            _cond.signal();
            return;
        }
        _finished = finish;
        _cond.signal();
    }
}

void NaviSyncUserResult::wait() {
    autil::ScopedLock lock(_cond);
    while (!_finished && 0 == atomic_read(&_dataCount)) {
        _cond.wait();
    }
}

NaviAsyncUserResult::NaviAsyncUserResult(NaviUserResultClosure *closure)
    : _resultClosure(closure)
    , _resultDataEof(false) {
    assert(closure && "closure must not be null");
    atomic_set(&_pendingNotify, 0);
}

bool NaviAsyncUserResult::nextData(NaviUserData &data, bool &eof) {
    assert(_resultDataEof && "do not call before data eof");
    if (_resultData.empty()) {
        data.data.reset();
        eof = true;
        return false;
    }
    data = std::move(_resultData.front());
    _resultData.pop_front();
    eof = _resultData.empty();
    return true;

}

void NaviAsyncUserResult::notify(bool finish) {
    NAVI_KERNEL_LOG(SCHEDULE1, "start notify, finish [%d]", finish);
    {
        autil::ScopedLock lock(_cond);
        if (_finished) {
            return;
        }
        _finished = finish;
    }
    atomic_inc(&_pendingNotify);

    NaviUserResultClosure *resultClosure = nullptr;
    ErrorCode ec;
    while (atomic_read(&_pendingNotify) > 0) {
        if (0 != _notifyCond.trylock()) {
            // will handle by other notify handler
            return;
        }
        if (atomic_read(&_pendingNotify) > 0) {
            atomic_dec(&_pendingNotify);
            NaviUserData data;
            bool eof = false;
            ec = EC_NONE;
            NAVI_KERNEL_LOG(SCHEDULE1, "start processData");
            bool ret = processData(data, eof, ec);
            NAVI_KERNEL_LOG(
                SCHEDULE1, "end process, ret [%d], eof [%d], ec [%d], port [%s]", ret, eof, ec, data.name.c_str());
            if (ret) {
                auto *p = data.data.get();
                NAVI_KERNEL_LOG(SCHEDULE1, "emplace back data [%p] type[%s]", p, p ? typeid(*p).name() : "");
                _resultData.emplace_back(std::move(data));
            }
            _resultDataEof = eof;
            if (_resultDataEof) {
                std::swap(resultClosure, _resultClosure);
                NAVI_KERNEL_LOG(SCHEDULE1, "swap result closure [%p]", resultClosure);
            }
        }
        _notifyCond.unlock();
    }

    if (resultClosure) {
        terminate(ec);
        NAVI_KERNEL_LOG(SCHEDULE1, "start call result closure [%p]", resultClosure);
        NaviLoggerScope scope(_logger);
        resultClosure->run(shared_from_this());
        NAVI_KERNEL_LOG(SCHEDULE1, "finish call result closure [%p]", resultClosure);
    }
}

bool NaviAsyncUserResult::processData(NaviUserData &data, bool &eof, ErrorCode &retEc) {
    auto ret = false;
    while (true) {
        if (nextPort(data, eof, retEc)) {
            ret = true;
            break;
        } else {
            ret = false;
            break;
        }
    }
    return ret;
}


}
