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
#include "aios/apps/facility/cm2/cm_basic/leader_election/leader_elector.h"

#include "aios/apps/facility/cm2/cm_basic/common/common.h"

using namespace std::placeholders;

namespace cm_basic {

AUTIL_LOG_SETUP(cm_basic, LeaderElector);

std::string LeaderElector::LEStateToString(LEStateType type)
{
    switch (type) {
    case LESTATE_BAD:
        return std::string("LESTATE_BAD");
    case LESTATE_WAITING:
        return std::string("LESTATE_WAITING");
    case LESTATE_PRIMARY:
        return std::string("LESTATE_PRIMARY");
    default:
        return std::string("LESTATE_BAD");
    }
}

LeaderElector::LeaderElector(cm_basic::ZkWrapper* p_zk_wrapper) : _state(LESTATE_BAD) { _pzkWrapper = p_zk_wrapper; }

LeaderElector::~LeaderElector() { shutdown(); }

void LeaderElector::setParameter(const std::string& path, const std::string& baseName)
{
    _path = path;
    _baseName = baseName;
    shutdown();
}

void LeaderElector::setHandler(const HandlerFuncType& handler)
{
    autil::ScopedLock lock(_cond);
    _handler = handler;
    if (_handler) {
        // inform
        _handler(_state);
    }
}

bool LeaderElector::checkIfExist(const std::string& value)
{
    std::vector<std::string> nodes;
    if (_pzkWrapper->getChild(_path, nodes) != ZOK) {
        AUTIL_LOG(WARN, "LeaderElector getChild failed. path:%s", _path.c_str());
        return false;
    }

    for (std::vector<std::string>::iterator it = nodes.begin(); it != nodes.end(); ++it) {
        const std::string nodePath = _path + "/" + *it;
        std::string spec;
        if (ZOK != _pzkWrapper->getData(nodePath, spec)) {
            AUTIL_LOG(WARN, "can't read data for path:%s", nodePath.c_str());
            continue;
        }

        if (spec == value) {
            AUTIL_LOG(WARN, "spec:%s is existed for path %s", value.c_str(), nodePath.c_str());
            return true;
        }
    }

    return false;
}

bool LeaderElector::checkIn(const std::string& value)
{
    if (!isBad()) {
        return true;
    }

    shutdown();
    {
        autil::ScopedLock lock(_mutex);
        if (!_pzkWrapper->open()) {
            AUTIL_LOG(WARN, "LeaderElector start failed.");
            return false;
        }

        _pzkWrapper->setConnCallback(std::bind(&LeaderElector::connChange, this, _1, _2, _3));
        _pzkWrapper->setDeleteCallback(std::bind(&LeaderElector::existChange, this, _1, _2, _3));

        if (!value.empty() && checkIfExist(value)) {
            return false;
        }

        if (_pzkWrapper->touchSeq(_path + "/" + _baseName, _indeedName, value)) {
            size_t pos = _indeedName.rfind('/');
            if (pos != std::string::npos) {
                _indeedName.erase(0, pos + 1);
            }
            bool bExist = false;
            _pzkWrapper->check(_path + "/" + _indeedName, bExist, true);
            if (checkPrimary()) {
                return true;
            }
        } else {
            AUTIL_LOG(WARN, "LeaderElector create node failed. basename(%s)value(%s)", _baseName.c_str(),
                      value.c_str());
        }
    }
    shutdown();
    return false;
}

LeaderElector::LEStateType LeaderElector::getState() const
{
    autil::ScopedLock lock(_cond);
    return _state;
}

bool LeaderElector::setValue(const std::string& value)
{
    autil::ScopedLock lock(_mutex);
    if (isBad() || _indeedName.empty()) {
        AUTIL_LOG(WARN, "setvalve failed.state(%d) indeedName(%s)", getState(),
                  _indeedName.empty() ? "NULL" : _indeedName.c_str());
        return false;
    }
    return _pzkWrapper->set(_path + "/" + _indeedName, value);
}

void LeaderElector::shutdown()
{
    _pzkWrapper->setChildCallback(0);
    _pzkWrapper->setDataCallback(0);
    _pzkWrapper->close();
    stateChange(LESTATE_BAD);
}

void LeaderElector::stateChange(LEStateType state)
{
    autil::ScopedLock lock(_cond);
    if (state == _state) {
        return;
    }
    _state = state;
    if (LESTATE_BAD == _state) {
        _indeedName.clear();
    }

    if (_handler) {
        // inform
        _handler(_state);
    }
    _cond.broadcast();
}

bool LeaderElector::checkPrimary()
{
    std::vector<std::string> nodes;
    if (_pzkWrapper->getChild(_path, nodes) != ZOK) {
        AUTIL_LOG(WARN, "LeaderElector getChild failed. path:%s", _path.c_str());
        return false;
    }
    if (nodes.empty()) {
        AUTIL_LOG(WARN, "LeaderElector no node found. %s", _path.c_str());
        return false;
    }
    bool b_primary = false;
    if (_pzkWrapper->leaderElectorStrategy(nodes, _path.c_str(), _indeedName.c_str(), b_primary) < 0) {
        return false;
    }
    stateChange(b_primary ? LESTATE_PRIMARY : LESTATE_WAITING);
    return true;
}

void LeaderElector::connChange(cm_basic::ZkWrapper* zk, const std::string& path, cm_basic::ZkWrapper::ZkStatus status)
{
    autil::ScopedLock lock(_mutex);
    if (cm_basic::ZkWrapper::ZK_BAD == status) {
        stateChange(LESTATE_BAD);
    } else if (cm_basic::ZkWrapper::ZK_CONNECTING == status) {
        ;
    } else if (cm_basic::ZkWrapper::ZK_CONNECTED == status) {
        ;
    }
}

void LeaderElector::existChange(cm_basic::ZkWrapper* zk, const std::string& path, cm_basic::ZkWrapper::ZkStatus status)
{
    autil::ScopedLock lock(_mutex);
    if (!checkPrimary()) {
        stateChange(LESTATE_BAD);
    }
}

LeaderElector::LEStateType LeaderElector::waitStateChangeFrom(LEStateType state, int timeout)
{
    autil::ScopedLock lock(_cond);
    if (_state != state) {
        return _state;
    }
    _cond.wait(timeout);
    return _state;
}

} // namespace cm_basic
