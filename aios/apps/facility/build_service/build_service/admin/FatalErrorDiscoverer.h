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
#pragma once

#include <memory>
#include <stddef.h>
#include <vector>

#include "build_service/common_define.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/RoleNameGenerator.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class FatalErrorDiscoverer
{
public:
    FatalErrorDiscoverer();
    ~FatalErrorDiscoverer();

private:
    FatalErrorDiscoverer(const FatalErrorDiscoverer&);
    FatalErrorDiscoverer& operator=(const FatalErrorDiscoverer&);

public:
    void fillFatalErrors(::google::protobuf::RepeatedPtrField<proto::FatalErrorMsg>* fatalErrors);

    void supplementFatalErrorInfo(KeyValueMap& info);

    template <typename NodesType>
    void collectNodeFatalErrors(const NodesType& nodes)
    {
        detectConfigError(nodes);
        if (_hasFatalError) {
            return;
        }
        _nodesError.resize(nodes.size());
        for (size_t i = 0; i < nodes.size(); i++) {
            auto current = nodes[i]->getCurrentStatus();
            if (current.status() == proto::WS_STARTED || current.status() == proto::WS_STOPPED ||
                current.status() == proto::WS_SUSPEND_READ) {
                _nodesError.clear();
                _nodesError.resize(nodes.size());
                return;
            }
            for (const auto& errorInfo : current.errorinfos()) {
                if (errorInfo.advice() == proto::BS_STOP) {
                    _nodesError[i]._hasError = true;
                    _nodesError[i]._errorInfo.set_errormsg(errorInfo.errormsg());
                    _nodesError[i]._errorInfo.set_rolename(
                        proto::RoleNameGenerator::generateRoleName(nodes[i]->getPartitionId()));
                    break;
                }
            }
        }
    }

private:
    bool isAllNodesHasError()
    {
        for (size_t i = 0; i < _nodesError.size(); i++) {
            if (!_nodesError[i]._hasError) {
                return false;
            }
        }
        return true;
    }
    template <typename NodesType>
    void detectConfigError(const NodesType& nodes)
    {
        if (_hasFatalError == true) {
            return;
        }
        for (size_t i = 0; i < nodes.size(); i++) {
            auto current = nodes[i]->getCurrentStatus();
            if (current.status() == proto::WS_STARTED || current.status() == proto::WS_STOPPED ||
                current.status() == proto::WS_SUSPEND_READ) {
                continue;
            }
            for (const auto& errorInfo : current.errorinfos()) {
                if (errorInfo.advice() == proto::BS_FATAL_ERROR) {
                    _hasFatalError = true;
                    ErrorPair errorPair;
                    errorPair._hasError = true;
                    errorPair._errorInfo.set_errormsg(errorInfo.errormsg());
                    errorPair._errorInfo.set_rolename(
                        proto::RoleNameGenerator::generateRoleName(nodes[i]->getPartitionId()));
                    _fatalErrors.push_back(errorPair);
                }
            }
        }
    }

private:
    struct ErrorPair {
        ErrorPair(bool hasError, proto::FatalErrorMsg& errorInfo) : _hasError(hasError), _errorInfo(errorInfo) {}
        ErrorPair() : _hasError(false) {}
        bool _hasError;
        proto::FatalErrorMsg _errorInfo;
    };
    std::vector<ErrorPair> _nodesError;

    bool _hasFatalError;
    std::vector<ErrorPair> _fatalErrors;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FatalErrorDiscoverer);

}} // namespace build_service::admin
