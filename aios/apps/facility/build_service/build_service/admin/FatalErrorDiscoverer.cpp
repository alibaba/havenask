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
#include "build_service/admin/FatalErrorDiscoverer.h"

#include <cstddef>
#include <map>
#include <string>

using namespace std;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, FatalErrorDiscoverer);

FatalErrorDiscoverer::FatalErrorDiscoverer() : _hasFatalError(false) {}

FatalErrorDiscoverer::~FatalErrorDiscoverer() {}

void FatalErrorDiscoverer::fillFatalErrors(::google::protobuf::RepeatedPtrField<proto::FatalErrorMsg>* fatalErrors)
{
    if (!isAllNodesHasError() && !_hasFatalError) {
        return;
    }

    if (_hasFatalError) {
        for (size_t i = 0; i < _fatalErrors.size(); i++) {
            *fatalErrors->Add() = _fatalErrors[i]._errorInfo;
        }
        return;
    }

    for (size_t i = 0; i < _nodesError.size(); i++) {
        *fatalErrors->Add() = _nodesError[i]._errorInfo;
    }
}

void FatalErrorDiscoverer::supplementFatalErrorInfo(KeyValueMap& info)
{
    vector<string> errorInfos;
    if (_hasFatalError) {
        for (size_t i = 0; i < _fatalErrors.size(); i++) {
            if (!errorInfos.empty()) {
                break;
            }
            if (!_fatalErrors[i]._hasError) {
                continue;
            }
            string msg = string("Role=") + _fatalErrors[i]._errorInfo.rolename() + string(";Msg=") +
                         _fatalErrors[i]._errorInfo.errormsg();
            errorInfos.push_back(msg);
        }
    }

    for (size_t i = 0; i < _nodesError.size(); i++) {
        if (!errorInfos.empty()) {
            break;
        }
        if (!_nodesError[i]._hasError) {
            continue;
        }
        string msg = string("Role=") + _nodesError[i]._errorInfo.rolename() + string(";Msg=") +
                     _nodesError[i]._errorInfo.errormsg();
        errorInfos.push_back(msg);
    }

    if (!errorInfos.empty()) {
        info["fatalError"] = errorInfos[0];
    }
}

}} // namespace build_service::admin
