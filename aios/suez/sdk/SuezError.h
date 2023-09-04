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

#include <assert.h>
#include <map>
#include <string>

#include "autil/legacy/jsonizable.h"

namespace suez {

#define INVALID_ERROR_CODE (-1)
struct SuezError : public autil::legacy::Jsonizable {
    SuezError() : errCode(INVALID_ERROR_CODE) {}
    SuezError(int code, const std::string &msg) : errCode(code), errMsg(msg) { assert(INVALID_ERROR_CODE != code); }
    bool operator==(const SuezError &other) const {
        return this->errCode == other.errCode && this->errMsg == other.errMsg && this->errTarget == other.errTarget;
    }
    bool operator!=(const SuezError &other) const { return !(*this == other); }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("error_code", errCode, errCode);
        json.Jsonize("error_message", errMsg, errMsg);
        if (!errTarget.empty()) {
            json.Jsonize("error_target", errTarget, errTarget);
        }
    }
    int errCode;
    std::string errMsg;
    std::string errTarget;
};

typedef std::map<std::string, SuezError> SuezErrorMap;

class SuezErrorCollector {
public:
    SuezErrorCollector() {}
    void setError(const std::string &key, const SuezError &error) { _errorMap[key] = error; }
    void setGlobalError(const SuezError &error) { _globalError = error; }
    const SuezErrorMap &getErrorMap() const { return _errorMap; }
    const SuezError &getGlobalError() const { return _globalError; }
    SuezErrorMap _errorMap;
    SuezError _globalError;
};

extern const SuezError ERROR_NONE;
extern const SuezError DEPLOY_CONFIG_ERROR;
extern const SuezError DEPLOY_INDEX_ERROR;
extern const SuezError DEPLOY_DATA_ERROR;
extern const SuezError CONFIG_LOAD_ERROR;
extern const SuezError LOAD_UNKNOWN_ERROR;
extern const SuezError LOAD_LACKMEM_ERROR;
extern const SuezError LOAD_FORCE_REOPEN_UNKNOWN_ERROR;
extern const SuezError LOAD_FORCE_REOPEN_LACKMEM_ERROR;
extern const SuezError LOAD_RT_ERROR;
extern const SuezError CREATE_TABLE_ERROR;
extern const SuezError LOAD_RAW_FILE_TABLE_ERROR;
extern const SuezError DEPLOY_DISK_QUOTA_ERROR;
extern const SuezError UPDATE_RT_ERROR;
extern const SuezError FORCE_RELOAD_ERROR;

} // namespace suez
