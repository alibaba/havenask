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
#include "aios/network/opentelemetry/core/eagleeye/EagleeyeUtil.h"

namespace opentelemetry {

bool EagleeyeUtil::validSpan(const SpanPtr& span) {
    return false;
}

const std::string& EagleeyeUtil::getETraceId(const SpanPtr& span) {
    return EMPTY_STRING;
}

const std::string& EagleeyeUtil::getERpcId(const SpanPtr& span) {
    return EMPTY_STRING;

}

const std::string& EagleeyeUtil::getEAppId(const SpanPtr& span) {
    return EMPTY_STRING;
}

void EagleeyeUtil::setEAppId(const SpanPtr& span, const std::string& eAppId) {
}

void EagleeyeUtil::putEUserData(const SpanPtr& span, const std::string& k, const std::string& v) {
}

void EagleeyeUtil::putEUserData(const SpanPtr& span, const std::map<std::string, std::string>& dataMap) {
}

const std::string& EagleeyeUtil::getEUserData(const SpanPtr& span, const std::string& k) {
    return EMPTY_STRING;
}

const std::map<std::string, std::string>& EagleeyeUtil::getEUserDatas(const SpanPtr& span) {
    static std::map<std::string, std::string> emptyMap;
    return emptyMap;
}

void EagleeyeUtil::splitUserData(const std::string &userData, std::map<std::string, std::string>& datas) {
}

std::string EagleeyeUtil::joinUserData(const std::map<std::string, std::string> &m) {
    return EMPTY_STRING;
}

bool EagleeyeUtil::isSampled(const SpanContextPtr& context) {
    return false;
}

}
