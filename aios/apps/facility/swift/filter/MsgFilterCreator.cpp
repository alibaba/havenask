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
#include "swift/filter/MsgFilterCreator.h"

#include <cstddef>

#include "autil/CommonMacros.h"
#include "autil/StringTokenizer.h"
#include "autil/TimeUtility.h"
#include "swift/filter/AndMsgFilter.h"
#include "swift/filter/ContainMsgFilter.h"
#include "swift/filter/InMsgFilter.h"

namespace swift {
namespace filter {
class MsgFilter;
} // namespace filter
} // namespace swift

namespace swift {
namespace filter {
AUTIL_LOG_SETUP(swift, MsgFilterCreator);
using namespace std;
using namespace autil;

string MsgFilterCreator::OPERATOR_IN_FILTER_SEPARATOR = " IN ";
string MsgFilterCreator::OPERATOR_AND_FILTER_SEPARATOR = " AND ";
string MsgFilterCreator::OPERATOR_CONTAIN_FILTER_SEPARATOR = " CONTAIN ";

MsgFilterCreator::MsgFilterCreator() {}

MsgFilterCreator::~MsgFilterCreator() {}

MsgFilter *MsgFilterCreator::createMsgFilter(const std::string &filterDesc) {
    size_t pos1 = filterDesc.find(OPERATOR_AND_FILTER_SEPARATOR);
    size_t pos2 = filterDesc.find(OPERATOR_CONTAIN_FILTER_SEPARATOR);
    size_t pos3 = filterDesc.find(OPERATOR_IN_FILTER_SEPARATOR);
    if (pos1 != string::npos) {
        return createAndMsgFilter(filterDesc);
    } else if (pos2 != string::npos) {
        return createContainMsgFilter(filterDesc);
    } else if (pos3 != string::npos) {
        return createInMsgFilter(filterDesc);
    } else {
        return nullptr;
    }
}

ContainMsgFilter *MsgFilterCreator::createContainMsgFilter(const std::string &containDesc) {
    StringTokenizer st(containDesc,
                       OPERATOR_CONTAIN_FILTER_SEPARATOR,
                       StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    if (st.getNumTokens() != 2) {
        return nullptr;
    }
    ContainMsgFilter *msgFilter = new ContainMsgFilter(st[0], st[1]);
    if (!msgFilter->init()) {
        delete msgFilter;
        msgFilter = nullptr;
    }
    return msgFilter;
}

InMsgFilter *MsgFilterCreator::createInMsgFilter(const std::string &inDesc) {
    StringTokenizer st(
        inDesc, OPERATOR_IN_FILTER_SEPARATOR, StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    if (st.getNumTokens() != 2) {
        return nullptr;
    }
    InMsgFilter *msgFilter = new InMsgFilter(st[0], st[1]);
    if (!msgFilter->init()) {
        delete msgFilter;
        msgFilter = nullptr;
    }
    return msgFilter;
}

AndMsgFilter *MsgFilterCreator::createAndMsgFilter(const std::string &andDesc) {
    StringTokenizer st(
        andDesc, OPERATOR_AND_FILTER_SEPARATOR, StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    if (st.getNumTokens() == 0) {
        return nullptr;
    }
    AndMsgFilter *andFilter = new AndMsgFilter();
    for (size_t i = 0; i < st.getNumTokens(); i++) {
        size_t pos1 = st[i].find(OPERATOR_CONTAIN_FILTER_SEPARATOR);
        size_t pos2 = st[i].find(OPERATOR_IN_FILTER_SEPARATOR);
        MsgFilter *msgFilter = nullptr;
        if (pos1 != string::npos) {
            msgFilter = createContainMsgFilter(st[i]);
        } else if (pos2 != string::npos) {
            msgFilter = createInMsgFilter(st[i]);
        }
        if (msgFilter == nullptr) {
            DELETE_AND_SET_NULL(andFilter);
            return nullptr;
        } else {
            andFilter->addMsgFilter(msgFilter);
        }
    }
    return andFilter;
}

} // namespace filter
} // namespace swift
