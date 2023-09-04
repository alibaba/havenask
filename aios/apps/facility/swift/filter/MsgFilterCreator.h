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

#include <string>

#include "autil/Log.h"
#include "swift/common/Common.h"

namespace swift {
namespace filter {
class AndMsgFilter;
class InMsgFilter;
class ContainMsgFilter;
class MsgFilter;
} // namespace filter
} // namespace swift

namespace swift {
namespace filter {

class MsgFilterCreator {
public:
    static std::string OPERATOR_IN_FILTER_SEPARATOR;
    static std::string OPERATOR_AND_FILTER_SEPARATOR;
    static std::string OPERATOR_CONTAIN_FILTER_SEPARATOR;

public:
    MsgFilterCreator();
    ~MsgFilterCreator();

private:
    MsgFilterCreator(const MsgFilterCreator &);
    MsgFilterCreator &operator=(const MsgFilterCreator &);

public:
    static MsgFilter *createMsgFilter(const std::string &filterDesc);

private:
    static InMsgFilter *createInMsgFilter(const std::string &inDesc);
    static AndMsgFilter *createAndMsgFilter(const std::string &andDesc);
    static ContainMsgFilter *createContainMsgFilter(const std::string &containDesc);

private:
    friend class MsgFilterCreatorTest;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(MsgFilterCreator);

} // namespace filter
} // namespace swift
