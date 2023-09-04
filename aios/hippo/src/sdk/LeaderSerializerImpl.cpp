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
#include "sdk/LeaderSerializerImpl.h"

using namespace std;
USE_HIPPO_NAMESPACE(common);

BEGIN_HIPPO_NAMESPACE(sdk);

HIPPO_LOG_SETUP(sdk, LeaderSerializerImpl);

LeaderSerializerImpl::LeaderSerializerImpl(const string &fileName,
        LeaderState *leaderState)
    : LeaderSerializer(fileName)
    , _leaderState(leaderState)
{
}

LeaderSerializerImpl::~LeaderSerializerImpl() {
    DELETE_AND_SET_NULL_HIPPO(_leaderState);
}

bool LeaderSerializerImpl::read(string &content) const {
    return _leaderState->read(_fileName, content);
}

bool LeaderSerializerImpl::write(const string &content) {
    return _leaderState->write(_fileName, content);
}

bool LeaderSerializerImpl::checkExist(bool &bExist) {
    return _leaderState->check(_fileName, bExist);
}

END_HIPPO_NAMESPACE(sdk);
