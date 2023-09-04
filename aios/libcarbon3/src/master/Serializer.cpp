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
#include "master/Serializer.h"
#include "carbon/Log.h"

using namespace std;
using namespace hippo;

BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, Serializer);

Serializer::Serializer(LeaderSerializer *serializer, const string &id) {
    _leaderSerializer = serializer;
    _id = id;
}

Serializer::~Serializer() {
    delete _leaderSerializer;
}

bool Serializer::read(string &content) const {
    return _leaderSerializer->read(content);
}

bool Serializer::write(const std::string &content) {
    return _leaderSerializer->write(content);
}

bool Serializer::checkExist(bool &bExist) {
    return _leaderSerializer->checkExist(bExist);
}


END_CARBON_NAMESPACE(master);

