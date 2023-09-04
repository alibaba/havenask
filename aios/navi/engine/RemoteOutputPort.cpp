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
#include "navi/engine/RemoteOutputPort.h"

namespace navi {

RemoteOutputPort::RemoteOutputPort(const EdgeDef &def,
                                   SubGraphBorder *border)
    : Port(def.input(), def.output(), IOT_OUTPUT, PST_SERIALIZE_DATA, border)
{
    _logger.addPrefix("RemoteOutputPort");
}

RemoteOutputPort::~RemoteOutputPort() {
}

void RemoteOutputPort::setCallback(NaviPartId partId, bool eof) {
}

void RemoteOutputPort::getCallback(NaviPartId partId, bool eof) {
    // schedule remote input
}

}
