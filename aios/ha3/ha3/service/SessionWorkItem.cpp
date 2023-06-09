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
#include "ha3/service/SessionWorkItem.h"

#include <assert.h>

#include "ha3/service/Session.h"

namespace isearch {
namespace service {
AUTIL_LOG_SETUP(ha3, SessionWorkItem);

SessionWorkItem::SessionWorkItem(Session *session){
    assert(session);
    _session = session;
}

SessionWorkItem::~SessionWorkItem() {
}

void SessionWorkItem::process() {
    _session->processRequest();
}
void SessionWorkItem::drop() {
    _session->dropRequest();
    destroy();
}


} // namespace service
} // namespace isearch

