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
#include "suez/table/DummyTableBuilder.h"

#include <cassert>

namespace suez {

bool DummyTableBuilder::start() { return true; }

void DummyTableBuilder::stop() {}

void DummyTableBuilder::suspend() {}

void DummyTableBuilder::resume() {}

void DummyTableBuilder::skip(int64_t timestamp) {}

void DummyTableBuilder::forceRecover() {}

bool DummyTableBuilder::isRecovered() { return true; }

bool DummyTableBuilder::needCommit() const { return false; }

std::pair<bool, TableVersion> DummyTableBuilder::commit() {
    assert(false);
    return {false, TableVersion()};
}

} // namespace suez
