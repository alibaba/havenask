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
#include "indexlib/util/counter/CounterCreator.h"

#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/Counter.h"
#include "indexlib/util/counter/MultiCounter.h"
#include "indexlib/util/counter/StateCounter.h"
#include "indexlib/util/counter/StringCounter.h"

using namespace std;

using namespace indexlib::util;
namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, CounterCreator);

CounterCreator::CounterCreator() {}

CounterCreator::~CounterCreator() {}

CounterBasePtr CounterCreator::CreateCounter(const string& path, CounterBase::CounterType type)
{
    CounterBasePtr counter;
    switch (type) {
    case CounterBase::CT_DIRECTORY:
        counter.reset(new MultiCounter(path));
        break;
    case CounterBase::CT_ACCUMULATIVE:
        counter.reset(new AccumulativeCounter(path));
        break;
    case CounterBase::CT_STATE:
        counter.reset(new StateCounter(path));
        break;
    case CounterBase::CT_STRING:
        counter.reset(new StringCounter(path));
        break;
    default:
        assert(false);
    }
    return counter;
}
}} // namespace indexlib::util
