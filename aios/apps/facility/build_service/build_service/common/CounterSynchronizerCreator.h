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

#include "build_service/common/CounterSynchronizer.h"
#include "build_service/common_define.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/util/Log.h"
#include "indexlib/util/counter/CounterMap.h"

namespace build_service { namespace common {

class CounterSynchronizerCreator
{
public:
    CounterSynchronizerCreator();
    ~CounterSynchronizerCreator();

private:
    CounterSynchronizerCreator(const CounterSynchronizerCreator&);
    CounterSynchronizerCreator& operator=(const CounterSynchronizerCreator&);

public:
    static bool validateCounterConfig(const config::CounterConfigPtr& counterConfig);

    static indexlib::util::CounterMapPtr loadCounterMap(const config::CounterConfigPtr& counterConfig,
                                                        bool& loadFromExisted);

    static CounterSynchronizer* create(const indexlib::util::CounterMapPtr& counterMap,
                                       const config::CounterConfigPtr& counterConfig);

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CounterSynchronizerCreator);

}} // namespace build_service::common
