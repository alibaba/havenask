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
#include "suez/table/wal/WALStrategy.h"

#include "NoneWAL.h"
#include "QueueWAL.h"
#include "RealtimeSwiftWAL.h"
#include "SyncWal.h"
#include "autil/Log.h"
#include "build_service/util/SwiftClientCreator.h"
#include "suez/table/wal/WALConfig.h"

using namespace std;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, WALStrategy);

unique_ptr<WALStrategy> WALStrategy::create(const WALConfig &config, const SwiftClientCreatorPtr &swiftClientCreator) {
    const string &strategyName = config.strategy;
    AUTIL_LOG(INFO, "create wal startegyName:%s", strategyName.c_str());
    if (strategyName == "none" || strategyName.empty()) {
        return make_unique<NoneWAL>();
    } else if (strategyName == "sync") {
        return make_unique<SyncWal>();
    } else if (strategyName == "realtime_swift") {
        auto strategy = make_unique<RealtimeSwiftWAL>();
        if (!strategy->init(config, swiftClientCreator)) {
            return nullptr;
        }
        return strategy;
    } else if (strategyName == "queue") {
        auto strategy = make_unique<QueueWAL>();
        if (!strategy->init(config)) {
            return nullptr;
        }
        return strategy;
    } else {
        AUTIL_LOG(ERROR, "unknown strategy: %s", strategyName.c_str());
        return nullptr;
    }
}

bool WALStrategy::needSink(const std::string &strategyType) { return strategyType == "realtime_swift"; }

} // namespace suez
