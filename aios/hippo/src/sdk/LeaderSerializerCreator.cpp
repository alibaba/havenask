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
#include "hippo/LeaderSerializerCreator.h"
#include "util/PathUtil.h"
#include "sdk/LeaderSerializerImpl.h"

using namespace std;

USE_HIPPO_NAMESPACE(util);
USE_HIPPO_NAMESPACE(sdk);
USE_HIPPO_NAMESPACE(common);

namespace hippo {

LeaderSerializerCreator::LeaderSerializerCreator() {
}

LeaderSerializerCreator::~LeaderSerializerCreator() {
}

LeaderSerializer* LeaderSerializerCreator::create(
        worker_framework::LeaderChecker *leaderChecker,
        const std::string &zookeeperRoot, const std::string &fileName,
        const std::string &backupRoot)
{
    if (NULL == leaderChecker) {
        return NULL;
    }
    string zkHost = PathUtil::getHostFromZkPath(zookeeperRoot);
    string zkBasePath = PathUtil::getPathFromZkPath(zookeeperRoot);
    if (zkBasePath.empty()) {
        return NULL;
    }
    return new LeaderSerializerImpl(fileName, new LeaderState(
                    leaderChecker, leaderChecker->getZkWrapper(),
                    zkBasePath, backupRoot));
}

}
