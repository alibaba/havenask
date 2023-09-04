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
#include "util/SignatureUtil.h"
#include "autil/HashAlgorithm.h"

using namespace std;
using namespace autil;

BEGIN_HIPPO_NAMESPACE(util);

HIPPO_LOG_SETUP(util, SignatureUtil);

SignatureUtil::SignatureUtil() {
}

SignatureUtil::~SignatureUtil() {
}

int64_t SignatureUtil::signature(const proto::ProcessLaunchContext &launchContext) {
    string buf;
    launchContext.SerializeToString(&buf);
    return HashAlgorithm::hashString64(buf.c_str(), buf.length());
}

int64_t SignatureUtil::signature(const string &podLaunchDesc) {
    return HashAlgorithm::hashString64(
            podLaunchDesc.c_str(), podLaunchDesc.length());
}


END_HIPPO_NAMESPACE(util);

