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
#ifndef HIPPO_SIGNATUREUTIL_H
#define HIPPO_SIGNATUREUTIL_H

#include "util/Log.h"
#include "common/common.h"
#include "hippo/proto/Common.pb.h"

BEGIN_HIPPO_NAMESPACE(util);

class SignatureUtil
{
public:
    SignatureUtil();
    ~SignatureUtil();
private:
    SignatureUtil(const SignatureUtil &);
    SignatureUtil& operator=(const SignatureUtil &);
public:
    static int64_t signature(const proto::ProcessLaunchContext &launchContext);
    static int64_t signature(const std::string &podLaunchDesc);
    
private:
    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(SignatureUtil);

END_HIPPO_NAMESPACE(util);

#endif //HIPPO_SIGNATUREUTIL_H
