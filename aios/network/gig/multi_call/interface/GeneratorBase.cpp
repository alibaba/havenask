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
#include "aios/network/gig/multi_call/interface/GeneratorBase.h"

using namespace std;

namespace multi_call {

GeneratorBase::GeneratorBase(const std::string &clusterName, const std::string &bizName, const std::string &strategy,
                             const std::string &requestId, SourceIdTy sourceId, VersionTy version,
                             VersionTy preferVersion)
    : _clusterName(clusterName)
    , _bizName(bizName)
    , _strategy(strategy)
    , _requestId(requestId)
    , _sourceId(sourceId)
    , _version(version)
    , _preferVersion(preferVersion)
    , _requestType(RT_NORMAL)
    , _disableRetry(false)
    , _disableProbe(false)
    , _disableDegrade(false)
    , _ignoreWeightLabelInConsistentHash(false)
{
}

GeneratorBase::~GeneratorBase() {}

} // namespace multi_call
