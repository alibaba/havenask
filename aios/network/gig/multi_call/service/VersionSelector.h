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
#ifndef ISEARCH_MULTI_CALL_VERSIONSELECTOR_H
#define ISEARCH_MULTI_CALL_VERSIONSELECTOR_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/service/CachedRequestGenerator.h"
#include "aios/network/gig/multi_call/service/SearchServiceSnapshot.h"
#include "aios/network/gig/multi_call/util/RandomGenerator.h"
#include <string>
#include <vector>

namespace multi_call {

class VersionSelector {
public:
    VersionSelector(const std::string &bizName)
        : _bizName(bizName),
          _randomGenerator(autil::TimeUtility::currentTimeInMicroSeconds()) {}
    virtual ~VersionSelector() {}

private:
    VersionSelector(const VersionSelector &) = delete;
    VersionSelector &operator=(const VersionSelector &) = delete;

public:
    virtual bool select(const std::shared_ptr<CachedRequestGenerator> &generator,
                        const SearchServiceSnapshotPtr &snapshot) = 0;

    virtual const SearchServiceSnapshotInVersionPtr &
    getBizVersionSnapshot() = 0;

    virtual const SearchServiceSnapshotInVersionPtr &
    getProbeBizVersionSnapshot() = 0;

    virtual const std::vector<SearchServiceSnapshotInVersionPtr> &
    getCopyBizVersionSnapshots() = 0;

protected:
    bool needCopy(size_t copyProviderCount, size_t normalProviderCount);

protected:
    std::string _bizName;
    RandomGenerator _randomGenerator;
};

MULTI_CALL_TYPEDEF_PTR(VersionSelector);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_VERSIONSELECTOR_H
