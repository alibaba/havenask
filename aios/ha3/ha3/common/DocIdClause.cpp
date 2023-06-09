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
#include "ha3/common/DocIdClause.h"

#include <iosfwd>

#include "autil/DataBuffer.h"

#include "ha3/common/GlobalIdentifier.h"
#include "ha3/isearch.h"
#include "ha3/proto/BasicDefs.pb.h"
#include "autil/Log.h"

using namespace std;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, DocIdClause);

void DocIdClause::getGlobalIdVector(const proto::Range &hashIdRange,
                                    FullIndexVersion fullVersion,
                                    GlobalIdVector& gids) {
    gids.clear();
    for (GlobalIdVector::const_iterator it = _globalIdVector.begin();
         it != _globalIdVector.end(); ++it)
    {
        const GlobalIdentifier &gid = *it;
        uint32_t hashId = gid.getHashId();
        if (hashId >= hashIdRange.from()
            && hashId <= hashIdRange.to()
            && fullVersion == gid.getFullIndexVersion())
        {
            gids.push_back(gid);
        }
    }
}

void DocIdClause::getGlobalIdVector(const proto::Range &hashIdRange,
                                    GlobalIdVector& gids) {
    gids.clear();
    for (GlobalIdVector::const_iterator it = _globalIdVector.begin();
         it != _globalIdVector.end(); ++it)
    {
        const GlobalIdentifier &gid = *it;
        uint32_t hashId = gid.getHashId();
        if (hashId >= hashIdRange.from() && hashId <= hashIdRange.to()) {
            gids.push_back(gid);
        }
    }
}

void DocIdClause::getGlobalIdVectorMap(const proto::Range &hashIdRange,
                                       FullIndexVersion fullVersion,
                                       GlobalIdVectorMap &globalIdVectorMap)
{
    globalIdVectorMap.clear();

    for (GlobalIdVector::iterator it = _globalIdVector.begin();
         it != _globalIdVector.end(); ++it)
    {
        const GlobalIdentifier &gid = *it;
        uint32_t hashId = gid.getHashId();
        if (hashId >= hashIdRange.from()
            && hashId <= hashIdRange.to()
            && fullVersion == gid.getFullIndexVersion())
        {
            versionid_t incVersion = gid.getIndexVersion();
            globalIdVectorMap[incVersion].push_back(gid);
        }
    }
}

void DocIdClause::addGlobalId(const GlobalIdentifier &globalId) {
    _globalIdVector.push_back(globalId);
}

void DocIdClause::getHashidVersionSet(HashidVersionSet &hashidVersionSet) {
    hashidVersionSet.clear();
    for (GlobalIdVector::const_iterator it = _globalIdVector.begin();
         it != _globalIdVector.end(); ++it)
    {
        const GlobalIdentifier &gid = *it;
        hashidVersionSet.insert(std::make_pair(gid.getHashId(),
                        gid.getFullIndexVersion()));
    }
}

void DocIdClause::getHashidVector(vector<hashid_t> &hashids) {
    hashids.clear();
    for (GlobalIdVector::const_iterator it = _globalIdVector.begin();
         it != _globalIdVector.end(); ++it)
    {
        hashids.push_back((*it).getHashId());
    }
}

void DocIdClause::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_globalIdVector);
    dataBuffer.write(_termVector);
    dataBuffer.write(_queryString);
    dataBuffer.write(_summaryProfileName);
    dataBuffer.write(_signature);
}

void DocIdClause::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(_globalIdVector);
    dataBuffer.read(_termVector);
    dataBuffer.read(_queryString);
    dataBuffer.read(_summaryProfileName);
    dataBuffer.read(_signature);
}

} // namespace common
} // namespace isearch
