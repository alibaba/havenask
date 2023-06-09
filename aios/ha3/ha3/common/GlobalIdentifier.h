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

#include <stdint.h>
#include <memory>
#include <ostream>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/LongHashValue.h"
#include "ha3/isearch.h"
#include "indexlib/indexlib.h"

namespace autil {
class DataBuffer;
}  // namespace autil

namespace isearch {
namespace common {

#define PHASE_ONE_HAS_PK64(phaseOneInfoFlag)   \
    (phaseOneInfoFlag & common::pob_primarykey64_flag)

#define PHASE_ONE_HAS_PK128(phaseOneInfoFlag)  \
    (phaseOneInfoFlag & common::pob_primarykey128_flag)

#define PHASE_ONE_HAS_FULL_VERSION(phaseOneInfoFlag)   \
    (phaseOneInfoFlag & common::pob_fullversion_flag)

#define PHASE_ONE_HAS_INDEX_VERSION(phaseOneInfoFlag)  \
    (phaseOneInfoFlag & common::pob_indexversion_flag)

#define PHASE_ONE_HAS_IP(phaseOneInfoFlag) \
    (phaseOneInfoFlag & common::pob_ip_flag)

#define PHASE_ONE_HAS_RAWPK(phaseOneInfoFlag) \
    (phaseOneInfoFlag & common::pob_rawpk_flag)

enum PhaseOneBasicInfoMask {
    pob_primarykey64_flag = 1 << 0,
    pob_primarykey128_flag = 1 << 1,
    pob_indexversion_flag = 1 << 2,
    pob_fullversion_flag = 1 << 3,
    pob_ip_flag = 1 << 4,
    pob_rawpk_flag = 1 << 5
};

struct ExtraDocIdentifier {
public:
    ExtraDocIdentifier() {
        primaryKey = 0;
        indexVersion = 0;
        fullIndexVersion = 0;
        ip = 0;
    }
    ~ExtraDocIdentifier() {}
public:
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
public:
    inline bool operator<(const ExtraDocIdentifier& rhs) const {
        if (fullIndexVersion != rhs.fullIndexVersion) {
            return fullIndexVersion < rhs.fullIndexVersion;
        }
        if (indexVersion != rhs.indexVersion) {
            return indexVersion < rhs.indexVersion;
        }
        return primaryKey < rhs.primaryKey;
    }

    inline bool operator>(const ExtraDocIdentifier& rhs) const {
        return rhs < *this;
    }
    inline bool operator==(const ExtraDocIdentifier& rhs) const {
        return  primaryKey == rhs.primaryKey
            && indexVersion == rhs.indexVersion
            && fullIndexVersion == rhs.fullIndexVersion;
    }
public:
    primarykey_t primaryKey;                // 64bit ? 128bit?
    versionid_t  indexVersion;              // == FETCH_SUMMARY_TYPE == BY_DOCID
    FullIndexVersion fullIndexVersion;      // == FETCH_SUMMARY_TYPE == BY_DOCID || doDedup
    uint32_t ip;                            // same cluster?
};

class GlobalIdentifier
{
public:
    GlobalIdentifier() {
        setDocId(0);
        setHashId(0);
        setClusterId(0);
        _pos = (uint32_t)-1;
    }
    //construct function used only for test
    GlobalIdentifier(docid_t docId,
                     hashid_t hashId,
                     versionid_t version = 0,
                     clusterid_t clusterId = 0,
                     primarykey_t primaryKey = 0,
                     FullIndexVersion fullIndexVersion = 0,
                     uint32_t ip = 0)
    {
        setDocId(docId);
        setHashId(hashId);
        _extraDocIdentifier.indexVersion = version;
        setClusterId(clusterId);
        _extraDocIdentifier.primaryKey = primaryKey;
        _extraDocIdentifier.fullIndexVersion = fullIndexVersion;
        _extraDocIdentifier.ip = ip;
        _pos = (uint32_t)-1;
    }

public:
    inline docid_t getDocId() const {
        return _docId;
    }
    inline void setDocId(docid_t docid) {
        _docId = docid;
    }
    inline hashid_t getHashId() const {
        return _hashId;
    }
    inline void setHashId(hashid_t hashId) {
        _hashId = hashId;
    }
    inline void setClusterId(clusterid_t clusterId) {
        _clusterId = clusterId;
    }
    inline clusterid_t getClusterId() const {
        return _clusterId;
    }
    inline primarykey_t getPrimaryKey() const;
    inline void setPrimaryKey(primarykey_t primaryKey);
    inline versionid_t getIndexVersion() const;
    inline void setIndexVersion(const versionid_t &indexVersion);
    inline void setFullIndexVersion(FullIndexVersion version);
    inline FullIndexVersion getFullIndexVersion() const;
    inline uint32_t getIp() const;
    inline void setIp(uint32_t ip);
    inline uint32_t getPosition() const;
    inline void setPosition(uint32_t pos);
public:
    inline void setExtraDocIdentifier(const ExtraDocIdentifier &meta) {
        _extraDocIdentifier = meta;
    }
    ExtraDocIdentifier &getExtraDocIdentifier() {
        return _extraDocIdentifier;
    }
public:
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);

    // todo
    inline bool operator<(const GlobalIdentifier &rhs) const {
        if (_docId != rhs._docId) {
            return _docId < rhs._docId;
        }
        if (_hashId != rhs._hashId) {
            return _hashId < rhs._hashId;
        }
        if (_clusterId != rhs._clusterId) {
            return _clusterId < rhs._clusterId;
        }
        return _extraDocIdentifier < rhs._extraDocIdentifier;
    }
    inline bool operator>(const GlobalIdentifier& rhs) const {
        return rhs < *this;
    }
    inline bool operator==(const GlobalIdentifier& rhs) const {
        return _docId == rhs._docId
            && _hashId == rhs._hashId
            && _clusterId == rhs._clusterId
            && _extraDocIdentifier == rhs._extraDocIdentifier;
    }
public:
    std::string toString() const;
protected:
    docid_t _docId;
    hashid_t _hashId;
    clusterid_t _clusterId;
    ExtraDocIdentifier _extraDocIdentifier;
    // for merge summary
    uint32_t _pos;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<GlobalIdentifier> GlobalIdentifierPtr;

////////////////////////////////////////////////////////////////////
// inline
inline primarykey_t GlobalIdentifier::getPrimaryKey() const {
    return _extraDocIdentifier.primaryKey;
}

inline void GlobalIdentifier::setPrimaryKey(primarykey_t primaryKey) {
    _extraDocIdentifier.primaryKey = primaryKey;
}

inline versionid_t GlobalIdentifier::getIndexVersion() const {
    return _extraDocIdentifier.indexVersion;
}

inline void GlobalIdentifier::setIndexVersion(const versionid_t &indexVersion) {
    _extraDocIdentifier.indexVersion = indexVersion;
}

inline void GlobalIdentifier::setFullIndexVersion(FullIndexVersion version) {
    _extraDocIdentifier.fullIndexVersion = version;
}

inline FullIndexVersion GlobalIdentifier::getFullIndexVersion() const {
    return _extraDocIdentifier.fullIndexVersion;
}

inline uint32_t GlobalIdentifier::getIp() const {
    return _extraDocIdentifier.ip;
}

inline void GlobalIdentifier::setIp(uint32_t ip) {
    _extraDocIdentifier.ip = ip;
}

inline uint32_t GlobalIdentifier::getPosition() const {
    return _pos;
}

inline void GlobalIdentifier::setPosition(uint32_t pos) {
    _pos = pos;
}


inline std::ostream& operator<<(std::ostream& os, const ExtraDocIdentifier& meta) {
    os << "fullIndexVersion:" << meta.fullIndexVersion
       << " indexVersion:" << meta.indexVersion
       << " ip:" << meta.ip
       << " primaryKey:" << meta.primaryKey;
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const GlobalIdentifier& gid) {
    os << gid.toString();
    return os;
}

inline std::string GlobalIdentifier::toString() const {
    std::stringstream ss;
    ss << _extraDocIdentifier
        << " docId:" << getDocId()
        << " hashId: " << getHashId()
        << " clusterId: " << getClusterId();
    return ss.str();
}

} // namespace common
} // namespace isearch
