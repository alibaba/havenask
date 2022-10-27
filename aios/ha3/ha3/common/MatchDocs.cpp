#include <ha3/common/MatchDocs.h>
#include <ha3/common/CommonDef.h>
#include <ha3/common/GlobalIdentifier.h>
#include <iostream>

using namespace autil;
using namespace autil::legacy;
using namespace std;
using namespace matchdoc;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, MatchDocs);

MatchDocs::MatchDocs() {
    _totalMatchDocs = 0;
    _actualMatchDocs = 0;
    _serializeLevel = SL_QRS;
}

MatchDocs::~MatchDocs() {
    _allocator.reset(); // auto deallocate by allocator deconstruct
    _matchDocs.clear();
}

uint32_t MatchDocs::size() const {
    return _matchDocs.size();
}

void MatchDocs::resize(uint32_t count) {
    _matchDocs.resize(count, INVALID_MATCHDOC);
}

docid_t MatchDocs::getDocId(uint32_t pos) const {
    if ( pos >= _matchDocs.size()) {
        return INVALID_DOCID;
    } else {
        return _matchDocs[pos].getDocId();
    }
}

MatchDoc MatchDocs::getMatchDoc(uint32_t pos) const {
    if (pos >= _matchDocs.size()) {
        return INVALID_MATCHDOC;
    }
    return _matchDocs[pos];
}

MatchDoc MatchDocs::stealMatchDoc(uint32_t pos) {
    MatchDoc tmp = _matchDocs[pos];
    _matchDocs[pos] = INVALID_MATCHDOC;
    return tmp;
}

void MatchDocs::stealMatchDocs(std::vector<MatchDoc> &matchDocVect) {
    assert(matchDocVect.empty());
    swap(_matchDocs, matchDocVect);
}
//todo maybe delete
// void MatchDocs::setHashId(hashid_t hashid) {
//     auto hashIdRef = _allocator->getHashIdRef();
//     for (auto it = _matchDocs.begin(); it != _matchDocs.end(); ++it) {
//         auto &identifier = docIdRef->getReference(*it);
//         identifier.setHashId(hashid);
//     }
// }

// hashid_t MatchDocs::getHashId(uint32_t pos) const {
//     if (pos < _matchDocs.size()) {
//         auto docIdRef = _allocator->getDocIdentifierRef();
//         auto &identifier = docIdRef->getReference(_matchDocs[pos]);
//         return identifier.getHashId();
//     }
//     return hashid_t(-1);
// }

MatchDoc& MatchDocs::getMatchDoc(uint32_t pos) {
    assert(pos < _matchDocs.size());
    return _matchDocs[pos];
}

uint32_t MatchDocs::totalMatchDocs() const {
    return _totalMatchDocs;
}

void MatchDocs::setTotalMatchDocs(uint32_t totalMatchDocs) {
    _totalMatchDocs = totalMatchDocs;
}

uint32_t MatchDocs::actualMatchDocs() const {
    return _actualMatchDocs;
}

void MatchDocs::setActualMatchDocs(uint32_t actualMatchDocs) {
    _actualMatchDocs = actualMatchDocs;
}

void MatchDocs::addMatchDoc(MatchDoc matchDoc) {
    _matchDocs.push_back(matchDoc);
}

void MatchDocs::serialize(DataBuffer &dataBuffer) const {
    dataBuffer.writeBytes(&_totalMatchDocs, sizeof(uint32_t));
    dataBuffer.writeBytes(&_actualMatchDocs, sizeof(uint32_t));
    bool bExisted = (_allocator != NULL);
    dataBuffer.writeBytes(&bExisted, sizeof(bExisted));
    if (bExisted) {
        auto length_before = dataBuffer.getDataLen();
        _allocator->setSortRefFlag(false);
        _allocator->serialize(dataBuffer, _matchDocs, _serializeLevel);
        auto length_after = dataBuffer.getDataLen();
        HA3_LOG(TRACE3, "serializer serilize length[%d]", length_after - length_before);
    }
}

void MatchDocs::deserialize(DataBuffer &dataBuffer, autil::mem_pool::Pool *pool) {
    dataBuffer.readBytes(&_totalMatchDocs, sizeof(uint32_t));
    dataBuffer.readBytes(&_actualMatchDocs, sizeof(uint32_t));
    bool bExisted = false;
    dataBuffer.readBytes(&bExisted, sizeof(bExisted));
    if (!bExisted) {
        return;
    }
    _allocator.reset(new Ha3MatchDocAllocator(pool));
    auto length_before = dataBuffer.getDataLen();
    _allocator->deserialize(dataBuffer, _matchDocs);
    auto length_after = dataBuffer.getDataLen();
    HA3_LOG(TRACE3, "deserializer deserilize length[%d]", length_before - length_after);
}

void MatchDocs::insertMatchDoc(uint32_t pos, MatchDoc matchDoc) {
    assert(pos < _matchDocs.size());
    // if (_matchDocs[pos]) {
    //     delete _matchDocs[pos];
    //     _matchDocs[pos] = INVALID_MATCHDOC;
    // }
    _matchDocs[pos] = matchDoc;
}

void MatchDocs::setClusterId(clusterid_t clusterId) {
    if (!_allocator) {
        return;
    }
    auto clusterIdRef =
        _allocator->declareWithConstructFlag<clusterid_t>(
                CLUSTER_ID_REF, CLUSTER_ID_REF_GROUP, false, SL_QRS);
    _allocator->extend();
    for (const auto &doc : _matchDocs) {
        auto &id = clusterIdRef->getReference(doc);
        id = clusterId;
    }
}

#define SET_EXTRA_DOCID_HELPER(type, name, refName)                     \
    do {                                                                \
        auto ref = _allocator->findReference<type>(refName);            \
        if (!ref) {                                                     \
            break;                                                      \
        }                                                               \
        for (auto it = _matchDocs.begin();                              \
             it != _matchDocs.end(); ++it)                              \
        {                                                               \
            ref->set((*it), name);                                      \
        }                                                               \
    } while(0)

void MatchDocs::setGlobalIdInfo(hashid_t hashId,
                                versionid_t indexVersion,
                                FullIndexVersion fullIndexVersion,
                                uint32_t ip,
                                uint8_t phaseOneInfoFlag)
{
    if (!_allocator) {
        return;
    }
    _allocator->declareWithConstructFlag<hashid_t>(HASH_ID_REF,
            common::HA3_GLOBAL_INFO_GROUP, false, SL_QRS);
    if (PHASE_ONE_HAS_INDEX_VERSION(phaseOneInfoFlag)) {
        _allocator->declareWithConstructFlag<versionid_t>(INDEXVERSION_REF,
                common::HA3_GLOBAL_INFO_GROUP, false, SL_QRS);
    }
    if (PHASE_ONE_HAS_FULL_VERSION(phaseOneInfoFlag)) {
        _allocator->declareWithConstructFlag<FullIndexVersion>(FULLVERSION_REF,
                common::HA3_GLOBAL_INFO_GROUP, false, SL_QRS);
    }
    if (PHASE_ONE_HAS_IP(phaseOneInfoFlag)) {
        _allocator->declareWithConstructFlag<uint32_t>(IP_REF,
                common::HA3_GLOBAL_INFO_GROUP, false, SL_QRS);
    }
    _allocator->extend();
    SET_EXTRA_DOCID_HELPER(hashid_t, hashId, HASH_ID_REF);
    SET_EXTRA_DOCID_HELPER(versionid_t, indexVersion, INDEXVERSION_REF);
    SET_EXTRA_DOCID_HELPER(FullIndexVersion, fullIndexVersion, FULLVERSION_REF);
    SET_EXTRA_DOCID_HELPER(uint32_t, ip, IP_REF);
}

ReferenceBase* MatchDocs::getRawPrimaryKeyRef() const {
    return _allocator->findReferenceWithoutType(
            RAW_PRIMARYKEY_REF);
}

#define GET_EXTRA_DOCID_REF_HELPER(type, name, vr_name) \
    Reference<type>* MatchDocs::get##name##Ref() const {      \
        if (!_allocator) {                   \
            return NULL;                                \
        }                                                               \
        return _allocator->findReference<type>(vr_name); \
    }

GET_EXTRA_DOCID_REF_HELPER(hashid_t, HashId, HASH_ID_REF)
GET_EXTRA_DOCID_REF_HELPER(clusterid_t, ClusterId, CLUSTER_ID_REF)
GET_EXTRA_DOCID_REF_HELPER(uint64_t, PrimaryKey64, PRIMARYKEY_REF)
GET_EXTRA_DOCID_REF_HELPER(primarykey_t, PrimaryKey128, PRIMARYKEY_REF)
GET_EXTRA_DOCID_REF_HELPER(versionid_t, IndexVersion, INDEXVERSION_REF)
GET_EXTRA_DOCID_REF_HELPER(FullIndexVersion, FullIndexVersion, FULLVERSION_REF)
GET_EXTRA_DOCID_REF_HELPER(uint32_t, Ip, IP_REF)

bool MatchDocs::hasPrimaryKey() const {
    if (!_allocator) {
        return false;
    }
    if (_allocator->findReferenceWithoutType(
                    PRIMARYKEY_REF))
    {
        return true;
    }
    return _allocator->findReferenceWithoutType(
            RAW_PRIMARYKEY_REF);
}

END_HA3_NAMESPACE(common);
