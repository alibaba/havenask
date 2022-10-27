#ifndef ISEARCH_MATCHDOCS_H
#define ISEARCH_MATCHDOCS_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/common/CommonDef.h>
#include <indexlib/index_base/index_meta/version.h>

BEGIN_HA3_NAMESPACE(common);

class MatchDocs
{
public:
    MatchDocs();
    ~MatchDocs();
public:
    uint32_t size() const;
    void resize(uint32_t count);

    docid_t getDocId(uint32_t pos) const;

    matchdoc::MatchDoc getMatchDoc(uint32_t pos) const;
    matchdoc::MatchDoc &getMatchDoc(uint32_t pos);

    matchdoc::MatchDoc stealMatchDoc(uint32_t pos);

    void stealMatchDocs(std::vector<matchdoc::MatchDoc> &matchDocVect);

    // todo
    // void setHashId(hashid_t pid);
    // hashid_t getHashId(uint32_t pos) const;

    void setClusterId(clusterid_t clusterId);
    void addMatchDoc(matchdoc::MatchDoc matchDoc);
    void insertMatchDoc(uint32_t pos, matchdoc::MatchDoc matchDoc);

    void setTotalMatchDocs(uint32_t totalMatchDocs);
    uint32_t totalMatchDocs() const;

    void setActualMatchDocs(uint32_t actualMatchDocs);
    uint32_t actualMatchDocs() const;

    void setMatchDocAllocator(const Ha3MatchDocAllocatorPtr &allocator) {
        _allocator = allocator;
    }
    const Ha3MatchDocAllocatorPtr &getMatchDocAllocator() const {
        return _allocator;
    }
    std::string toString() const;
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer, autil::mem_pool::Pool *pool);

    bool hasPrimaryKey() const;
    void setGlobalIdInfo(hashid_t hashId, versionid_t indexVersion, 
                         FullIndexVersion fullIndexVersion, uint32_t ip,
                         uint8_t phaseOneInfoFlag);
    std::vector<matchdoc::MatchDoc>& getMatchDocsVect() {
        return _matchDocs;
    }

    matchdoc::ReferenceBase* getRawPrimaryKeyRef() const;
    matchdoc::Reference<hashid_t> *getHashIdRef() const;
    matchdoc::Reference<clusterid_t> *getClusterIdRef() const;
    matchdoc::Reference<primarykey_t> *getPrimaryKey128Ref() const;
    matchdoc::Reference<uint64_t> *getPrimaryKey64Ref() const;
    matchdoc::Reference<FullIndexVersion> *getFullIndexVersionRef() const;
    matchdoc::Reference<versionid_t> *getIndexVersionRef() const;
    matchdoc::Reference<uint32_t> *getIpRef() const;

    void setSerializeLevel(uint8_t serializeLevel) {
        assert(SL_NONE < serializeLevel &&
               serializeLevel <= SL_MAX);
        _serializeLevel = serializeLevel;
    }
private:
    std::vector<matchdoc::MatchDoc> _matchDocs;
    Ha3MatchDocAllocatorPtr _allocator;
    uint32_t _totalMatchDocs;
    uint32_t _actualMatchDocs;
    // for searcher cache
    uint8_t _serializeLevel;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(common);

#endif //ISEARCH_MATCHDOCS_H
