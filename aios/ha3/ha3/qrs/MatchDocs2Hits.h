#ifndef ISEARCH_MATCHDOCS2HITS_H
#define ISEARCH_MATCHDOCS2HITS_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Request.h>
#include <ha3/common/MatchDocs.h>
#include <ha3/common/Hits.h>

BEGIN_HA3_NAMESPACE(qrs);

class MatchDocs2Hits
{
public:
    MatchDocs2Hits();
    ~MatchDocs2Hits();
private:
    MatchDocs2Hits(const MatchDocs2Hits &);
    MatchDocs2Hits& operator=(const MatchDocs2Hits &);
public:
    common::Hits* convert(const common::MatchDocs *matchDocs, 
                          const common::RequestPtr &requestPtr,
                          const std::vector<common::SortExprMeta>& sortExprMetaVec,
                          common::ErrorResult& errResult,
                          const std::vector<std::string> &clusterList);
private:
    common::HitPtr convertOne(const matchdoc::MatchDoc matchDoc);
    void fillExtraDocIdentifier(const matchdoc::MatchDoc matchDoc,
                                common::ExtraDocIdentifier &extraDocIdentifier);
    void clear();
    void extractNames(const matchdoc::ReferenceVector &referVec, 
                      const std::string &prefix,
                      std::vector<std::string> &nameVec) const;
    bool initAttributeReferences(const common::RequestPtr &requestPtr,
                                 const common::Ha3MatchDocAllocatorPtr &allocatorPtr);
    void initUserData(const common::Ha3MatchDocAllocatorPtr &allocatorPtr);
    void initIdentifiers(const common::Ha3MatchDocAllocatorPtr &allocatorPtr,
                         const common::MatchDocs *matchDocs);
private:
    common::Hits *_hits;
    bool _hasPrimaryKey;
    std::vector<const matchdoc::ReferenceBase *> _sortReferVec;
    matchdoc::ReferenceVector _userDataReferVec;
    std::vector<std::string> _userDataNames;
    std::vector<std::pair<std::string, matchdoc::ReferenceBase *>> _attributes;
    const matchdoc::Reference<hashid_t> *_hashIdRef;
    const matchdoc::Reference<clusterid_t> *_clusterIdRef;
    matchdoc::Reference<common::Tracer> *_tracerRef;
    const matchdoc::Reference<uint64_t> *_primaryKey64Ref;
    const matchdoc::Reference<primarykey_t> *_primaryKey128Ref;
    const matchdoc::Reference<FullIndexVersion> *_fullVersionRef;
    const matchdoc::Reference<versionid_t> *_indexVersionRef;
    const matchdoc::Reference<uint32_t> *_ipRef;
    const matchdoc::ReferenceBase *_rawPkRef;
private:
    friend class MatchDocs2HitsTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(MatchDocs2Hits);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_MATCHDOCS2HITS_H
