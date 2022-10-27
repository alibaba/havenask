#ifndef ISEARCH_BS_NORMALSORTDOCSORTER_H
#define ISEARCH_BS_NORMALSORTDOCSORTER_H

#include <autil/mem_pool/Pool.h>
#include <autil/ConstString.h>
#include <autil/mem_pool/PoolVector.h>
#include <autil/mem_pool/pool_allocator.h>
#include <unordered_map>
#include <tr1/functional>
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/builder/SortDocumentSorter.h"
#include "build_service/builder/NormalSortDocConvertor.h"

namespace build_service {
namespace builder {

struct SimpleHash {
    size_t operator()(const autil::ConstString &constString) const
    {
        return std::tr1::_Fnv_hash::hash(constString.data(), constString.size());
    }
};

class NormalSortDocSorter : public SortDocumentSorter
{
public:
    typedef autil::mem_pool::pool_allocator<std::pair<const autil::ConstString, std::pair<int32_t, int32_t> > > _map_allocator_type;
    typedef std::unordered_map<autil::ConstString, std::pair<int32_t, int32_t>,
        SimpleHash,
        std::equal_to<autil::ConstString>,
        _map_allocator_type
        > DocumentPkPosMap;
    typedef autil::mem_pool::PoolVector<uint32_t> PosVector;
    typedef autil::mem_pool::PoolVector<PosVector> MergePosVector;
    
public:
    NormalSortDocSorter(std::vector<SortDocument>& documentVec,
                         NormalSortDocConvertor* converter,
                         bool hasSub)
        : _documentVec(documentVec)
        , _converter(converter)
        , _documentPkPosMap(
            new DocumentPkPosMap(10, DocumentPkPosMap::hasher(),
                                 DocumentPkPosMap::key_equal(),
                                 _map_allocator_type(_pool.get())))
        , _mergePosVector(new MergePosVector(_pool.get()))
        , _hasSub(hasSub)
    {
    }
    
    ~NormalSortDocSorter() {
    }
private:
    NormalSortDocSorter(const NormalSortDocSorter &);
    NormalSortDocSorter& operator=(const NormalSortDocSorter &);

public:
    inline void push(const SortDocument& sortDoc, uint32_t pos) {
        const __typeof__(sortDoc._pk) &pkStr = sortDoc._pk;
        DocumentPkPosMap::iterator iter = _documentPkPosMap->find(pkStr);
        
        if (iter == _documentPkPosMap->end()) {
            (*_documentPkPosMap)[pkStr] = std::make_pair(pos, -1);
            return;
        }
        
        if (sortDoc._docType == ADD_DOC) {
            iter->second.first = pos;
            iter->second.second = -1;
        } else {
            if (iter->second.second != -1) {
                (*_mergePosVector)[iter->second.second].push_back(pos);
            } else {
                PosVector tmpVec(_pool.get());
                tmpVec.push_back(pos);
                (*_mergePosVector).push_back(tmpVec);
                iter->second.second = (*_mergePosVector).size() - 1;
            }
        }
    }
    void sort(std::vector<uint32_t>& sortDocVec) override;

private:
    std::vector<SortDocument> &_documentVec;
    NormalSortDocConvertor* _converter;
    std::unique_ptr<DocumentPkPosMap> _documentPkPosMap;
    std::unique_ptr<MergePosVector> _mergePosVector;
    bool _hasSub;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(NormalSortDocSorter);

}
}

#endif //ISEARCH_BS_NORMALSORTDOCSORTER_H
