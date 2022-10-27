#include "CheatSorter.h"
#include "autil/StringUtil.h"
using namespace std;
using namespace autil::mem_pool;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(rank);

BEGIN_HA3_NAMESPACE(sorter);
HA3_LOG_SETUP(sorter, CheatSorter);

#define SCORE_NAME "score"


class MatchDocComp {
public:
    MatchDocComp(const Comparator *comp) {
        _comp = comp;
    }
public:
    bool operator() (matchdoc::MatchDoc lft, matchdoc::MatchDoc rht) {
        return _comp->compare(rht, lft);
    }
private:
    const Comparator *_comp;
};

CheatSorter::CheatSorter(const std::string &name)
    : Sorter(name)
    , _comp(NULL)
    , _idAttributRef(NULL)
{ 
}

CheatSorter::CheatSorter(const CheatSorter &other)
    : Sorter(other.getSorterName())
    , _comp(other._comp)
    , _idAttributRef(other._idAttributRef)
{
}

CheatSorter::~CheatSorter() { 
    POOL_DELETE_CLASS(_comp);
}

Sorter* CheatSorter::clone() {
    return new CheatSorter(*this);
}


bool CheatSorter::beginSort(suez::turing::SorterProvider *provider_) {
    auto provider = dynamic_cast<SorterProvider *>(provider_);
    const matchdoc::ReferenceBase *ref = NULL;
    KeyValueMap kvMap = provider->getSortKVPairs();
    string valueStr = kvMap["sort_key"];
    bool sortFlag=false;
    if(valueStr.size() > 0){
        if (valueStr[0] == '+') {
            sortFlag = true;
            valueStr = valueStr.substr(1);
        } else if(valueStr[0] == '-'){
            valueStr = valueStr.substr(1);
            sortFlag = false;
        }
        ref = provider->requireAttributeWithoutType(valueStr);
    }else{
        ref = provider->findVariableReference<score_t>(suez::turing::SCORE_REF);
    }
    if(ref == NULL){
        return false;
    }
    string cheatIdStr = kvMap["cheat_ids"];
    fillCheatIDs(cheatIdStr);
    if(_cheatIDSet.size() > 0){
        _idAttributRef = provider->requireAttribute<int32_t>("id");
    }
    ComparatorItem item;
    item.first = ref;
    item.second = sortFlag;
    vector<ComparatorItem> items;
    items.push_back(item);
    _comp = provider->createComparator(items);
    return _comp != NULL;
}

void CheatSorter::fillCheatIDs(const string &cheatIdStr) {
    if(cheatIdStr.empty()) {
        return;
    }
    vector<string> strVec = autil::StringUtil::split(cheatIdStr, "|");
    istringstream iss;
    int32_t cheatID;
    for(size_t i = 0; i < strVec.size(); ++i){
        iss.clear();
        iss.str(strVec[i]);
        iss>>cheatID;
        _cheatIDSet.insert(cheatID);
    }
}

void CheatSorter::sort(SortParam &sortParam) {
    assert(_comp);
    PoolVector<matchdoc::MatchDoc> &matchDocs = sortParam.matchDocs;
    uint32_t end = min(sortParam.requiredTopK, (uint32_t)sortParam.matchDocs.size());
    size_t size = _cheatIDSet.size();
    size_t count = 0;
    if(size > 0 && _idAttributRef != NULL){
        int32_t id;
        for(size_t i = 0; i < sortParam.matchDocs.size() && count < size; i++){
            id = _idAttributRef->get(matchDocs[i]);
            if(_cheatIDSet.count(id) > 0 ){
                swap(matchDocs[i], matchDocs[count++]);
            }
        }
        std::sort(matchDocs.begin(),
                  matchDocs.begin() + count, 
                  MatchDocComp(_comp));
    }
    if(count < end){
        std::sort(matchDocs.begin()+count,
                  matchDocs.begin()+end,  
                  MatchDocComp(_comp));
    }
}

void CheatSorter::endSort() {
}

void CheatSorter::destroy() {
    delete this;
}

END_HA3_NAMESPACE(sorter);

