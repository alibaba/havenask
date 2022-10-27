#ifndef ISEARCH_PHRASEINDOCPOSITIONSTATE_H
#define ISEARCH_PHRASEINDOCPOSITIONSTATE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <indexlib/index/normal/inverted_index/accessor/in_doc_position_state.h>
#include <ha3/search/PhraseInDocPositionIterator.h>
#include <vector>

IE_NAMESPACE_BEGIN(index);
class PhraseInDocPositionState : public InDocPositionState 
{
public:
    PhraseInDocPositionState(std::vector<pos_t> *posVector, tf_t tf = 0);
    PhraseInDocPositionState(const PhraseInDocPositionState &state);

    virtual ~PhraseInDocPositionState() {}

    virtual InDocPositionIterator* CreateInDocIterator() const;
    virtual InDocPositionState* Clone() const;
    virtual void Free() {
        delete this;
    }
protected:
    PositionVectorPtr _posVectorPtr;
private:
    HA3_LOG_DECLARE();
};

IE_NAMESPACE_END(index);

#endif //ISEARCH_PHRASEINDOCPOSITIONSTATE_H
