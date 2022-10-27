#include <ha3/rank/Scorer.h>
#include <ha3/rank/ScoringProvider.h>
#include <suez/turing/expression/plugin/ScorerModuleFactory.h>
#include <suez/turing/expression/framework/VariableTypeTraits.h>

using namespace std;
USE_HA3_NAMESPACE(rank);
using namespace build_service::plugin;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(rank);

const string SCORER_NAME = "PersonalScorer";
const static char *PSFIELD = "PS";

class PersonalScorer : public Scorer {
public:
    PersonalScorer(const string &name) : Scorer(name) {
        _tscoreRef = NULL;
        _bscoreRef = NULL;
        _scoreRef = NULL;
        _priceRef = NULL;
        _useridRef = NULL;
    }
public:
    Scorer* clone() {
        return new PersonalScorer(*this);
    }
    bool beginRequest(suez::turing::ScoringProvider *provider_) {
        auto provider = dynamic_cast<ScoringProvider *>(provider_);
        _bscoreRef = provider->declareVariable<score_t>("b_score", true);
        if (_bscoreRef == NULL) {
            return false;
        }
        _tscoreRef = provider->declareVariable<score_t>("t_score", true);
        if (_tscoreRef == NULL) {
            return false;
        }
        _priceRef = provider->requireAttribute<double>("price");
        if(_priceRef == NULL) {
            return false;
        }
        _useridRef = provider->requireAttribute<int32_t>("userid");
        if(_useridRef == NULL) {
            return false;
        }
        _scoreRef = provider->getScoreReference();
        if(_scoreRef == NULL) {
            return false;
        }
        _personal_keys = provider->declareGlobalVariable<std::string>("personal_keys");
        _personal_counts = provider->declareGlobalVariable<std::string>("personal_counts");
        //Get PS type
        if (provider->getRequest()->getVirtualAttributeClause() != NULL) {
            const std::vector<VirtualAttribute*>& vecVA =
                provider->getRequest()->getVirtualAttributeClause()->getVirtualAttributes();
            for (uint32_t i=0; i<vecVA.size(); ++i) {
                if (vecVA[i]->getVirtualAttributeName() == PSFIELD) {
                    _psType = vecVA[i]->getVirtualAttributeSyntaxExpr()->getExprResultType();
                    _psName = vecVA[i]->getVirtualAttributeSyntaxExpr()->getExprString();
                }
            }
        }
        if (_psType == vt_unknown) {
            return true;
        }

#define TEST_CHECK_EXPR_VALUE_MACRO(vt_type)                            \
        case vt_type: {                                                 \
            typedef suez::turing::VariableTypeTraits<vt_type, false>::AttrExprType TT; \
            const auto *attributeRefPS = provider->requireAttribute<TT>("PS"); \
            if (NULL == attributeRefPS) {                               \
                HA3_LOG(ERROR, "Get PS Field error");                   \
                return false;                                           \
            }                                                           \
            _psRef = attributeRefPS;                                    \
            break;                                                      \
        }

        switch (_psType) {
            NUMERIC_VARIABLE_TYPE_MACRO_HELPER(TEST_CHECK_EXPR_VALUE_MACRO);
        default: {
            HA3_LOG(WARN, "Unknow PS Type");
            return false;
        }
        }
        *_personal_keys = "b_score#t_score";
        *_personal_counts = "20#5";
        return true;
    }
    void endRequest() {
    }
    score_t score(matchdoc::MatchDoc &matchDoc) {
        double price = _priceRef->get(matchDoc);
        int32_t userid = _useridRef->get(matchDoc);
        score_t score = _scoreRef->get(matchDoc);

        score_t bscore = (score_t)(score + price/2.0);
        score_t tscore = (score_t)(userid/10.0);
        _tscoreRef->set(matchDoc, tscore);
        _bscoreRef->set(matchDoc, bscore);
        return bscore + tscore;
    }
    void destroy() { delete this; }
private:
    const matchdoc::Reference<double> *_priceRef;
    const matchdoc::Reference<int32_t> *_useridRef;
    const matchdoc::Reference<score_t> *_scoreRef;
    const matchdoc::ReferenceBase *_psRef;
    matchdoc::Reference<score_t> *_bscoreRef;
    matchdoc::Reference<score_t> *_tscoreRef;
    std::string *_personal_keys;
    std::string *_personal_counts;
    VariableType _psType;
    std::string _psName;

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(rank, PersonalScorer);

class PersonalScorerModuleFactory : public ScorerModuleFactory {
public:
    bool init(const KeyValueMap &parameters) { return true; }
    Scorer* createScorer(const char *scorerName,
                         const KeyValueMap &scorerParameters,
                         suez::ResourceReader *resourceReader,
                         CavaPluginManager *cavaPluginManager)
    {
        if (SCORER_NAME == scorerName) {
            return new PersonalScorer(scorerName);
        } else {
            return NULL;
        }
    }
    void destroy() { };
};

extern "C"
ModuleFactory* createFactory() {
    return new PersonalScorerModuleFactory;
}

extern "C"
void destroyFactory(ModuleFactory *factory) {
    delete factory;
}

END_HA3_NAMESPACE(rank);
