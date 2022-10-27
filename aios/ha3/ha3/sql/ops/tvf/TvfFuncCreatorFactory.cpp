#include "ha3/sql/ops/tvf/TvfFuncCreatorFactory.h"

using namespace std;
using namespace iquan;
BEGIN_HA3_NAMESPACE(sql);

AUTIL_LOG_SETUP(tvf, TvfFuncCreatorFactory);

TvfFuncCreatorFactory::TvfFuncCreatorFactory()
{
}

TvfFuncCreatorFactory::~TvfFuncCreatorFactory() {
    for (auto &iter : _funcCreatorMap) {
        delete iter.second;
    }
}

bool TvfFuncCreatorFactory::init(const KeyValueMap &parameters) {
    return true;
}

bool TvfFuncCreatorFactory::init(const KeyValueMap &parameters,
                                 const build_service::config::ResourceReaderPtr &resourceReaderPtr)
{
    return init(parameters);
}

bool TvfFuncCreatorFactory::init(const KeyValueMap &parameters,
                                 const build_service::config::ResourceReaderPtr &resourceReaderPtr,
                                 TvfResourceContainer *resourceContainer)
{
    return init(parameters, resourceReaderPtr);
}

TvfFuncCreatorMap& TvfFuncCreatorFactory::getTvfFuncCreators() {
    return _funcCreatorMap;
}

TvfFuncCreator *TvfFuncCreatorFactory::getTvfFuncCreator(const std::string &name) {
    auto it = _funcCreatorMap.find(name);
    if (it != _funcCreatorMap.end()) {
        return it->second;
    }
    return nullptr;
}

bool TvfFuncCreatorFactory::addTvfFunctionCreator(TvfFuncCreator *creator) {
    if (creator == nullptr) {
        return false;
    }
    const std::string &name = creator->getName();
    if (_funcCreatorMap.find(name) != _funcCreatorMap.end()) {
        SQL_LOG(ERROR, "add tvf func [%s] creator failed, already exists.", name.c_str());
        return false;
    }
    _funcCreatorMap[name] = creator;
    return true;
}

END_HA3_NAMESPACE(sql)
