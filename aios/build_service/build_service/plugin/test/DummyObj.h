#ifndef ISEARCH_BS_DUMMYOBJ_H
#define ISEARCH_BS_DUMMYOBJ_H

#include "build_service/util/Log.h"
#include "build_service/plugin/ModuleFactory.h"

namespace build_service {
namespace plugin {

class DummyObj
{
public:
    virtual ~DummyObj() {}
public:
    virtual std::string getName() = 0;
};

class DummyObjFactory : public ModuleFactory
{
public:
    virtual ~DummyObjFactory() {}
    virtual DummyObj* createObj(const std::string &name) = 0;
};

}
}

#endif //ISEARCH_BS_DUMMYOBJ_H
