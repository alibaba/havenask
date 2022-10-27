#include "build_service/plugin/ModuleFactory.h"
#include "build_service/plugin/test/DummyObj.h"
#include <string>

using namespace std;
namespace build_service {
namespace plugin {

class MyDummyObj : public DummyObj {
public:
    MyDummyObj(const string &name) : _name(name) {
        _name = "MyDummyObj: " + _name;
    }
    string getName() {return _name;}
private:
    string _name;
};

class DummyFactory : public DummyObjFactory
{
public:
    DummyFactory() {}
    virtual ~DummyFactory() {}

public:
    bool init(const KeyValueMap &parameters) {
        return true;
    }

    void destroy() override {
        delete this;
    }

    DummyObj* createObj(const std::string &name) {
        return new MyDummyObj(name);
    }
};

extern "C"
ModuleFactory* createFactory() {
    return new DummyFactory;
}

extern "C"
void destroyFactory(IE_NAMESPACE(plugin)::ModuleFactory *factory) {
    factory->destroy();
}

}
}
