#include <string>

#include "indexlib/plugin/ModuleFactory.h"
#include "indexlib/plugin/test/DummyObj.h"

using namespace std;

namespace indexlib { namespace plugin {

class MyDummyObj : public DummyObj
{
public:
    MyDummyObj(const string& name) : _name(name) { _name = "MyDummyObj: " + _name; }
    string getName() { return _name; }

private:
    string _name;
};

class DummyFactory : public DummyObjFactory
{
public:
    DummyFactory() {}
    virtual ~DummyFactory() {}

public:
    bool init() { return true; }

    void destroy() { delete this; }

    DummyObj* createObj(const std::string& name) { return new MyDummyObj(name); }
};

extern "C" ModuleFactory* createFactory() { return new DummyFactory; }

extern "C" void destroyFactory(ModuleFactory* factory) { factory->destroy(); }

}} // namespace indexlib::plugin
