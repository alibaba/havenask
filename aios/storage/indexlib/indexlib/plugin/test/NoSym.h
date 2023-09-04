#pragma once
#include <string>

using namespace std;

namespace indexlib { namespace plugin {

class NoSym
{
public:
    string getName() { return "NoSym"; }
    bool isUsed() const;
};

}} // namespace indexlib::plugin
