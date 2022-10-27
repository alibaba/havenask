#include <ha3/config/ResourceTypeSet.h>
#include <autil/StringTokenizer.h>

using namespace std;
using namespace autil;

BEGIN_HA3_NAMESPACE(config);
HA3_LOG_SETUP(config, ResourceTypeSet);

ResourceTypeSet::ResourceTypeSet() { 
}

ResourceTypeSet::~ResourceTypeSet() { 
}

void ResourceTypeSet::fromString(const string &str) {
    _types.clear();
    StringTokenizer tokens(str, ",", StringTokenizer::TOKEN_IGNORE_EMPTY | 
                           StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < tokens.getNumTokens(); i++) {
        _types.insert(tokens[i]);
    }
}
    
string ResourceTypeSet::toString() const {
    string str;
    for (set<string>::const_iterator it = _types.begin(); 
         it != _types.end(); it++)
    {
        if (str.empty()) {
            str = *it;
        } else {
            str += "," + *it;
        }
    }
    return str;
}

bool ResourceTypeSet::empty() const {
    return _types.empty();
}


END_HA3_NAMESPACE(config);

