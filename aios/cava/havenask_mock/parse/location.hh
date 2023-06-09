#pragma once
#include <string>

namespace cava {
struct Begin {
    int line;
    std::string* filename;
};
class location {
    public:
        Begin begin;
};
}