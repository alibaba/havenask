#pragma once

#include <limits>
#include <stdint.h>

#include "autil/DataBuffer.h"

namespace matchdoc {

class FiveBytesString {
public:
    FiveBytesString() {}
    FiveBytesString(const FiveBytesString &other) {
        for (size_t i = 0; i < 5; ++i) {
            data[i] = other.data[i];
        }
    }
    FiveBytesString &operator=(const FiveBytesString &other) {
        for (size_t i = 0; i < 5; ++i) {
            data[i] = other.data[i];
        }
        return *this;
    }
    std::string toString() const { return std::string(data, 5); }
    void serialize(autil::DataBuffer &dataBuffer) const {
        for (size_t i = 0; i < 5; ++i) {
            dataBuffer.write(data[i]);
        }
    }
    void deserialize(autil::DataBuffer &dataBuffer) {
        for (size_t i = 0; i < 5; ++i) {
            dataBuffer.read(data[i]);
        }
    }

public:
    char data[5];
};
} // namespace matchdoc
