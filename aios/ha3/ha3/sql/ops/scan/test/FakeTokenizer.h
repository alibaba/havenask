#pragma once
#include <build_service/analyzer/Tokenizer.h>

BEGIN_HA3_NAMESPACE(sql);

class FakeTokenizer : public build_service::analyzer::Tokenizer {
    bool init(const build_service::KeyValueMap &parameters,
              const build_service::config::ResourceReaderPtr &resourceReaderPtr)
        override
    {
        return true;
    }
    void tokenize(const char *text, size_t len) override {}
    bool next(build_service::analyzer::Token &token) override { return false; }
    build_service::analyzer::Tokenizer* clone() override {
        return new FakeTokenizer();
    }
};

END_HA3_NAMESPACE(sql);
