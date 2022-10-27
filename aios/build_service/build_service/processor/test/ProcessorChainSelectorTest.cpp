#include "build_service/test/unittest.h"
#include "build_service/processor/ProcessorChainSelector.h"
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;
using namespace testing;
using namespace build_service::config;

IE_NAMESPACE_USE(document);

namespace build_service {
namespace processor {

class ProcessorChainSelectorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void ProcessorChainSelectorTest::setUp() {
}

void ProcessorChainSelectorTest::tearDown() {
}

TEST_F(ProcessorChainSelectorTest, testSimple) {
    string selectorConfigStr = R"({
        "select_fields": ["field", "section"],
        "select_rules":[
        { 
            "matcher": {"field": "f1", "section": "s1"},
            "dest_chains": ["chain1"]
        },
        { 
            "matcher": {"field": "f2", "section": "s2"},
            "dest_chains": ["chain2", "chain1"]
        },
        { 
            "matcher": {"field": "f3", "section": "s3"},
            "dest_chains": ["chain3"]
        }]
    })";
    ProcessorChainSelectorConfigPtr selectorConfig(new ProcessorChainSelectorConfig);
    EXPECT_NO_THROW(FromJsonString(*selectorConfig, selectorConfigStr));

    string chainConfigStr = R"([
    {
        "clusters": ["clsuter1"],
        "document_processor_chain": [],
        "chain_name": "chain1"
    },
    {
        "clusters": ["clsuter2"],
        "document_processor_chain": [],
        "chain_name": "chain2"
    },
    {
        "clusters": ["clsuter3"],
        "document_processor_chain": [],
        "chain_name": "chain3"
    }
    ])";
    DocProcessorChainConfigVecPtr chainConfig(new DocProcessorChainConfigVec);
    EXPECT_NO_THROW(FromJsonString(*chainConfig, chainConfigStr));

    ProcessorChainSelector selector;
    ASSERT_TRUE(selector.init(chainConfig, selectorConfig));

    {
        RawDocumentPtr rawDoc(new DefaultRawDocument);
        rawDoc->setField("field", "f1");
        rawDoc->setField("section", "s1");

        const ProcessorChainSelector::ChainIdVector* chains = selector.selectChain(rawDoc);
        ASSERT_TRUE(chains);
        ASSERT_EQ(1, chains->size());
        ASSERT_EQ(0, (*chains)[0]);//chain1
    }
    {
        RawDocumentPtr rawDoc(new DefaultRawDocument);
        rawDoc->setField("field", "f2");
        rawDoc->setField("section", "s2");

        const ProcessorChainSelector::ChainIdVector* chains = selector.selectChain(rawDoc);
        ASSERT_TRUE(chains);
        ASSERT_EQ(2, chains->size());
        ASSERT_EQ(1, (*chains)[0]); //chain2
        ASSERT_EQ(0, (*chains)[1]); //chain1
    }
    {
        RawDocumentPtr rawDoc(new DefaultRawDocument);
        rawDoc->setField("field", "f3");
        rawDoc->setField("section", "s3");

        const ProcessorChainSelector::ChainIdVector* chains = selector.selectChain(rawDoc);
        ASSERT_TRUE(chains);
        ASSERT_EQ(1, chains->size());
        ASSERT_EQ(2, (*chains)[0]);//chain3
    }
}
  
TEST_F(ProcessorChainSelectorTest, testInitFailed) {
    string selectorConfigStr = R"({
        "select_fields": ["field", "section"],
        "select_rules":[
        { 
            "matcher": {"field": "f1", "section": "s1"},
            "dest_chains": ["chain1"]
        },
        { 
            "matcher": {"field": "f2", "section": "s2"},
            "dest_chains": ["chain2", "chain1"]
        },
        { 
            "matcher": {"field": "f3", "section": "s3"},
            "dest_chains": ["chain3"]
        }]
    })";
    ProcessorChainSelectorConfigPtr selectorConfig(new ProcessorChainSelectorConfig);
    EXPECT_NO_THROW(FromJsonString(*selectorConfig, selectorConfigStr));
    {
      //test chain name empty fail
      string chainConfigStr = R"([
      {
        "clusters": ["clsuter1"],
        "document_processor_chain": [],
        "chain_name": "chain1"
      },
      {
        "clusters": ["clsuter2"],
        "document_processor_chain": [],
        "chain_name": "chain2"
      },
      {
        "clusters": ["clsuter3"],
        "document_processor_chain": []
      }
      ])";
      DocProcessorChainConfigVecPtr chainConfig(new DocProcessorChainConfigVec);
      EXPECT_NO_THROW(FromJsonString(*chainConfig, chainConfigStr));
      ASSERT_EQ("", (*chainConfig)[2].chainName);
      ProcessorChainSelector selector;
      ASSERT_FALSE(selector.init(chainConfig, selectorConfig));

    }
    {
      // test chain not exist, will not take effecto
      string chainConfigStr = R"([
      {
        "clusters": ["clsuter1"],
        "document_processor_chain": [],
        "chain_name": "chain1"
      },
      {
        "clusters": ["clsuter2"],
        "document_processor_chain": [],
        "chain_name": "chain2"
      }
      ])";
      DocProcessorChainConfigVecPtr chainConfig(new DocProcessorChainConfigVec);
      EXPECT_NO_THROW(FromJsonString(*chainConfig, chainConfigStr));
      ASSERT_EQ(2, chainConfig->size());
      ASSERT_NE("chain3", (*chainConfig)[0].chainName);
      ASSERT_NE("chain3", (*chainConfig)[0].chainName);
      ProcessorChainSelector selector;
      ASSERT_TRUE(selector.init(chainConfig, selectorConfig));
      ASSERT_EQ(2, selector._selectFields.size());
      ASSERT_EQ(2, selector._chainsMap.size());
    }
}
TEST_F(ProcessorChainSelectorTest, testSelectChainFail) {
    string selectorConfigStr = R"({
        "select_fields": ["field", "section"],
        "select_rules":[
        { 
            "matcher": {"field": "f1", "section": "s1"},
            "dest_chains": ["chain1"]
        },
        { 
            "matcher": {"field": "f2", "section": "s2"},
            "dest_chains": ["chain2", "chain1"]
        },
        { 
            "matcher": {"field": "f3", "section": "s3"},
            "dest_chains": ["chain3"]
        }]
    })";
    ProcessorChainSelectorConfigPtr selectorConfig(new ProcessorChainSelectorConfig);
    EXPECT_NO_THROW(FromJsonString(*selectorConfig, selectorConfigStr));

    string chainConfigStr = R"([
    {
        "clusters": ["clsuter1"],
        "document_processor_chain": [],
        "chain_name": "chain1"
    },
    {
        "clusters": ["clsuter2"],
        "document_processor_chain": [],
        "chain_name": "chain2"
    },
    {
        "clusters": ["clsuter3"],
        "document_processor_chain": [],
        "chain_name": "chain3"
    }
    ])";
    DocProcessorChainConfigVecPtr chainConfig(new DocProcessorChainConfigVec);
    EXPECT_NO_THROW(FromJsonString(*chainConfig, chainConfigStr));

    ProcessorChainSelector selector;
    ASSERT_TRUE(selector.init(chainConfig, selectorConfig));
    {
        // select chain failed for empty field
        RawDocumentPtr rawDoc(new DefaultRawDocument);
        rawDoc->setField("field", "");
        rawDoc->setField("section", "s1");

        const ProcessorChainSelector::ChainIdVector* chains = selector.selectChain(rawDoc);
        ASSERT_TRUE(chains == NULL);
    }
    {
        // select chain failed for error field value
        RawDocumentPtr rawDoc(new DefaultRawDocument);
        rawDoc->setField("field", "not-exist");
        rawDoc->setField("section", "s1");

        const ProcessorChainSelector::ChainIdVector* chains = selector.selectChain(rawDoc);
        ASSERT_TRUE(chains == NULL);
    }
}
TEST_F(ProcessorChainSelectorTest, testDuplicateChainName) {
    string selectorConfigStr = R"({
        "select_fields": ["field", "section"],
        "select_rules":[
        { 
            "matcher": {"field": "f1", "section": "s1"},
            "dest_chains": ["chain1"]
        },
        { 
            "matcher": {"field": "f2", "section": "s2"},
            "dest_chains": ["chain2", "chain1"]
        }]
    })";
    ProcessorChainSelectorConfigPtr selectorConfig(new ProcessorChainSelectorConfig);
    EXPECT_NO_THROW(FromJsonString(*selectorConfig, selectorConfigStr));

    string chainConfigStr = R"([
    {
        "clusters": ["clsuter1"],
        "document_processor_chain": [],
        "chain_name": "chain1"
    },
    {
        "clusters": ["clsuter2"],
        "document_processor_chain": [],
        "chain_name": "chain2"
    },
    {
        "clusters": ["clsuter3"],
        "document_processor_chain": [],
        "chain_name": "chain2"
    }
    ])";
    DocProcessorChainConfigVecPtr chainConfig(new DocProcessorChainConfigVec);
    EXPECT_NO_THROW(FromJsonString(*chainConfig, chainConfigStr));

    ProcessorChainSelector selector;
    ASSERT_FALSE(selector.init(chainConfig, selectorConfig));
}

TEST_F(ProcessorChainSelectorTest, testSelectByDifferentField) {
    string selectorConfigStr = R"({
        "select_fields": ["field", "section"],
        "select_rules":[
        { 
            "matcher": {"field": "f1"},
            "dest_chains": ["chain1"]
        },
        { 
            "matcher": {"field": "f2"},
            "dest_chains": ["chain2", "chain1"]
        },
        { 
            "matcher": {"section": "s3"},
            "dest_chains": ["chain3"]
        },
        { 
            "matcher": {"field": "f1", "section": "s3"},
            "dest_chains": ["chain1", "chain3"]
        }]
    })";
    ProcessorChainSelectorConfigPtr selectorConfig(new ProcessorChainSelectorConfig);
    EXPECT_NO_THROW(FromJsonString(*selectorConfig, selectorConfigStr));

    string chainConfigStr = R"([
    {
        "clusters": ["clsuter1"],
        "document_processor_chain": [],
        "chain_name": "chain1"
    },
    {
        "clusters": ["clsuter2"],
        "document_processor_chain": [],
        "chain_name": "chain2"
    },
    {
        "clusters": ["clsuter3"],
        "document_processor_chain": [],
        "chain_name": "chain3"
    }
    ])";
    DocProcessorChainConfigVecPtr chainConfig(new DocProcessorChainConfigVec);
    EXPECT_NO_THROW(FromJsonString(*chainConfig, chainConfigStr));

    ProcessorChainSelector selector;
    ASSERT_TRUE(selector.init(chainConfig, selectorConfig));
    
    {
        RawDocumentPtr rawDoc(new DefaultRawDocument);
        rawDoc->setField("fff", "fff");

        const ProcessorChainSelector::ChainIdVector* chains = selector.selectChain(rawDoc);
        ASSERT_TRUE(chains == NULL);
    }
    {
        RawDocumentPtr rawDoc(new DefaultRawDocument);
        rawDoc->setField("field", "f1");
        rawDoc->setField("section", "s1");

        const ProcessorChainSelector::ChainIdVector* chains = selector.selectChain(rawDoc);
        ASSERT_TRUE(chains == NULL);
    }
    {
        RawDocumentPtr rawDoc(new DefaultRawDocument);
        rawDoc->setField("field", "f1");
        const ProcessorChainSelector::ChainIdVector* chains = selector.selectChain(rawDoc);
        ASSERT_TRUE(chains);
        ASSERT_EQ(1, chains->size());
        ASSERT_EQ(0, (*chains)[0]); //chain1
    }
    {
        RawDocumentPtr rawDoc(new DefaultRawDocument);
        rawDoc->setField("field", "f2");

        const ProcessorChainSelector::ChainIdVector* chains = selector.selectChain(rawDoc);
        ASSERT_TRUE(chains);
        ASSERT_EQ(2, chains->size());
        ASSERT_EQ(1, (*chains)[0]); //chain2
        ASSERT_EQ(0, (*chains)[1]); //chain1
    }
    {
        RawDocumentPtr rawDoc(new DefaultRawDocument);
        rawDoc->setField("field", "f1");
        rawDoc->setField("section", "s3");

        const ProcessorChainSelector::ChainIdVector* chains = selector.selectChain(rawDoc);
        ASSERT_TRUE(chains);
        ASSERT_EQ(2, chains->size());
        ASSERT_EQ(0, (*chains)[0]);//chain1
        ASSERT_EQ(2, (*chains)[1]);//chain3
    }
}

}
}
