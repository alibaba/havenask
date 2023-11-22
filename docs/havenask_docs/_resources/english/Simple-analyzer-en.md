## Overview

The simple analyzer (simple) allows you to fully control searches. This analyzer is suitable for scenarios in which other built-in analyzers cannot meet search requirements. Spaces are used to separate terms in field values and search queries.



## Additional considerations

- This analyzer applies only to fields of the TEXT data type. To use the analyzer, you must set the analyzers parameter to simple_analyzer when you configure the tokenizer schema.

- This analyzer does not allow you to intervene in text analysis.