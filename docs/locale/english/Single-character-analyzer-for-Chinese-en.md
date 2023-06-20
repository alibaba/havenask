# Overview of analyzers

The single character analyzer for Chinese (chn_single) tokenizes text into characters or words. This analyzer is suitable for searches that are not based on Chinese semantics.

```

Original content: 菊花茶123

Analysis result: 菊 花 茶 123

```



# Additional considerations

- This analyzer applies only to fields of the TEXT data type. To use the analyzer, set the analyzer to chn_single when you configure a schema.

- This analyzer does not allow you to intervene in text analysis.