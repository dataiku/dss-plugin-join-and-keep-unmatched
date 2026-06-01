# dss-plugin-join-and-keep-unmatched

⚠️ Starting with DSS version 11.3 this plugin is considered "deprecated". Use the native [join unmatched](https://doc.dataiku.com/dss/latest/other_recipes/join.html#adding-output-datasets-for-unmatched-rows) feature instead.

This Dataiku DSS plugin provides a recipe that joins two datasets and outputs three datasets:

- the result of the inner join
- the rows from the left dataset that did not match
- the rows from the right dataset that did not match

## Why this plugin is deprecated

Recent DSS versions provide the same capability natively in the Join recipe. You can now send unmatched rows to dedicated output datasets directly from DSS, without relying on this plugin.

See the DSS documentation:

- https://doc.dataiku.com/dss/latest/other_recipes/join.html#adding-output-datasets-for-unmatched-rows

More info: https://www.dataiku.com/dss/plugins/info/join-and-keep-unmatched.html

## Requirements

For python 3, DSS 7.0.2 or newer is required.
