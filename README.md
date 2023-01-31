# dss-plugin-join-and-keep-unmatched

⚠️ Starting with DSS version 11.3 this plugin is considered \"deprecated\", we recommend using the native [join unmatched](https://doc.dataiku.com/dss/latest/other_recipes/join.html#adding-output-datasets-for-unmatched-rows) feature instead.

This Dataiku DSS plugin provides a recipe that joins two datasets and outputs three datasets:
- the result of the inner join
- the rows from the the left dataset that did not match
- the rows from the the right dataset that did not match

More info: https://www.dataiku.com/dss/plugins/info/join-and-keep-unmatched.html

## Requirements

For python 3, DSS 7.0.2 or newer is required.
