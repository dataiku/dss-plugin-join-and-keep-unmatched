import logging
import pandas as pd
import dataiku
from dataiku.customrecipe import *
from dataiku.sql import SelectQuery, Column, JoinTypes, toSQL
from dataiku.core.sql import SQLExecutor2

logger = logging.getLogger(__name__)

# Prepare and check parameters

input_first_names = get_input_names_for_role('first_dataset')
input_first_name = input_first_names[0]
input_first_dataset = dataiku.Dataset(input_first_names[0])

input_second_names = get_input_names_for_role('second_dataset')
input_second_name = input_second_names[0]
input_second_dataset = dataiku.Dataset(input_second_names[0])

def get_output_dataset(role):
    names = get_output_names_for_role(role)
    return dataiku.Dataset(names[0]) if len(names) > 0 else None

output_first_dataset = get_output_dataset('first_output')
output_second_dataset = get_output_dataset('second_output')
output_inner_dataset = get_output_dataset('inner_output')

class RecipeParams:
    def __init__(self, raw_params):
        self.first_dataset_keys = raw_params['first_dataset_keys']
        self.second_dataset_keys = raw_params['second_dataset_keys']

        # in case there are duplicate names in selected columns, just suffix them with _1 and _2
        self.selected_columns = []
        for col_name in raw_params['columns_from_first_dataset']:
            if col_name not in raw_params['columns_from_second_dataset']:
                self.selected_columns.append({'table': input_first_name, 'name': col_name, 'alias': col_name})
            else:
                self.selected_columns.append({'table': input_first_name, 'name': col_name, 'alias': col_name+'_1'})
        for col_name in raw_params['columns_from_second_dataset']:
            if col_name not in raw_params['columns_from_first_dataset']:
                self.selected_columns.append({'table': input_second_name, 'name': col_name, 'alias': col_name})
            else:
                self.selected_columns.append({'table': input_second_name, 'name': col_name, 'alias': col_name+'_2'})

        if (len(self.first_dataset_keys) != len(self.second_dataset_keys)):
            raise Exception("The number of join keys must be the same for the first and the second datasets")

params = RecipeParams(get_recipe_config())

# Generate SQL

join_conditions = []

for key in range(len(params.first_dataset_keys)):
    condition = Column(params.first_dataset_keys[key], input_first_name).eq_null_unsafe(Column(params.second_dataset_keys[key], input_second_name))
    join_conditions.append(condition)

if output_inner_dataset:
    query = SelectQuery()
    query.select_from(input_first_dataset, alias=input_first_name)
    for col in params.selected_columns:
        query.select(Column(col['name'], col['table']), alias=col['alias'])
    query.join(input_second_dataset, JoinTypes.INNER, join_conditions, alias=input_second_name)
    sql_inner = toSQL(query, input_first_dataset)

if output_first_dataset:
    query = SelectQuery()
    query.select(Column('*', input_first_name))
    query.select_from(input_first_dataset, alias=input_first_name)
    query.join(input_second_dataset, JoinTypes.LEFT, join_conditions, alias=input_second_name)
    query.where(Column(params.second_dataset_keys[0], input_second_name).is_null())
    sql_left = toSQL(query, dataset=input_first_dataset)

if output_second_dataset:
    query = SelectQuery()
    query.select(Column('*', input_second_name))
    query.select_from(input_first_dataset, alias=input_first_name)
    query.join(input_second_dataset, JoinTypes.RIGHT, join_conditions, alias=input_second_name)
    query.where(Column(params.first_dataset_keys[0], input_first_name).is_null())
    sql_right = toSQL(query, dataset=input_first_dataset)

# Execute SQL

e = SQLExecutor2()
if output_inner_dataset:
    logger.info("Execute query for inner join.")
    e.exec_recipe_fragment(output_inner_dataset, sql_inner)
if output_first_dataset:
    logger.info("Execute query for non matching rows from first dataset.")
    e.exec_recipe_fragment(output_first_dataset, sql_left)
if output_second_dataset:
    logger.info("Execute query for non matching rows from second dataset.")
    e.exec_recipe_fragment(output_second_dataset, sql_right)
