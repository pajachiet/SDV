import json

import pandas as pd

from sdv.data_navigator import CSVDataLoader
from sdv.modeler import Modeler
from sdv.sampler import Sampler

parent_table = pd.DataFrame([{'parent_id': i + 1} for i in range(3)])
parent_table.to_csv('parent.csv', header=True, index=False)

parent_table_metadata = {
    'fields': [
        {
            'name': 'parent_id',
            'subtype': 'integer',
            'type': 'number',
            'regex': '^[0-9]{10}$'
        },

    ],
    'headers': True,
    'name': 'parent',
    'path': 'parent.csv',
    'primary_key': 'parent_id',
    'use': True
}

child_table = pd.DataFrame([
    {
        'child_id': 1,
        'parent_id': 1,
        'value': 'A'
    },
    {
        'child_id': 2,
        'parent_id': 1,
        'value': 'B'
    },
    {
        'child_id': 3,
        'parent_id': 1,
        'value': 'A'
    },
    {
        'child_id': 4,
        'parent_id': 2,
        'value': 'B',
    },
    {
        'child_id': 5,
        'parent_id': 2,
        'value': 'A'
    }
])
child_table.to_csv('child.csv', header=True, index=False)

child_table_metadata = {
    'fields': [
        {
            'name': 'child_id',
            'subtype': 'integer',
            'type': 'number',
            'regex': '^[0-9]{10}$'
        },
        {
            'name': 'parent_id',
            'ref': {
                'field': 'parent_id',
                'table': 'parent'
            },
            'subtype': 'integer',
            'type': 'number',
        },
        {
            'name': 'value',
            'type': 'categorical',
        },

    ],
    'headers': True,
    'name': 'child',
    'path': 'child.csv',
    'primary_key': 'child_id',
    'use': True
}

metadata = {
    'path': '',
    'tables': [
        parent_table_metadata,
        child_table_metadata
    ]
}

metadata_filename = 'metadata.json'
with open(metadata_filename, 'w') as f:
    json.dump(metadata, f)

# At his point we have two tables, `parent` and `child` that are related
# Fist, we are going to add a field `num_children` on the parent table, counting the amount of children rows related to each parent row

# We count the values for each child
foreign_related = child_table.groupby('parent_id').count()['value'].to_frame()

# We add it to the parent table
parent_table = parent_table.merge(foreign_related, how='left', left_on='parent_id', right_index=True)

# We clean possible missing values
parent_table.value.fillna(0, inplace=True)
parent_table.value = parent_table.value.astype(int)

# Rename the column
parent_table.rename(columns={'value': 'num_children'}, inplace=True)

# We save our data to disk
parent_table.to_csv('parent.csv', header=True, index=False)

## Update the metadata and we can go
metadata['tables'][0]['fields'].append({
    'name': 'num_children',
    'type': 'number',
    'subtype': 'integer'
})

with open(metadata_filename, 'w') as f:
    json.dump(metadata, f)

# Now its time to sample
data_loader = CSVDataLoader(metadata_filename)
data_navigator = data_loader.load_data()
data_navigator.transform_data()

modeler = Modeler(data_navigator)
modeler.model_database()

sampler = Sampler(data_navigator, modeler)

# Now we are going to sample a few rows from the parent table:

sampled_data = {}
sampled_data['parent'] = sampler.sample_rows('parent', 10)


# At this pointthe sampled rows are also stored raw (with the conditional parameters for the children row) in sampler.sampled['parent']
# so we will iterate over each row, pass them to sampler.

def sample_children(sampled):
    def f(row):
        num_children = int(row['num_children'])
        print(num_children)
        result = sampler._sample_child_rows('parent', row, sampled, num_children)

    return f


sampled_data['parent'].apply(sample_children(sampled_data), axis=1)

# Now all that is left is to reset the indices
sampled_data = sampler.reset_indices_tables(sampled_data)

