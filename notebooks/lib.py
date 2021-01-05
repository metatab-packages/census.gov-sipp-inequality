import pandas as pd 



'''
The following code is an example of reading pipe-delimited Survey of Income and Program Participation (SIPP) 
    data into a Pandas dataframe. Specifically, this code loads and merges the primary dataset and the 
    calendar-year replicate weights (as opposed to the longitudinal replicate weights) in preparation for 
    analysis.
This code is written for Python 3, and requires version 0.24 or higher of the Pandas module. 
Note the use of 'usecols' in the first pd.read_csv statement. Most machines do not have enough memory to read
    the entire SIPP file into memory. Use 'usecols' to read in only the columns you are interested in. If you
    still encounter an out-of-memory error, either select less columns, or consider using the Dask module.
Run this code in the same directory as the data.
This code was written by Adam Smith. Please report errors to <census.sipp@census.gov>.
'''

def get_metadata(sipp):

    #Read in the primary data file schema to get data-type information for
    #    each variable.
    rd_schema = pd.read_json(sipp.reference('pu2018_schema').resolved_url.get_resource().read())

    #Read in the replicate weight data file schema to get data-type information 
    #    for each variable.
    rw_schema = pd.read_json(sipp.reference('rw2018_schema').resolved_url.get_resource().read())


    #Define Pandas data types based on the schema data-type information for both schema dataframes
    rd_schema['dtype'] = ['Int64' if x == 'integer' \
                else 'object' if x == 'string' \
                else 'Float64' if x == 'float' \
                else 'ERROR' \
                for x in rd_schema['dtype']]

    rw_schema['dtype'] = ['Int64' if x == 'integer' \
                else 'object' if x == 'string' \
                else 'Float64' if x == 'float' \
                else 'ERROR' \
                for x in rw_schema['dtype']]
    
    
    col_label_map = dict(zip(rd_schema['name'], rd_schema['label']))
    dtype_map = dict(zip(rd_schema['name'], rd_schema['dtype']))

    col_set = pd.read_csv('../data/columns.csv')

    def columns_by_group(groups=None):

        if groups is None:
            groups = list(col_set.group.unique())

        t = col_set[col_set.group.isin(['id'] + groups)]

        return list(t.column)

    return columns_by_group, col_label_map, dtype_map, col_set

# Create counts per household of number of people in each category
import re
erel_desc = \
""" 1. Householder with relatives
    2. Householder with NO relatives
    3. Opposite-sex husband/wife/spouse
    4. Opposite-sex unmarried partner
    5. Same-sex husband/wife/spouse
    6. Same-sex unmarried partner
    7. Child
    8. Grandchild
    9. Parent
    10. Sibling
    11. Parent/Child-in-law (mother/father/son/daughter-in-law)
    12. Brother/Sister-in-law
    13. Aunt/Uncle, Niece/Nephew
    14. Other relative
    15. Foster child
    16. Housemate/Roommate
    17. Roomer/Boarder
    18. Other nonrelative 
""".splitlines()

erel_map = {
    1: 'hh_rel', # Householder with relatives
    2: 'hh_norel', # Householder with NO relatives
    3: 'os_spouse', # Opposite-sex husband/wife/spouse
    4: 'os_partner', # Opposite-sex unmarried partner
    5: 'ss_spouse', # Same-sex husband/wife/spouse
    6: 'ss_partner', # Same-sex unmarried partner
    7: 'child', # Child
    8: 'grandchild', # Grandchild
    9: 'parent', # Parent
    10: 'sibling', # Sibling
    11: 'pc_inlaw', # Parent/Child-in-law (mother/father/son/daughter-in-law)
    12: 'sib_inlaw', # Brother/Sister-in-law
    13: 'aunn', # Aunt/Uncle, Niece/Nephew
    14: 'other_rel', # Other relative
    15: 'foster_child', # Foster child
    16: 'roommate', # Housemate/Roommate
    17: 'boarder', # Roomer/Boarder
    18: 'other_nonrel', # Other nonrelative
}

"""
 EEDUC
    31. Less than 1st grade
    32. 1st, 2nd, 3rd or 4th grade
    33. 5th or 6th grade
    34. 7th or 8th grade
    35. 9th grade
    36. 10th grade
    37. 11th grade
    38. 12th grade, no diploma
    39. High School Graduate (diploma or GED or equivalent)
    40. Some college credit, but less than 1 year (regular Jr.coll./coll./univ.)
    41. 1 or more years of college, no degree (regular Jr.coll./coll./univ.)
    42. Associate's degree (2-year college)
    43. Bachelor's degree (for example: BA, AB, BS)
    44. Master's degree (for example: MA, MS, MBA, MSW)
    45. Professional School degree (for example: MD (doctor), DDS (dentist), JD (lawyer))
    46. Doctorate degree (for example: Ph.D., Ed.D.) 
"""

eeduc_to_years = {
    31: 0,
    32: 2.5,
    33: 5.5,
    34: 7.5,
    35: 9,
    36: 10,
    37: 11,
    38: 12,
    39: 12,
    40: 13,
    41: 14,
    42: 14,
    43: 16,
    44: 18,
    45: 22,
    46: 22
}
