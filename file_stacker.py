#import libraries, install if needed using pip or conda
import pandas as pd
import re
from os import listdir, chdir
from os.path import splitext
import numpy as np
import warnings

class FileStack:
    def __init__(self, path, extensions, pattern='', tag=None, recreate=True):
        """
        Class which reads files from directory as list of dataframes, then can stack them into a single dataframe with optional grouping.

        Parameters
        ---------
        path: path to files directory
        extensions: valid extensions to select
        pattern: regular expression of files to select, if not provided
                select all files of the specified extension
        tag: optional arbitrary tag to distinguish paths, patterns, or extensions
                of files from another set of files loaded into dictionary
        recreate: optional True or False to recreate files dictionary. By default
                recreate == True, the dictionary will be recreated, only
                containing new files loaded given path/extensions/pattern.
                If False is specified, new files are appended to the dictionary.
        """
        self.path = path
        self.extensions = extensions
        self.pattern = pattern
        self.tag = tag
        self.recreate = recreate
        self.files = self.files_dict()
        
        

    def files_dict(self):
        """
        Returns a dictionary of files from a given path

        Parameters
        ---------
        path: path to files directory
        extensions: valid extensions to select
        pattern: regular expression of files to select, if not provided
                select all files of the specified extension
        tag: optional arbitrary tag to distinguish paths, patterns, or extensions
                of files from another set of files loaded into dictionary
        recreate: optional True or False to recreate files dictionary. By default
                recreate == True, the dictionary will be recreated, only
                containing new files loaded given path/extensions/pattern.
                If False is specified, new files are appended to the dictionary.
        """
        #optional create of files list, if recreate == False, append new files to existing list
        if self.recreate == True:
            global files
            files = {}

        #valid extensions (must be tuple)
        extensions = self.extensions

        #file names contains the following text:
        find = self.pattern

        tag = self.tag

        path = self.path

        #change wd to specified path
        chdir(path)

        #specify directory path parameter in listdir(), or leave blank if using current directory
        items = listdir()

        if len(files) == 0:
            #create genericized dictionary of files with permitted extensions
            files = {i: {'name':splitext(i)[0], 'ext':splitext(i)[1], 'path':path, 'tag': tag,
                         'delim':csv.Sniffer().sniff(open(i, 'r').read()).delimiter if splitext(i)[1] == '.txt' else 'NA'} \
                     for i in items if i.endswith(extensions) and (re.search(find,i))}
        else:
            files.update({i: {'name':splitext(i)[0], 'ext':splitext(i)[1], 'path':path, 'tag': tag,
                              'delim':csv.Sniffer().sniff(open(i, 'r').read()).delimiter if splitext(i)[1] == '.txt' else 'NA'} \
                          for i in items if i.endswith(extensions) and (re.search(find,i))})

        return files



    def dfs(self, chunks=False, c_size=100000, nrows=None, encoding=None, all_sheets=False, **read_excel_kwargs):
        """
        Returns two objects: a list of dataframes (df_list), and a dictionary that maps metadata to that list (df_map)

        Parameters
        ---------
        files: must be a dictionary of files created from files_dict()
                function in this module
        chunks: optional parameter to chunk large files into smaller batches,
                to load into separate dataframes. Default is False.
        c_size: the chunk (batch) size to break large files into. There will
                be a different dataframe for each chunk
        all_sheets: optional parameter that applies to XLS/XLSX files, to load
                all sheets (value of True), or load the first sheet of the file
        recreate: optional True or False to recreate list of dataframes and dataframe metadata dictionary.
                By default recreate == True, where the objects will be recreated, only containing new dataframes
                from the current function call.
                If False is specified, new dataframes and metadata dictionary are appended to the objects.
        nrows: optionally return n rows

        **read_excel_kwargs: arguments which will be passed to `pd.DataFrame.to_excel`
                            [can be dictionary]
        """

        files = self.files

        if self.recreate == True:
            # create empty list 'df_list' to store dataframes; for each item in 'files' dictionary, will create pandas dataframe
            global df_list
            df_list = []

            # then, create genericized 'df_map' dictionary, mapping dataframe metadata to its index position in the 'df_list'
            global df_map
            df_map = {}

        for i, (k, v) in enumerate(files.items()):
            
            if chunks == True:
                chdir(v['path'])

                if v['ext'] in ['.xlsx','.xls']:
                    xl = pd.ExcelFile(k)
                    for sheet in xl.sheet_names:
                        parse = input('Parse sheet: \"'+sheet+'\"? Options: 1 = Yes, 0 = No:\t')
                        if parse == '1':
                            df_header = pd.read_excel(k, sheetname=sheet, nrows=1)
                            chunks = []
                            i_chunk = 0
                            # The first row is the header. We have already read it, so we skip it.
                            skiprows = 1
                            while True:
                                df_chunk = pd.read_excel(k, sheetname=sheet, nrows=c_size,
                                                         skiprows=skiprows, header=None)
                                skiprows += c_size
                                # When there is no data, we know we can break out of the loop.
                                if not df_chunk.shape[0]:
                                    break
                                else:
                                    print("  - chunk {i_chunk} ({df_chunk.shape[0]} rows)")
                                    chunks.append(df_chunk)
                                i_chunk += 1

                            df_chunks = pd.concat(chunks)
                            # Rename the columns to concatenate the chunks with the header.
                            columns = {i: col for i, col in enumerate(df_header.columns.tolist())}
                            df_chunks.rename(columns=columns, inplace=True)
                            df = pd.concat([df_header, df_chunks])
                            df_list.append(df)
                            df_map['%s_%s_%s' % (v['name'], sheet, str(i) if i >= 10 else '0'+str(i))] = {'ix':len(df_list)-1,
                                                                                    'chunk':i,
                                                                                    'row_count':len(df),
                                                                                    'features':list(df.columns),
                                                                                    'source': k
                                                                                   }
                            print('complete.')
                        else:
                            print('skipping...')
                    print('*'*20)

                else:
                    for i, chunk in enumerate(pd.read_csv(k, chunksize=c_size, nrows=nrows, encoding=encoding, engine='python')):
                        df = chunk
                        df.columns = [x.lstrip() for x in df.columns]
                        df_list.append(df)
                        df_map['%s_%s' % (v['name'], str(i) if i >= 10 else '0'+str(i))] = {'ix':len(df_list)-1,
                                                                            'chunk':i,
                                                                            'row_count':len(df),
                                                                            'features':list(df.columns),
                                                                            'source': k
                                                                           }

            else:

                #csv
                if v['ext'] == '.csv' or v['delim'] == ',':
                    chdir(v['path'])
                    df = pd.read_csv(k, nrows=nrows, encoding=encoding)
                    df_list.append(df)

                #tab-delimited
                if v['ext'] == '.txt' and v['delim'] == '\t':
                    chdir(v['path'])
                    f = open(k, 'r')
                    data = []
                    for row_num, line in enumerate(f):
                        values = line.strip().split('\t')
                        data.append([v for v in values])
                    headers = data.pop(0)
                    df = pd.DataFrame(data, columns=headers)
                    df_list.append(df)
                    f.close()

                # json
                elif v['ext'] == '.json':
                    chdir(v['path'])
                    df = pd.read_json(k)
                    df_list.append(df)


                # xls,xlsx
                elif v['ext'] in ['.xlsx','.xls']:
                    chdir(v['path'])

                    if all_sheets == True:
                        xl = pd.ExcelFile(k)
                        for sheet in xl.sheet_names:
                            parse = input('Parse sheet: \"'+sheet+'\"? Options: 1 = Yes, 0 = No:\t')
                            if parse == '1':
                                df = pd.read_excel(k, sheet_name=sheet, nrows=nrows, **read_excel_kwargs)
                                df_list.append(df)
                                df_map[v['name']+'_'+sheet] = {'ix':len(df_list)-1,'row_count':len(df),
                                                       'features':list(df.columns), 'source': k}
                                print('complete.')
                            else:
                                print('skipping...')
                        print('*'*20)
                    else:
                        df = pd.read_excel(k, nrows=nrows)
                        df_list.append(df)
                        df_map[v['name']] = {'ix':len(df_list)-1,'row_count':len(df),'features':list(df.columns), 'source': k}
                if v['ext'] in ['.xlsx','.xls']:
                    continue
                else:
                    df_map[v['name']] = {'ix':len(df_list)-1,'row_count':len(df), 'features':list(df.columns), 'source': k}

        #for k, v in df_map.items():
        #    print('name:',k,'\nindex:',v['ix'],'\nsource:',v['source'],'\nrow count:',v['row_count'],'\n')
        #    display(df_list[df_map[k]['ix']].head())
        #    print('-'*20)

        return df_list, df_map


    def stack(self, df_indices=None, cols=[], agg_cols=[], agg_funcs=['count'], filter_dict=None, **pd_concat_kwargs):
        """
        Returns concatenated/stacked dataframe from list of dataframes of arbitrary length,
        with optional grouping and filtering.

        Parameters
        ---------
        df_list: Must be a list of pandas dataframe objects to be concatenated
        cols: The shared columns between the dataframes in [df_list]. If
            agg_cols parameter is assigned, these columns will be grouped on
            in the pd.DataFrame.groupby() method
        agg_cols: List of columns to perform aggregate functions
            on (i.e. sum, min, max, count)
        agg_funcs: list of aggregate functions to pass to the pd.DataFrame.groupby.agg() method,
            applied to [agg_cols]
        filter_dict: A dictionary in the form {column, operator, value} to be passed as filters to
            the stacking function. Filters are applied before column slicing, so columns can have
            filters applied before exclusion.
            * Valid operators: '==', '!=', 'in', 'not in', 'regex', '>', '>=', '<', '<='
        **pd_concat_kwargs: arguments which will be passed to `pd.concat`
            object that is returned
        """
        df_list = self.dfs()[0]

        df_list = df_list[df_indices] if df_indices is not None else df_list
        
        try:
            len(cols + agg_cols)

        except:
            print("Must supply a list of shared columns (\"cols\") to be stacked, or \"agg_cols\" if a scalar aggregate value should be returned.")

        else:
            # create placeholder df
            stacked_df = pd.DataFrame()

            for df in df_list:
                # subset data using pd.DataFrame.filter() arguments
                if filter_dict:

                    for k, v in filter_dict.items():
                        f = filter_dict[k]
                        if f['operator'] in ['==','!=','>','>=', '<', '<=']:

                            quote = '\"' if type(f['value']) == str else ''
                            filter_ = 'df[df[\"'+ f['column'] + '\"] ' + f['operator'] + ' ' + quote + str(f['value']) + quote + ']'
                            df = eval(filter_)

                        elif f['operator'] in ['in','not in']:

                            checkTrue = ' != True' if f['operator'] == 'not in' else ''
                            filter_ = 'df[df[\"'+ f['column'] + '\"].isin('+str(f['value'])+')' + checkTrue + ']'
                            df = eval(filter_)

                        elif f['operator'] == 'regex':
                            df = df[df[f['column']].str.contains(f['value'])]


                #concatenate cols with agg_cols if both exist
                if agg_cols:
                    df = df[list(set(cols + agg_cols))]

                else:
                    df = df[cols]

                # concat stacked_df with new df from df_list, also passing kwargs
                stacked_df = pd.concat([stacked_df, df], **pd_concat_kwargs)

            if agg_cols and cols:

                stacked_df = stacked_df.groupby(cols)[agg_cols].agg(agg_funcs).reset_index()

                stacked_df.columns = stacked_df.columns.map(lambda x: '_'.join([str(i) for i in x]).rstrip('_'))

            if agg_cols and not cols:
    
                stacked_df = stacked_df[agg_cols].agg(agg_funcs).reset_index()

            return stacked_df